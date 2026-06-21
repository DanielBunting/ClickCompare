using DriverConnection = ClickHouse.Driver.ADO.ClickHouseConnection;
using NativeConnection = CH.Native.Connection.ClickHouseConnection;

namespace ClickCompare.Core;

/// <summary>
/// The 20M-row (20 × 1M chunks) bulk-load comparison from ingestion-comparisson.md §6: server-local
/// Parquet <c>file()</c> against 20 sequential 1M inserts on each client, every client tuned for 1M
/// writes (no batching staircase). The file route's transfer cost is split out so "copy" and "copy +
/// ingest" appear as their own numbers — without the copy the comparison flatters the file route, which
/// only wins once the bytes are already on the box.
/// </summary>
public enum BulkLoadRoute
{
    /// <summary>20 Parquet chunks already on the server's disk; one <c>file('…_*.parquet')</c> INSERT. Decode only.</summary>
    ParquetFileIngest,

    /// <summary>Just the transfer: copy the 20 chunks (~172 MB) onto the server. The doc's <c>docker cp</c> leg, isolated.</summary>
    ParquetCopyToServer,

    /// <summary>End-to-end: copy the 20 chunks onto the server, then the <c>file()</c> INSERT. The honest file-route number.</summary>
    ParquetCopyAndIngest,

    /// <summary>20 sequential <c>INSERT … FORMAT Parquet</c> HTTP POSTs — ships over the wire, no server-side files.</summary>
    ParquetHttpSequential,

    /// <summary>CH.Native: 20 × 1M streamed typed inserts, one INSERT per chunk.</summary>
    NativeSequential,

    /// <summary>ClickHouse.Driver tuned for 1M writes (BatchSize = 1M): 20 × 1M inserts, one HTTP INSERT per chunk.</summary>
    DriverSequential,
}

/// <summary>One row of the 20M-row bulk-load report.</summary>
public sealed record BulkLoadCase(string Name, BulkLoadRoute Route, int Chunks, int RowsPerChunk)
{
    public long TotalRows => (long)Chunks * RowsPerChunk;
    public override string ToString() => Name;
}

/// <summary>
/// The §6 bulk-load table. Chunk count defaults to 20 (the doc's 20M-row measurement) but is
/// overridable with <c>BULK_CHUNKS</c> (e.g. <c>BULK_CHUNKS=5</c> for a 5M-row, lighter-fan-out pass);
/// rows-per-chunk stays 1M so each chunk is one tuned 1M write. Labels reflect the active count.
/// </summary>
public static class BulkLoadCases
{
    public const int RowsPerChunk = 1_000_000;

    /// <summary>Chunks per run. Defaults to 20; override with <c>BULK_CHUNKS</c>.</summary>
    public static readonly int Chunks =
        int.TryParse(Environment.GetEnvironmentVariable("BULK_CHUNKS"), out var n) && n > 0 ? n : 20;

    public static readonly IReadOnlyList<BulkLoadCase> All = Build(Chunks);

    private static IReadOnlyList<BulkLoadCase> Build(int chunks)
    {
        var p = $"{chunks}x1M"; // label prefix, e.g. "5x1M" / "20x1M"
        return new[]
        {
            new BulkLoadCase($"{p} Parquet file() ingest (on disk, decode only)", BulkLoadRoute.ParquetFileIngest, chunks, RowsPerChunk),
            new BulkLoadCase($"{p} Parquet copy-to-server (copy only)", BulkLoadRoute.ParquetCopyToServer, chunks, RowsPerChunk),
            new BulkLoadCase($"{p} Parquet copy + file() ingest (end-to-end)", BulkLoadRoute.ParquetCopyAndIngest, chunks, RowsPerChunk),
            new BulkLoadCase($"{p} Parquet HTTP POST (sequential, over wire)", BulkLoadRoute.ParquetHttpSequential, chunks, RowsPerChunk),
            new BulkLoadCase($"{p} CH.Native typed (1 streamed INSERT/chunk)", BulkLoadRoute.NativeSequential, chunks, RowsPerChunk),
            new BulkLoadCase($"{p} ClickHouse.Driver BatchSize=1M (1 INSERT/chunk)", BulkLoadRoute.DriverSequential, chunks, RowsPerChunk),
        };
    }
}

/// <summary>
/// Executes the 20-chunk routes. The same 1M-row payload is reused for every chunk (identical ids
/// repeat across chunks) — for an ingestion-throughput measurement the server work is byte-for-byte the
/// same as 20 distinct chunks, and reusing one payload keeps client memory at one chunk, not twenty.
/// </summary>
public static class BulkLoadRunner
{
    // A distinct prefix so these fixtures don't collide with the single-file Parquet route's fixture.
    public const string GlobPattern = "bulk_chunk_*.parquet";
    public static string ChunkName(int index) => $"bulk_chunk_{index}.parquet";

    /// <summary>Copy one Parquet payload onto the server under N chunk names — the doc's ~181 MB
    /// <c>docker cp</c> staging step.</summary>
    public static async Task CopyChunksAsync(byte[] chunkParquet, int chunks, CancellationToken ct = default)
    {
        for (var i = 0; i < chunks; i++)
            await ClickHouseFixture.CopyFileToServerAsync(chunkParquet, ChunkName(i), ct);
    }

    /// <summary>One INSERT that globs every chunk already on disk — the server decodes all files in parallel.</summary>
    public static Task<long> FileIngestAsync(DriverConnection driver, long totalRows, CancellationToken ct = default) =>
        ParquetWorkload.FileIngestAsync(driver, totalRows, GlobPattern, ct);

    /// <summary>20 sequential <c>FORMAT Parquet</c> POSTs of the same payload.</summary>
    public static async Task<long> HttpSequentialAsync(
        ClickHouseHttp http, byte[] chunkParquet, int chunks, int rowsPerChunk, CancellationToken ct = default)
    {
        long total = 0;
        for (var i = 0; i < chunks; i++)
            total += await ParquetWorkload.PostInsertAsync(http, chunkParquet, rowsPerChunk, ct);
        return total;
    }

    /// <summary>CH.Native: 20 × 1M streamed typed inserts (its default internal blocks — its fast path).</summary>
    public static async Task<long> NativeSequentialAsync(
        NativeConnection native, IReadOnlyList<BulkRow> chunkRows, int chunks, CancellationToken ct = default)
    {
        long total = 0;
        for (var i = 0; i < chunks; i++)
            total += await NativeBulkInsertRunner.RunTypedAsync(native, chunkRows, ct: ct);
        return total;
    }

    /// <summary>ClickHouse.Driver tuned to one INSERT per 1M chunk (BatchSize = rows ⇒ no 100K staircase).</summary>
    public static async Task<long> DriverSequentialAsync(
        DriverConnection driver, IReadOnlyList<object[]> chunkRows, int chunks, CancellationToken ct = default)
    {
        var scenario = new InsertScenario("chunk", chunkRows.Count, chunkRows.Count, Mdop: 1);
        long total = 0;
        for (var i = 0; i < chunks; i++)
            total += await BulkInsertRunner.RunAsync(driver, scenario, chunkRows, ct);
        return total;
    }
}
