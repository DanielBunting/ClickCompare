using CH.Native.BulkInsert;
using NativeConnection = CH.Native.Connection.ClickHouseConnection;

namespace ClickCompare.Core;

/// <summary>Which CH.Native serialization path to drive.</summary>
public enum NativePath
{
    /// <summary>Compiled column extractors, no per-row boxing — CH.Native's intended fast path.</summary>
    Typed,

    /// <summary>object[] input (same shape as the Driver) — isolates boxing/extractor cost.</summary>
    Dynamic,
}

/// <summary>Wire compression for CH.Native. The codec is selectable; the level is hardcoded in the
/// client (LZ4 fast / Zstd 3) so there is deliberately no "level" axis.</summary>
public enum NativeCompression
{
    /// <summary><c>Compress=false</c> — no wire compression (trades network bytes for CPU).</summary>
    Off,

    /// <summary><c>Compress=true;CompressionMethod=Lz4</c> — the default; fast, lower ratio.</summary>
    Lz4,

    /// <summary><c>Compress=true;CompressionMethod=Zstd</c> — higher ratio, more CPU.</summary>
    Zstd,
}

/// <summary>
/// One CH.Native insert configuration. The CH.Native bulk API exposes far fewer throughput knobs than
/// the Driver: there is <b>no</b> server-setting passthrough (no async_insert / max_insert_threads),
/// <b>no</b> intra-inserter concurrency (one streamed INSERT, sequential block flushes), and the
/// compression level is fixed. The meaningful axes — and the only ones varied here — are
/// <see cref="BatchSize"/>, <see cref="Compression"/>, <see cref="Path"/>, and connection-level
/// <see cref="Streams"/>. (BulkInsertOptions.IncludeNullColumns / UsePooledArrays are dead no-ops in
/// 1.1.1, so no variant pretends to tune them.)
/// </summary>
public sealed record NativeInsertConfig(
    string Name,
    int BatchSize = 100_000,
    NativeCompression Compression = NativeCompression.Lz4,
    NativePath Path = NativePath.Typed,
    int Streams = 1)
{
    public override string ToString() => Name;
}

/// <summary>
/// Base config + single-variable variants for CH.Native. BASE = typed path, 100K block, LZ4 on, one
/// connection. Each variant changes exactly one axis. (Default BulkInsertOptions.BatchSize is 10K;
/// BASE raises it to 100K to match the Driver base and avoid tiny mid-stream flushes.)
/// </summary>
public static class NativeInsertConfigs
{
    public static readonly NativeInsertConfig Base = new("native: BASE (typed, 100k, lz4)");

    public static readonly IReadOnlyList<NativeInsertConfig> All = new[]
    {
        Base,

        // --- BatchSize (rows per native data block in the single streamed INSERT) ---------------
        Base with { Name = "native: batch=10k (default)", BatchSize = 10_000 },
        Base with { Name = "native: batch=500k", BatchSize = 500_000 },
        Base with { Name = "native: batch=1M", BatchSize = 1_000_000 },

        // --- Compression ------------------------------------------------------------------------
        Base with { Name = "native: compress=off", Compression = NativeCompression.Off },
        Base with { Name = "native: compress=zstd", Compression = NativeCompression.Zstd },

        // --- Serialization path -----------------------------------------------------------------
        Base with { Name = "native: dynamic (object[])", Path = NativePath.Dynamic },

        // --- App-level parallelism: N independent connections, disjoint id ranges ---------------
        // (A CH.Native connection is single-use/single-threaded — connection count is the ONLY
        //  parallelism dial; there is no intra-inserter pipelining.)
        Base with { Name = "native: streams=2", Streams = 2 },
        Base with { Name = "native: streams=4", Streams = 4 },
        Base with { Name = "native: streams=8", Streams = 8 },
        Base with { Name = "native: streams=10", Streams = 10 },
    };
}

/// <summary>
/// Executes a <see cref="NativeInsertConfig"/> against CH.Native, streaming the wide rows so 50M never
/// materialises. Returns the row count streamed (per-stream counts summed) for the correctness checks;
/// the authoritative verification is the server-side <c>count()</c> / <c>sum(id)</c> in the tests.
/// </summary>
public static class NativeWideInsertRunner
{
    public static async Task<long> RunAsync(
        string baseConnectionString, NativeInsertConfig cfg, long rowCount, CancellationToken ct = default)
    {
        var connStr = ApplyCompression(baseConnectionString, cfg.Compression);

        if (cfg.Streams <= 1)
            return await InsertSliceAsync(connStr, cfg, rowCount, startId: 0, ct);

        var perStream = rowCount / cfg.Streams;
        var tasks = new List<Task<long>>(cfg.Streams);
        for (var s = 0; s < cfg.Streams; s++)
        {
            var startId = (long)s * perStream;
            var count = s == cfg.Streams - 1 ? rowCount - startId : perStream; // last stream takes the remainder
            tasks.Add(InsertSliceAsync(connStr, cfg, count, startId, ct));
        }
        var written = await Task.WhenAll(tasks);
        return written.Sum();
    }

    private static async Task<long> InsertSliceAsync(
        string connStr, NativeInsertConfig cfg, long count, long startId, CancellationToken ct)
    {
        await using var conn = new NativeConnection(connStr);
        await conn.OpenAsync(ct);

        var options = new BulkInsertOptions { BatchSize = cfg.BatchSize };
        if (cfg.Path == NativePath.Typed)
            await conn.BulkInsertAsync(WideWorkload.Table, WideWorkload.StreamTyped(count, startId), options, ct);
        else
            await conn.BulkInsertAsync(
                WideWorkload.Table, WideWorkload.Columns, WideWorkload.StreamObjects(count, startId), options, ct);
        return count;
    }

    /// <summary>Rewrites the <c>Compress</c>/<c>CompressionMethod</c> keys of the native connection
    /// string to match the requested codec, leaving every other key untouched.</summary>
    private static string ApplyCompression(string connStr, NativeCompression c)
    {
        var parts = connStr
            .Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(p => !p.StartsWith("Compress", StringComparison.OrdinalIgnoreCase)) // covers Compress* + CompressionMethod
            .ToList();

        switch (c)
        {
            case NativeCompression.Off:
                parts.Add("Compress=false");
                break;
            case NativeCompression.Zstd:
                parts.Add("Compress=true");
                parts.Add("CompressionMethod=Zstd");
                break;
            default: // Lz4
                parts.Add("Compress=true");
                parts.Add("CompressionMethod=Lz4");
                break;
        }
        return string.Join(";", parts);
    }
}
