using Parquet.Serialization;
using DriverConnection = ClickHouse.Driver.ADO.ClickHouseConnection;
using NativeConnection = CH.Native.Connection.ClickHouseConnection;

namespace ClickCompare.Core;

/// <summary>
/// The Parquet ingestion routes from ingestion-comparisson.md §6, ported onto the harness. Two
/// questions are kept distinct:
/// <list type="bullet">
///   <item><b>Server-side decode</b> — fixtures minted by ClickHouse itself (<c>SELECT … FORMAT
///   Parquet</c>), shipped raw. Measures only the server's decode+ingest, the doc-faithful comparison.</item>
///   <item><b>Data born in the application</b> — rows authored to Parquet in-process with Parquet.Net,
///   then shipped. Folds the .NET write cost into the timed path, the apples-to-apples client story
///   against CH.Native's streamed serialization.</item>
/// </list>
/// Both feed the same (id, name, value) workload as every other benchmark.
/// </summary>
public sealed class ParquetRow
{
    public long Id { get; set; }
    public string Name { get; set; } = "";
    public double Value { get; set; }

    public static List<ParquetRow> Build(int rowCount)
    {
        var rows = new List<ParquetRow>(rowCount);
        for (long i = 0; i < rowCount; i++)
            rows.Add(new ParquetRow { Id = i, Name = "BulkItem_" + i, Value = i * 1.5 });
        return rows;
    }
}

public static class ParquetWorkload
{
    /// <summary>The fixture file name dropped under the server's <c>user_files</c> dir for the
    /// <c>file()</c> route.</summary>
    public const string ServerFileName = "fixture.parquet";

    // Parquet.Net emits PascalCase column names (Id/Name/Value); the table is lower-case. Rather than
    // decorate the POCO, let the server match case-insensitively — same effect, one place to reason about.
    private static readonly Dictionary<string, string> CaseInsensitiveMatch =
        new() { ["input_format_parquet_case_insensitive_column_matching"] = "1" };

    /// <summary>Mint a Parquet fixture by having ClickHouse serialize the canonical workload — no .NET
    /// Parquet writer in the loop, so a subsequent insert measures pure server decode.</summary>
    public static Task<byte[]> GenerateViaServerAsync(ClickHouseHttp http, int rowCount, CancellationToken ct = default) =>
        http.QueryBytesAsync(
            "SELECT toInt64(number) AS id, concat('BulkItem_', toString(number)) AS name, " +
            $"number * 1.5 AS value FROM numbers({rowCount}) FORMAT Parquet", ct);

    /// <summary>Author the rows to an in-memory Parquet file with Parquet.Net — the "data born in the
    /// application" producer. Call this inside the timed region to count the write cost.</summary>
    public static async Task<byte[]> WriteWithParquetNetAsync(IReadOnlyList<ParquetRow> rows, CancellationToken ct = default)
    {
        using var ms = new MemoryStream();
        await ParquetSerializer.SerializeAsync(rows, ms, cancellationToken: ct);
        return ms.ToArray();
    }

    /// <summary>Route 1/3: ship a Parquet payload as a single raw <c>INSERT … FORMAT Parquet</c> POST.</summary>
    public static async Task<long> PostInsertAsync(
        ClickHouseHttp http, byte[] parquet, long rowCount, CancellationToken ct = default)
    {
        await http.PostBodyAsync($"INSERT INTO {Workload.Table} FORMAT Parquet", parquet, CaseInsensitiveMatch, ct);
        return rowCount;
    }

    /// <summary>Route 2: server-local bulk load — one INSERT, server decodes the on-disk file across all
    /// cores. Assumes the fixture was already copied via <see cref="ClickHouseFixture.CopyFileToServerAsync"/>.</summary>
    public static async Task<long> FileIngestAsync(
        DriverConnection driver, long rowCount, string fileName = ServerFileName, CancellationToken ct = default)
    {
        await using var cmd = driver.CreateCommand();
        // SELECT * is positional — works for both the lower-case server fixture and the PascalCase
        // Parquet.Net one, since column order (id, name, value) is identical in both.
        cmd.CommandText = $"INSERT INTO {Workload.Table} SELECT * FROM file('{fileName}', 'Parquet')";
        await cmd.ExecuteNonQueryAsync(ct);
        return rowCount;
    }

    /// <summary>Reference leg: CH.Native's streamed typed insert, so the Parquet report carries its own
    /// native baseline without cross-referencing another suite.</summary>
    public static Task<long> NativeReferenceAsync(
        NativeConnection native, IReadOnlyList<BulkRow> typedRows, CancellationToken ct = default) =>
        NativeBulkInsertRunner.RunTypedAsync(native, typedRows, ct: ct);
}

/// <summary>Which Parquet ingestion route a <see cref="ParquetCase"/> exercises.</summary>
public enum ParquetRoute
{
    /// <summary>Server-minted Parquet, shipped as one raw <c>INSERT … FORMAT Parquet</c> HTTP POST.</summary>
    ServerGenHttpPost,

    /// <summary>Server-minted Parquet sitting on the server's disk, bulk-loaded via <c>file()</c>.</summary>
    ServerGenFileLocal,

    /// <summary>Rows authored to Parquet in-process with Parquet.Net (write cost timed), then POSTed.</summary>
    DotNetWriteHttpPost,

    /// <summary>CH.Native streamed typed insert — the native baseline, for reference in the same report.</summary>
    NativeReference,
}

/// <summary>One row of the Parquet ingestion report.</summary>
public sealed record ParquetCase(string Name, ParquetRoute Route, int RowCount)
{
    public override string ToString() => Name;
}

/// <summary>
/// The Parquet routes at the headline 1M-row size, against the CH.Native streaming baseline — the
/// §6 comparison reproduced inside the BenchmarkDotNet harness.
/// </summary>
public static class ParquetCases
{
    public const int M1 = 1_000_000;

    public static readonly IReadOnlyList<ParquetCase> All = new[]
    {
        new ParquetCase("Parquet HTTP POST (server-gen, decode only)", ParquetRoute.ServerGenHttpPost, M1),
        new ParquetCase("Parquet file() (server-local bulk load)", ParquetRoute.ServerGenFileLocal, M1),
        new ParquetCase("Parquet HTTP POST (Parquet.Net write + ship)", ParquetRoute.DotNetWriteHttpPost, M1),
        new ParquetCase("CH.Native streamed (reference)", ParquetRoute.NativeReference, M1),
    };
}
