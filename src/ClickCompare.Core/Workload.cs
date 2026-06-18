using ClickHouse.Driver.ADO;

namespace ClickCompare.Core;

/// <summary>
/// The exact workload from ingestion-comparisson.md:
///   schema (id Int64, name String, value Float64), MergeTree ORDER BY id,
///   rows like (i, "BulkItem_{i}", i * 1.5).
/// </summary>
public static class Workload
{
    public const string Table = "bulk_target";
    public static readonly string[] Columns = { "id", "name", "value" };

    /// <summary>
    /// Materialise the rows as the driver's native input shape (object[] per row with
    /// boxed long/double). Pre-building the list keeps row construction / boxing OUT of the
    /// measured insert so the benchmark isolates the variable under test (batching + streaming).
    /// To fold boxing back into the hot path, swap this for the lazy <see cref="Stream"/> below.
    /// </summary>
    public static List<object[]> Build(int rowCount)
    {
        var rows = new List<object[]>(rowCount);
        for (long i = 0; i < rowCount; i++)
            rows.Add(new object[] { i, "BulkItem_" + i, i * 1.5 });
        return rows;
    }

    /// <summary>Lazy variant — constructs each object[] on demand, including boxing in the hot path.</summary>
    public static IEnumerable<object[]> Stream(int rowCount)
    {
        for (long i = 0; i < rowCount; i++)
            yield return new object[] { i, "BulkItem_" + i, i * 1.5 };
    }

    public static async Task ResetTableAsync(ClickHouseConnection conn, CancellationToken ct = default)
    {
        await ExecAsync(conn, $"DROP TABLE IF EXISTS {Table}", ct);
        await ExecAsync(conn,
            $"CREATE TABLE {Table} (id Int64, name String, value Float64) ENGINE = MergeTree ORDER BY id", ct);
    }

    public static async Task TruncateAsync(ClickHouseConnection conn, CancellationToken ct = default) =>
        await ExecAsync(conn, $"TRUNCATE TABLE IF EXISTS {Table}", ct);

    public static async Task<long> CountAsync(ClickHouseConnection conn, CancellationToken ct = default)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT count() FROM {Table}";
        var scalar = await cmd.ExecuteScalarAsync(ct);
        return Convert.ToInt64(scalar);
    }

    /// <summary>Sum of id, used as a cheap data-integrity checksum in correctness tests.</summary>
    public static async Task<long> SumIdAsync(ClickHouseConnection conn, CancellationToken ct = default)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT sum(id) FROM {Table}";
        var scalar = await cmd.ExecuteScalarAsync(ct);
        return Convert.ToInt64(scalar);
    }

    private static async Task ExecAsync(ClickHouseConnection conn, string sql, CancellationToken ct)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
