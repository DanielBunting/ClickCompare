using CH.Native.BulkInsert;
using NativeConnection = CH.Native.Connection.ClickHouseConnection;

namespace ClickCompare.Core;

/// <summary>
/// Strongly-typed row for CH.Native's compiled columnar extractors (its intended fast path —
/// the one the investigation measured at ~150 ms/1M). Column names are mapped explicitly so the
/// schema (id, name, value) matches regardless of property casing.
/// </summary>
public sealed class BulkRow
{
    [Column(Name = "id")] public long Id { get; set; }
    [Column(Name = "name")] public string Name { get; set; } = "";
    [Column(Name = "value")] public double Value { get; set; }

    public static List<BulkRow> Build(int rowCount)
    {
        var rows = new List<BulkRow>(rowCount);
        for (long i = 0; i < rowCount; i++)
            rows.Add(new BulkRow { Id = i, Name = "BulkItem_" + i, Value = i * 1.5 });
        return rows;
    }
}

/// <summary>
/// Runs an insert through CH.Native's native-TCP bulk path. Both variants stream blocks inside a
/// <b>single</b> INSERT (there is no batch-per-INSERT staircase to parallelise here — that is the
/// clickhouse-cs-specific behaviour the multi-stream sweep targets). <see cref="BlockSize"/> is the
/// internal streaming block, not a separate query.
/// </summary>
public static class NativeBulkInsertRunner
{
    public const int BlockSize = 100_000;

    /// <summary>Typed path: compiled direct-to-buffer extractors, no per-cell boxing.</summary>
    public static async Task<long> RunTypedAsync(
        NativeConnection conn, IReadOnlyList<BulkRow> rows, int blockSize = BlockSize, CancellationToken ct = default)
    {
        var options = new BulkInsertOptions { BatchSize = blockSize };
        await conn.BulkInsertAsync(Workload.Table, rows, options, ct);
        return rows.Count;
    }

    /// <summary>
    /// Dynamic path: same object[] input shape as ClickHouse.Driver, over the native protocol.
    /// Isolates "native vs HTTP transport" from "compiled extractors vs boxed object[] serialization".
    /// </summary>
    public static async Task<long> RunDynamicAsync(
        NativeConnection conn, IReadOnlyCollection<object[]> rows, int blockSize = BlockSize, CancellationToken ct = default)
    {
        var options = new BulkInsertOptions { BatchSize = blockSize };
        await using var inserter = conn.CreateBulkInserter(Workload.Table, Workload.Columns, options);
        await inserter.InitAsync(ct);
        await inserter.AddRangeAsync(rows, ct);
        await inserter.CompleteAsync(ct);
        return rows.Count;
    }

    /// <summary>
    /// Deliberately mimic the clickhouse-cs default architecture on CH.Native: split the rows into
    /// <paramref name="chunkSize"/>-row chunks and send each as a <b>separate, sequential INSERT</b>
    /// (typed fast path). Strips away native's one-streamed-INSERT advantage so the leftover gap is
    /// pure protocol + serialization + per-INSERT server cost at identical fan-out.
    /// </summary>
    public static async Task<long> RunTypedMultiInsertAsync(
        NativeConnection conn, IReadOnlyList<BulkRow> rows, int chunkSize, CancellationToken ct = default)
    {
        var options = new BulkInsertOptions { BatchSize = chunkSize };
        long total = 0;
        for (var offset = 0; offset < rows.Count; offset += chunkSize)
        {
            var count = Math.Min(chunkSize, rows.Count - offset);
            await conn.BulkInsertAsync(Workload.Table, Slice(rows, offset, count), options, ct);
            total += count;
        }
        return total;
    }

    /// <summary>As <see cref="RunTypedMultiInsertAsync"/> but object[] input — same shape as the Driver default.</summary>
    public static async Task<long> RunDynamicMultiInsertAsync(
        NativeConnection conn, IReadOnlyList<object[]> rows, int chunkSize, CancellationToken ct = default)
    {
        var options = new BulkInsertOptions { BatchSize = chunkSize };
        long total = 0;
        for (var offset = 0; offset < rows.Count; offset += chunkSize)
        {
            var count = Math.Min(chunkSize, rows.Count - offset);
            await using var inserter = conn.CreateBulkInserter(Workload.Table, Workload.Columns, options);
            await inserter.InitAsync(ct);
            await inserter.AddRangeAsync(Slice(rows, offset, count), ct);
            await inserter.CompleteAsync(ct);
            total += count;
        }
        return total;
    }

    // Lazy window over a list — avoids copying a chunk per INSERT into the measured path.
    private static IEnumerable<T> Slice<T>(IReadOnlyList<T> list, int offset, int count)
    {
        for (var i = 0; i < count; i++)
            yield return list[offset + i];
    }
}
