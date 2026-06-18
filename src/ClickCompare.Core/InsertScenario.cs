using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Copy;

namespace ClickCompare.Core;

/// <summary>
/// One point in the (rows × batch size × stream count) space the investigation explores.
/// <para>
/// The hypothesis under test: the clickhouse-cs client (<see cref="ClickHouseBulkCopy"/>) splits an
/// insert into <see cref="BatchSize"/>-row batches, each a separate HTTP INSERT carrying a fixed
/// ~60–80 ms server commit cost. By default those batches run sequentially
/// (<see cref="Mdop"/> = 1), so the fixed cost is paid N times. Starting multiple streams at once
/// (<see cref="Mdop"/> &gt; 1) should overlap that fixed cost and collapse the staircase.
/// </para>
/// </summary>
public sealed record InsertScenario(string Name, int RowCount, int BatchSize, int Mdop)
{
    /// <summary>How many separate HTTP INSERTs this scenario will issue.</summary>
    public int BatchCount => (RowCount + BatchSize - 1) / BatchSize;

    public override string ToString() => Name;
}

/// <summary>Executes a scenario through ClickHouse.Driver's bulk-copy API.</summary>
public static class BulkInsertRunner
{
    /// <summary>
    /// Run a bulk insert and return the row count the driver reports written.
    /// Reuses a warm <paramref name="conn"/> so connection-open cost stays out of the measurement.
    /// </summary>
    public static async Task<long> RunAsync(
        ClickHouseConnection conn, InsertScenario scenario, IEnumerable<object[]> rows, CancellationToken ct = default)
    {
        using var bulk = new ClickHouseBulkCopy(conn)
        {
            DestinationTableName = Workload.Table,
            ColumnNames = Workload.Columns,
            BatchSize = scenario.BatchSize,
            MaxDegreeOfParallelism = scenario.Mdop,
        };

        await bulk.InitAsync();
        await bulk.WriteToServerAsync(rows, ct);
        return bulk.RowsWritten;
    }
}
