using System.Collections.Concurrent;
using BenchmarkDotNet.Attributes;
using ClickCompare.Core;
using ClickHouse.Driver.ADO;

namespace ClickCompare.Bench;

/// <summary>
/// Proves out the multi-stream-start hypothesis for the clickhouse-cs client
/// (<c>ClickHouseBulkCopy</c>): sweep MaxDegreeOfParallelism × BatchSize × RowCount and watch
/// whether starting multiple insert streams collapses the sequential per-INSERT staircase
/// described in ingestion-comparisson.md §3.
/// </summary>
[Config(typeof(BenchConfig))]
public class MultiStreamBenchmarks
{
    // Generated rows are cached per row-count so scenarios sharing a size don't rebuild them.
    private static readonly ConcurrentDictionary<int, List<object[]>> RowCache = new();

    private ClickHouseConnection _conn = null!;
    private List<object[]> _rows = null!;

    [ParamsSource(nameof(ScenarioSource))]
    public InsertScenario Scenario { get; set; } = null!;

    public static IEnumerable<InsertScenario> ScenarioSource => Scenarios.All;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        await ClickHouseFixture.StartAsync();
        _conn = new ClickHouseConnection(ClickHouseFixture.ConnectionString);
        await _conn.OpenAsync();
        await Workload.ResetTableAsync(_conn);
        _rows = RowCache.GetOrAdd(Scenario.RowCount, Workload.Build);
    }

    // Truncate before every measured insert so each iteration starts from an empty table.
    // Runs outside the timed region, so its cost is not attributed to the benchmark.
    [IterationSetup]
    public void IterationSetup() => Workload.TruncateAsync(_conn).GetAwaiter().GetResult();

    [Benchmark]
    public async Task<long> BulkInsert() =>
        await BulkInsertRunner.RunAsync(_conn, Scenario, _rows);

    [GlobalCleanup]
    public async Task GlobalCleanup()
    {
        if (_conn is not null)
            await _conn.DisposeAsync();
    }
}
