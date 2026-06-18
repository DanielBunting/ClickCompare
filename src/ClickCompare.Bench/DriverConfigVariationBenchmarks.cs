using BenchmarkDotNet.Attributes;
using ClickCompare.Core;
using DriverConnection = ClickHouse.Driver.ADO.ClickHouseConnection;

namespace ClickCompare.Bench;

/// <summary>
/// Sweeps ClickHouse.Driver's insert tuning knobs over the wide workload (10M rows by default,
/// configurable via <c>WIDE_ROWS</c>), one variable at a
/// time against a shared base (see <see cref="DriverInsertConfigs"/>): BatchSize, MaxDegreeOfParallelism,
/// schema-probe handling (UseSchemaCache / ColumnTypes), wire Format, server-side settings
/// (async_insert, max_insert_threads, parallel parsing) and app-level "mass parallelism" (N independent
/// clients). Reads the throughput cost/benefit of each against the same base so deltas are attributable.
/// </summary>
[Config(typeof(WideBenchConfig))]
public class DriverConfigVariationBenchmarks
{
    private DriverConnection _admin = null!; // DDL/truncate only; the insert runs on a fresh client per config
    private long _rows;

    [ParamsSource(nameof(ConfigSource))]
    public DriverInsertConfig Config { get; set; } = null!;

    public static IEnumerable<DriverInsertConfig> ConfigSource => DriverInsertConfigs.All;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        _rows = WideBenchConfig.RowCount;
        await ClickHouseFixture.StartAsync();
        _admin = new DriverConnection(ClickHouseFixture.ConnectionString);
        await _admin.OpenAsync();
        await WideWorkload.ResetTableAsync(_admin);
    }

    // Empty the table before each measured insert, outside the timed region.
    [IterationSetup]
    public void IterationSetup() => WideWorkload.TruncateAsync(_admin).GetAwaiter().GetResult();

    [Benchmark]
    public async Task<long> Insert() =>
        await DriverWideInsertRunner.RunAsync(ClickHouseFixture.ConnectionString, Config, _rows);

    [GlobalCleanup]
    public async Task GlobalCleanup()
    {
        if (_admin is not null) await _admin.DisposeAsync();
    }
}
