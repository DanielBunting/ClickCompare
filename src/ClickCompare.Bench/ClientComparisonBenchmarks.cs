using BenchmarkDotNet.Attributes;
using ClickCompare.Core;
using DriverConnection = ClickHouse.Driver.ADO.ClickHouseConnection;
using NativeConnection = CH.Native.Connection.ClickHouseConnection;

namespace ClickCompare.Bench;

/// <summary>
/// Head-to-head: CH.Native (latest) vs ClickHouse.Driver / clickhouse-cs (latest), same 1M-row
/// workload, same server. The Driver runs at its default staircase, best multi-stream, and tuned
/// single-INSERT points so the native ceiling can be read against each — answering both
/// "native vs clickhouse-cs" and "how much of the gap survives once the Driver is tuned".
/// </summary>
[Config(typeof(BenchConfig))]
public class ClientComparisonBenchmarks
{
    private DriverConnection _driver = null!;
    private NativeConnection _native = null!;
    private List<object[]> _objectRows = null!;
    private List<BulkRow> _typedRows = null!;

    [ParamsSource(nameof(CaseSource))]
    public ComparisonCase Case { get; set; } = null!;

    public static IEnumerable<ComparisonCase> CaseSource => ComparisonCases.All;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        await ClickHouseFixture.StartAsync();

        _driver = new DriverConnection(ClickHouseFixture.ConnectionString);
        await _driver.OpenAsync();

        _native = new NativeConnection(ClickHouseFixture.NativeConnectionString);
        await _native.OpenAsync(default);

        await Workload.ResetTableAsync(_driver);

        _objectRows = Workload.Build(Case.RowCount);
        _typedRows = BulkRow.Build(Case.RowCount);
    }

    // DDL is client-agnostic; truncate over the warm HTTP connection, outside the timed region.
    [IterationSetup]
    public void IterationSetup() => Workload.TruncateAsync(_driver).GetAwaiter().GetResult();

    [Benchmark]
    public async Task<long> Insert() =>
        await ComparisonRunner.RunAsync(Case, _driver, _native, _objectRows, _typedRows);

    [GlobalCleanup]
    public async Task GlobalCleanup()
    {
        if (_driver is not null) await _driver.DisposeAsync();
        if (_native is not null) await _native.CloseAsync();
    }
}
