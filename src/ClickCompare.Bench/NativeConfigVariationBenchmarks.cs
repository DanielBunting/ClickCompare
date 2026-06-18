using BenchmarkDotNet.Attributes;
using ClickCompare.Core;
using DriverConnection = ClickHouse.Driver.ADO.ClickHouseConnection;

namespace ClickCompare.Bench;

/// <summary>
/// Sweeps CH.Native's (fewer) insert tuning knobs over the wide workload (10M rows by default,
/// configurable via <c>WIDE_ROWS</c>), one variable at a
/// time against a shared base (see <see cref="NativeInsertConfigs"/>): BatchSize, compression
/// (off / LZ4 / Zstd), typed-vs-dynamic serialization path, and app-level parallelism (N independent
/// connections — the only concurrency dial CH.Native offers). DDL/truncate run over an HTTP Driver
/// connection (client-agnostic); the timed insert runs over the native TCP protocol.
/// </summary>
[Config(typeof(WideBenchConfig))]
public class NativeConfigVariationBenchmarks
{
    private DriverConnection _admin = null!; // DDL/truncate over HTTP; insert runs over native TCP
    private long _rows;

    [ParamsSource(nameof(ConfigSource))]
    public NativeInsertConfig Config { get; set; } = null!;

    public static IEnumerable<NativeInsertConfig> ConfigSource => NativeInsertConfigs.All;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        _rows = WideBenchConfig.RowCount;
        await ClickHouseFixture.StartAsync();
        _admin = new DriverConnection(ClickHouseFixture.ConnectionString);
        await _admin.OpenAsync();
        await WideWorkload.ResetTableAsync(_admin);
    }

    [IterationSetup]
    public void IterationSetup() => WideWorkload.TruncateAsync(_admin).GetAwaiter().GetResult();

    [Benchmark]
    public async Task<long> Insert() =>
        await NativeWideInsertRunner.RunAsync(ClickHouseFixture.NativeConnectionString, Config, _rows);

    [GlobalCleanup]
    public async Task GlobalCleanup()
    {
        if (_admin is not null) await _admin.DisposeAsync();
    }
}
