using BenchmarkDotNet.Attributes;
using ClickCompare.Core;
using DriverConnection = ClickHouse.Driver.ADO.ClickHouseConnection;
using NativeConnection = CH.Native.Connection.ClickHouseConnection;

namespace ClickCompare.Bench;

/// <summary>
/// The §6 bulk-load comparison at 20M rows (20 × 1M chunks): server-local Parquet <c>file()</c> against
/// 20 sequential 1M inserts on each client, every client tuned for 1M writes. The Parquet transfer is
/// timed in its own right — a copy-only row and a copy+ingest row — so the file route is read both as
/// "decode only" (bytes already on the box) and end-to-end, the way it actually competes with streaming.
/// Uses the low-iteration <see cref="WideBenchConfig"/> (1 warmup + 3 measured, no memory diagnoser):
/// each op moves 20M rows, so the story here is wall-clock / throughput, not allocations.
/// </summary>
[Config(typeof(WideBenchConfig))]
public class BulkLoad20MBenchmarks
{
    private DriverConnection _driver = null!;
    private NativeConnection _native = null!;
    private ClickHouseHttp _http = null!;

    private byte[] _chunkParquet = null!;       // one 1M-row Parquet payload, reused for every chunk
    private List<BulkRow> _typedRows = null!;    // one 1M-row chunk for the native route
    private List<object[]> _objectRows = null!;  // one 1M-row chunk for the driver route

    [ParamsSource(nameof(CaseSource))]
    public BulkLoadCase Case { get; set; } = null!;

    public static IEnumerable<BulkLoadCase> CaseSource => BulkLoadCases.All;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        await ClickHouseFixture.StartAsync();

        _driver = new DriverConnection(ClickHouseFixture.ConnectionString);
        await _driver.OpenAsync();

        _native = new NativeConnection(ClickHouseFixture.NativeConnectionString);
        await _native.OpenAsync(default);

        _http = ClickHouseHttp.ForFixture();

        await Workload.ResetTableAsync(_driver);

        // Build only the payload this route needs; for file-ingest, pre-stage the chunks (the copy is
        // not what that row measures), for copy/copy+ingest leave staging to the timed region.
        switch (Case.Route)
        {
            case BulkLoadRoute.ParquetFileIngest:
                _chunkParquet = await ParquetWorkload.GenerateViaServerAsync(_http, Case.RowsPerChunk);
                await BulkLoadRunner.CopyChunksAsync(_chunkParquet, Case.Chunks);
                break;
            case BulkLoadRoute.ParquetCopyToServer:
            case BulkLoadRoute.ParquetCopyAndIngest:
            case BulkLoadRoute.ParquetHttpSequential:
                _chunkParquet = await ParquetWorkload.GenerateViaServerAsync(_http, Case.RowsPerChunk);
                break;
            case BulkLoadRoute.NativeSequential:
                _typedRows = BulkRow.Build(Case.RowsPerChunk);
                break;
            case BulkLoadRoute.DriverSequential:
                _objectRows = Workload.Build(Case.RowsPerChunk);
                break;
        }
    }

    [IterationSetup]
    public void IterationSetup() => Workload.TruncateAsync(_driver).GetAwaiter().GetResult();

    [Benchmark]
    public async Task<long> Load() => Case.Route switch
    {
        BulkLoadRoute.ParquetFileIngest => await BulkLoadRunner.FileIngestAsync(_driver, Case.TotalRows),
        BulkLoadRoute.ParquetCopyToServer => await CopyOnlyAsync(),
        BulkLoadRoute.ParquetCopyAndIngest => await CopyAndIngestAsync(),
        BulkLoadRoute.ParquetHttpSequential =>
            await BulkLoadRunner.HttpSequentialAsync(_http, _chunkParquet, Case.Chunks, Case.RowsPerChunk),
        BulkLoadRoute.NativeSequential =>
            await BulkLoadRunner.NativeSequentialAsync(_native, _typedRows, Case.Chunks),
        BulkLoadRoute.DriverSequential =>
            await BulkLoadRunner.DriverSequentialAsync(_driver, _objectRows, Case.Chunks),
        _ => throw new ArgumentOutOfRangeException(),
    };

    // Copy-only: stage the 20 chunks and stop — this row is the transfer cost in isolation, so it
    // ingests nothing (the table stays empty).
    private async Task<long> CopyOnlyAsync()
    {
        await BulkLoadRunner.CopyChunksAsync(_chunkParquet, Case.Chunks);
        return 0;
    }

    private async Task<long> CopyAndIngestAsync()
    {
        await BulkLoadRunner.CopyChunksAsync(_chunkParquet, Case.Chunks);
        return await BulkLoadRunner.FileIngestAsync(_driver, Case.TotalRows);
    }

    [GlobalCleanup]
    public async Task GlobalCleanup()
    {
        _http?.Dispose();
        if (_driver is not null) await _driver.DisposeAsync();
        if (_native is not null) await _native.CloseAsync();
    }
}
