using BenchmarkDotNet.Attributes;
using ClickCompare.Core;
using DriverConnection = ClickHouse.Driver.ADO.ClickHouseConnection;
using NativeConnection = CH.Native.Connection.ClickHouseConnection;

namespace ClickCompare.Bench;

/// <summary>
/// Parquet ingestion routes from ingestion-comparisson.md §6, reproduced in the harness: server-decode
/// (HTTP POST of a server-minted file, and server-local <c>file()</c> bulk load) and the
/// data-born-in-the-app path (Parquet.Net writes the file in-process, then ships it). CH.Native's
/// streamed insert rides along as the in-report baseline.
/// <para>
/// Fixtures that aren't part of the question — the server-minted Parquet bytes, the on-disk copy, the
/// pre-built row list — are prepared in <see cref="GlobalSetup"/>, so each route times only its own work:
/// the HTTP routes time decode+ingest, the Parquet.Net route additionally times the write.
/// </para>
/// </summary>
[Config(typeof(BenchConfig))]
public class ParquetIngestionBenchmarks
{
    private DriverConnection _driver = null!;
    private NativeConnection _native = null!;
    private ClickHouseHttp _http = null!;

    private byte[] _serverParquet = null!;     // server-minted fixture (POST + file routes)
    private List<ParquetRow> _parquetRows = null!; // source rows for the Parquet.Net write route
    private List<BulkRow> _typedRows = null!;   // source rows for the native baseline

    [ParamsSource(nameof(CaseSource))]
    public ParquetCase Case { get; set; } = null!;

    public static IEnumerable<ParquetCase> CaseSource => ParquetCases.All;

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

        // Prepare only what this case needs — keep unrelated fixture cost out of the run entirely.
        switch (Case.Route)
        {
            case ParquetRoute.ServerGenHttpPost:
                _serverParquet = await ParquetWorkload.GenerateViaServerAsync(_http, Case.RowCount);
                break;
            case ParquetRoute.ServerGenFileLocal:
                _serverParquet = await ParquetWorkload.GenerateViaServerAsync(_http, Case.RowCount);
                await ClickHouseFixture.CopyFileToServerAsync(_serverParquet, ParquetWorkload.ServerFileName);
                break;
            case ParquetRoute.DotNetWriteHttpPost:
                _parquetRows = ParquetRow.Build(Case.RowCount); // rows exist; only the write+ship is timed
                break;
            case ParquetRoute.NativeReference:
                _typedRows = BulkRow.Build(Case.RowCount);
                break;
        }
    }

    // DDL is client-agnostic; truncate over the warm HTTP connection, outside the timed region.
    [IterationSetup]
    public void IterationSetup() => Workload.TruncateAsync(_driver).GetAwaiter().GetResult();

    [Benchmark]
    public async Task<long> Insert() => Case.Route switch
    {
        ParquetRoute.ServerGenHttpPost =>
            await ParquetWorkload.PostInsertAsync(_http, _serverParquet, Case.RowCount),
        ParquetRoute.ServerGenFileLocal =>
            await ParquetWorkload.FileIngestAsync(_driver, Case.RowCount),
        ParquetRoute.DotNetWriteHttpPost =>
            await ParquetWorkload.PostInsertAsync(
                _http, await ParquetWorkload.WriteWithParquetNetAsync(_parquetRows), Case.RowCount),
        ParquetRoute.NativeReference =>
            await ParquetWorkload.NativeReferenceAsync(_native, _typedRows),
        _ => throw new ArgumentOutOfRangeException(),
    };

    [GlobalCleanup]
    public async Task GlobalCleanup()
    {
        _http?.Dispose();
        if (_driver is not null) await _driver.DisposeAsync();
        if (_native is not null) await _native.CloseAsync();
    }
}
