using ClickCompare.Core;
using ClickHouse.Driver.ADO;
using Xunit;
using NativeConnection = CH.Native.Connection.ClickHouseConnection;

namespace ClickCompare.Tests;

/// <summary>
/// Validates the 20-chunk bulk-load wiring (the chunk loop, the <c>file()</c> glob, and the server
/// file copy) at a reduced size — 4 chunks × 25K rows — so it runs fast yet exercises every route. A
/// throughput route that loses or duplicates a chunk would show here as a wrong total count.
/// </summary>
[Collection(ClickHouseCollection.Name)]
public class BulkLoad20MCorrectnessTests
{
    private const int Chunks = 4;
    private const int RowsPerChunk = 25_000;

    public static IEnumerable<object[]> Routes() =>
        BulkLoadCases.All.Select(c => new object[] { c.Route });

    [Theory]
    [MemberData(nameof(Routes))]
    public async Task Route_loads_every_chunk(BulkLoadRoute route)
    {
        await using var driver = new ClickHouseConnection(ClickHouseFixture.ConnectionString);
        await driver.OpenAsync();
        await Workload.ResetTableAsync(driver);

        await using var native = new NativeConnection(ClickHouseFixture.NativeConnectionString);
        await native.OpenAsync(default);

        using var http = ClickHouseHttp.ForFixture();

        var reported = await RunRouteAsync(route, driver, native, http);
        var count = await Workload.CountAsync(driver);

        if (route == BulkLoadRoute.ParquetCopyToServer)
        {
            // Copy-only stages files but ingests nothing.
            Assert.Equal(0, reported);
            Assert.Equal(0, count);
            return;
        }

        long expected = (long)Chunks * RowsPerChunk;
        Assert.Equal(expected, reported);
        Assert.Equal(expected, count);
    }

    private static async Task<long> RunRouteAsync(
        BulkLoadRoute route, ClickHouseConnection driver, NativeConnection native, ClickHouseHttp http)
    {
        var parquet = await ParquetWorkload.GenerateViaServerAsync(http, RowsPerChunk);
        long total = (long)Chunks * RowsPerChunk;

        switch (route)
        {
            case BulkLoadRoute.ParquetFileIngest:
                await BulkLoadRunner.CopyChunksAsync(parquet, Chunks);
                return await BulkLoadRunner.FileIngestAsync(driver, total);
            case BulkLoadRoute.ParquetCopyToServer:
                await BulkLoadRunner.CopyChunksAsync(parquet, Chunks);
                return 0;
            case BulkLoadRoute.ParquetCopyBatchedToServer:
                // The benchmark route only times the copy, but here also ingest from the batched
                // location to prove the tar landed where file() can glob it.
                await BulkLoadRunner.CopyChunksBatchedAsync(parquet, Chunks);
                return await ParquetWorkload.FileIngestAsync(driver, total, BulkLoadRunner.BatchedGlobPattern);
            case BulkLoadRoute.ParquetCopyAndIngest:
                await BulkLoadRunner.CopyChunksAsync(parquet, Chunks);
                return await BulkLoadRunner.FileIngestAsync(driver, total);
            case BulkLoadRoute.ParquetHttpSequential:
                return await BulkLoadRunner.HttpSequentialAsync(http, parquet, Chunks, RowsPerChunk);
            case BulkLoadRoute.NativeSequential:
                return await BulkLoadRunner.NativeSequentialAsync(native, BulkRow.Build(RowsPerChunk), Chunks);
            case BulkLoadRoute.DriverSequential:
                return await BulkLoadRunner.DriverSequentialAsync(driver, Workload.Build(RowsPerChunk), Chunks);
            default:
                throw new ArgumentOutOfRangeException(nameof(route));
        }
    }
}
