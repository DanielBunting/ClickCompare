using ClickCompare.Core;
using ClickHouse.Driver.ADO;
using Xunit;
using NativeConnection = CH.Native.Connection.ClickHouseConnection;

namespace ClickCompare.Tests;

/// <summary>
/// Every Parquet ingestion route must land the same rows as any other path — a fast decode that
/// drops or reorders rows is not a win. Runs each route from <see cref="ParquetCases"/> against a
/// real ClickHouse at a reduced row count and asserts exact count + sum(id) checksum. Doubles as the
/// smoke test for the new plumbing: HTTP fixture URI, container file copy, and Parquet.Net authoring.
/// </summary>
[Collection(ClickHouseCollection.Name)]
public class ParquetIngestionCorrectnessTests
{
    // Smaller than the 1M benchmark size — correctness doesn't need scale, and these run per-case.
    private const int Rows = 50_000;

    public static IEnumerable<object[]> Routes() =>
        ParquetCases.All.Select(c => new object[] { c.Route });

    [Theory]
    [MemberData(nameof(Routes))]
    public async Task Route_inserts_every_row_exactly_once(ParquetRoute route)
    {
        await using var driver = new ClickHouseConnection(ClickHouseFixture.ConnectionString);
        await driver.OpenAsync();
        await Workload.ResetTableAsync(driver);

        await using var native = new NativeConnection(ClickHouseFixture.NativeConnectionString);
        await native.OpenAsync(default);

        using var http = ClickHouseHttp.ForFixture();

        var reported = await RunRouteAsync(route, driver, native, http, Rows);

        var count = await Workload.CountAsync(driver);
        var sumId = await Workload.SumIdAsync(driver);
        var expectedSum = (long)Rows * (Rows - 1) / 2; // 0 + 1 + … + (n-1)

        Assert.Equal(Rows, reported);
        Assert.Equal(Rows, count);
        Assert.Equal(expectedSum, sumId);
    }

    private static async Task<long> RunRouteAsync(
        ParquetRoute route, ClickHouseConnection driver, NativeConnection native, ClickHouseHttp http, int rows)
    {
        switch (route)
        {
            case ParquetRoute.ServerGenHttpPost:
            {
                var bytes = await ParquetWorkload.GenerateViaServerAsync(http, rows);
                return await ParquetWorkload.PostInsertAsync(http, bytes, rows);
            }
            case ParquetRoute.ServerGenFileLocal:
            {
                var bytes = await ParquetWorkload.GenerateViaServerAsync(http, rows);
                await ClickHouseFixture.CopyFileToServerAsync(bytes, ParquetWorkload.ServerFileName);
                return await ParquetWorkload.FileIngestAsync(driver, rows);
            }
            case ParquetRoute.DotNetWriteHttpPost:
            {
                var bytes = await ParquetWorkload.WriteWithParquetNetAsync(ParquetRow.Build(rows));
                return await ParquetWorkload.PostInsertAsync(http, bytes, rows);
            }
            case ParquetRoute.NativeReference:
                return await ParquetWorkload.NativeReferenceAsync(native, BulkRow.Build(rows));
            default:
                throw new ArgumentOutOfRangeException(nameof(route));
        }
    }
}
