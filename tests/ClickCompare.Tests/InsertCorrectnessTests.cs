using ClickCompare.Core;
using ClickHouse.Driver.ADO;
using Xunit;
using NativeConnection = CH.Native.Connection.ClickHouseConnection;

namespace ClickCompare.Tests;

/// <summary>
/// A fast insert is only interesting if it inserts everything. These tests run every benchmark
/// scenario against a real ClickHouse and assert exact row count + a sum(id) checksum — so a
/// multi-stream config that silently drops or duplicates rows can't pose as a performance win.
/// They also act as the harness smoke test (container boots, schema applies, driver connects).
/// </summary>
[Collection(ClickHouseCollection.Name)]
public class InsertCorrectnessTests
{
    public static IEnumerable<object[]> Scenarios() =>
        ClickCompare.Core.Scenarios.All.Select(s => new object[] { s });

    [Theory]
    [MemberData(nameof(Scenarios))]
    public async Task Scenario_inserts_every_row_exactly_once(InsertScenario scenario)
    {
        await using var conn = new ClickHouseConnection(ClickHouseFixture.ConnectionString);
        await conn.OpenAsync();
        await Workload.ResetTableAsync(conn);

        var rows = Workload.Build(scenario.RowCount);
        var reported = await BulkInsertRunner.RunAsync(conn, scenario, rows);

        var count = await Workload.CountAsync(conn);
        var sumId = await Workload.SumIdAsync(conn);
        var expectedSum = (long)scenario.RowCount * (scenario.RowCount - 1) / 2; // 0 + 1 + ... + (n-1)

        Assert.Equal(scenario.RowCount, reported);
        Assert.Equal(scenario.RowCount, count);
        Assert.Equal(expectedSum, sumId);
    }

    public static IEnumerable<object[]> Comparisons() =>
        ComparisonCases.All.Select(c => new object[] { c });

    [Theory]
    [MemberData(nameof(Comparisons))]
    public async Task Comparison_case_inserts_every_row_exactly_once(ComparisonCase comparison)
    {
        // DDL + verification over HTTP; the insert itself runs on whichever client the case names.
        await using var driver = new ClickHouseConnection(ClickHouseFixture.ConnectionString);
        await driver.OpenAsync();
        await Workload.ResetTableAsync(driver);

        await using var native = new NativeConnection(ClickHouseFixture.NativeConnectionString);
        await native.OpenAsync(default);

        var objectRows = Workload.Build(comparison.RowCount);
        var typedRows = BulkRow.Build(comparison.RowCount);
        var reported = await ComparisonRunner.RunAsync(comparison, driver, native, objectRows, typedRows);

        var count = await Workload.CountAsync(driver);
        var sumId = await Workload.SumIdAsync(driver);
        var expectedSum = (long)comparison.RowCount * (comparison.RowCount - 1) / 2;

        Assert.Equal(comparison.RowCount, reported);
        Assert.Equal(comparison.RowCount, count);
        Assert.Equal(expectedSum, sumId);
    }

    [Fact]
    public void Batch_count_matches_expected_stream_fan_out()
    {
        // Documents the mechanism the benchmark measures: BatchSize → number of HTTP INSERTs.
        var def = new InsertScenario("default", 1_000_000, 100_000, 1);
        Assert.Equal(10, def.BatchCount); // 10 sequential INSERTs → the 929 ms staircase

        var tuned = new InsertScenario("one-insert", 1_000_000, 1_000_000, 1);
        Assert.Equal(1, tuned.BatchCount); // single INSERT → fixed cost paid once
    }
}
