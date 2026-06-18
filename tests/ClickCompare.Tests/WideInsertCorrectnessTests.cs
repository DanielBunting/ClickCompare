using System.Diagnostics;
using ClickCompare.Core;
using ClickHouse.Driver.ADO;
using Xunit;

namespace ClickCompare.Tests;

/// <summary>
/// Correctness gate for every wide-insert config (both clients). A faster config is only a win if it
/// still inserts the wide row set <b>exactly</b> — so each config runs at a modest row count and we
/// assert: the client-reported count, the server <c>count()</c>, the <c>sum(id)</c> checksum (no
/// dropped/duplicated rows), and a per-column round-trip on sampled ids (GUID, both Decimals, all five
/// DateTime64(3) timestamps survive serialization intact). This also smoke-tests that each tuning knob
/// — schema cache, column types, RowBinaryWithDefaults, async_insert, mass parallelism, Zstd, the
/// dynamic path — actually executes against a real server, not just compiles.
/// </summary>
[Collection(ClickHouseCollection.Name)]
public class WideInsertCorrectnessTests
{
    // Big enough to exercise multi-batch (10k batches → 5 batches) and 8-way stream fan-out, small
    // enough to keep ~28 configs fast.
    private const int Rows = 50_000;

    // async_insert + wait_for_async_insert=0 returns before the server flushes, so counts are
    // eventually-consistent — poll rather than read once.
    private static readonly TimeSpan FlushTimeout = TimeSpan.FromSeconds(20);

    public static IEnumerable<object[]> DriverConfigs() =>
        DriverInsertConfigs.All.Select(c => new object[] { c });

    public static IEnumerable<object[]> NativeConfigs() =>
        NativeInsertConfigs.All.Select(c => new object[] { c });

    [Theory]
    [MemberData(nameof(DriverConfigs))]
    public async Task Driver_config_inserts_every_wide_row_exactly_once(DriverInsertConfig config)
    {
        await using var conn = await FreshTableAsync();
        var reported = await DriverWideInsertRunner.RunAsync(ClickHouseFixture.ConnectionString, config, Rows);
        await AssertInsertedCleanlyAsync(conn, reported);
    }

    [Theory]
    [MemberData(nameof(NativeConfigs))]
    public async Task Native_config_inserts_every_wide_row_exactly_once(NativeInsertConfig config)
    {
        await using var conn = await FreshTableAsync();
        var reported = await NativeWideInsertRunner.RunAsync(ClickHouseFixture.NativeConnectionString, config, Rows);
        await AssertInsertedCleanlyAsync(conn, reported);
    }

    private static async Task<ClickHouseConnection> FreshTableAsync()
    {
        var conn = new ClickHouseConnection(ClickHouseFixture.ConnectionString);
        await conn.OpenAsync();
        await WideWorkload.ResetTableAsync(conn);
        return conn;
    }

    private static async Task AssertInsertedCleanlyAsync(ClickHouseConnection conn, long reported)
    {
        Assert.Equal(Rows, reported);

        var count = await WaitForCountAsync(conn, Rows, FlushTimeout);
        Assert.Equal(Rows, count);
        Assert.Equal(WideWorkload.ExpectedIdSum(Rows), await WideWorkload.SumIdAsync(conn));

        await AssertSampleRoundTripAsync(conn);
    }

    // Pull a handful of rows back and confirm every column survived the round trip byte-for-byte.
    private static async Task AssertSampleRoundTripAsync(ClickHouseConnection conn)
    {
        long[] ids = { 0, 1, 12_345, Rows / 2, Rows - 1 };

        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            "SELECT id, guid, d_a, d_b, l_a, l_b, l_c, " +
            "toUnixTimestamp64Milli(ts_created), toUnixTimestamp64Milli(ts_updated), " +
            "toUnixTimestamp64Milli(ts_event), toUnixTimestamp64Milli(ts_ingested), " +
            "toUnixTimestamp64Milli(ts_expiry) " +
            $"FROM {WideWorkload.Table} WHERE id IN ({string.Join(',', ids)}) ORDER BY id";

        var seen = new List<long>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var id = reader.GetInt64(0);
            var expected = WideWorkload.Generate(id);
            seen.Add(id);

            Assert.Equal(expected.Guid, reader.GetFieldValue<Guid>(1));
            Assert.Equal(expected.DA, reader.GetDecimal(2));
            Assert.Equal(expected.DB, reader.GetDecimal(3));
            Assert.Equal(expected.LA, reader.GetInt64(4));
            Assert.Equal(expected.LB, reader.GetInt64(5));
            Assert.Equal(expected.LC, reader.GetInt64(6));
            Assert.Equal(WideRow.UnixMs(expected.TsCreated), reader.GetInt64(7));
            Assert.Equal(WideRow.UnixMs(expected.TsUpdated), reader.GetInt64(8));
            Assert.Equal(WideRow.UnixMs(expected.TsEvent), reader.GetInt64(9));
            Assert.Equal(WideRow.UnixMs(expected.TsIngested), reader.GetInt64(10));
            Assert.Equal(WideRow.UnixMs(expected.TsExpiry), reader.GetInt64(11));
        }

        Assert.Equal(ids.OrderBy(x => x).ToArray(), seen.ToArray());
    }

    private static async Task<long> WaitForCountAsync(ClickHouseConnection conn, long expected, TimeSpan timeout)
    {
        var sw = Stopwatch.StartNew();
        long count;
        do
        {
            count = await WideWorkload.CountAsync(conn);
            if (count >= expected) break;
            await Task.Delay(100);
        }
        while (sw.Elapsed < timeout);
        return count;
    }
}
