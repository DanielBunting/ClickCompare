using ClickCompare.Core;
using ClickHouse.Driver.ADO;
using Xunit;
using NativeConnection = CH.Native.Connection.ClickHouseConnection;

namespace ClickCompare.Tests;

/// <summary>
/// The resilience flip-side of the performance story. ClickHouse's <c>max_execution_time</c> is a
/// <b>per-query</b> ceiling, so one streamed INSERT lives or dies on a single timeout budget, while
/// a batched insert resets the clock every batch. This proves a batched insert survives a timeout
/// that kills the equivalent single streamed INSERT — for both CH.Native and clickhouse-cs.
///
/// Uses its own <see cref="TimeoutConstrainedServer"/> (timeout configured at container setup), so
/// the short ceiling never touches the performance suite's shared container. One container per test
/// class — hence <see cref="IAsyncLifetime"/> here rather than the shared collection.
/// </summary>
public class TimeoutResilienceTests : IAsyncLifetime
{
    // Big enough that one INSERT blows the ceiling, while each 100K-row batch stays well under it.
    private const int Rows = 5_000_000;
    private const int BatchSize = 100_000;
    private const double TimeoutSeconds = 0.3;

    private TimeoutConstrainedServer _server = null!;

    public async Task InitializeAsync() => _server = await TimeoutConstrainedServer.StartAsync(TimeoutSeconds);

    public async Task DisposeAsync() => await _server.DisposeAsync();

    public static IEnumerable<object[]> Clients() => new[]
    {
        // (single-INSERT client, batched client) — same rows, same client, different fan-out.
        new object[] { Client.NativeTyped, Client.NativeTypedSplit },
        new object[] { Client.Driver, Client.Driver },
    };

    [Theory]
    [MemberData(nameof(Clients))]
    public async Task One_streamed_insert_times_out_where_the_batched_insert_survives(
        Client singleClient, Client batchedClient)
    {
        // Build only the shape this client consumes (object[] for the Driver, typed for CH.Native).
        var useNative = singleClient != Client.Driver;
        var objectRows = useNative ? new List<object[]>() : Workload.Build(Rows);
        var typedRows = useNative ? BulkRow.Build(Rows) : new List<BulkRow>();

        // Single = one INSERT (Driver: BatchSize = all rows; Native: one streamed INSERT).
        var single = new ComparisonCase("single", singleClient, Rows, useNative ? BatchSize : Rows);
        // Batched = many small INSERTs, each its own timeout budget.
        var batched = new ComparisonCase("batched", batchedClient, Rows, BatchSize);

        await using var admin = new ClickHouseConnection(_server.ConnectionString);
        await admin.OpenAsync();

        // (a) One INSERT — one budget for all 5M rows → expect a server-side timeout.
        await Workload.ResetTableAsync(admin);
        var singleError = await RunFreshAsync(single, objectRows, typedRows);
        Assert.NotNull(singleError);
        Assert.True(LooksLikeTimeout(singleError!), $"Expected a timeout, got: {Flatten(singleError!)}");

        // (b) Same rows, 100K-row batches — each INSERT resets the clock → expect success.
        await Workload.ResetTableAsync(admin);
        var batchedError = await RunFreshAsync(batched, objectRows, typedRows);
        Assert.Null(batchedError);
        Assert.Equal(Rows, await Workload.CountAsync(admin));
    }

    // A timed-out query can leave its connection unusable, so each phase gets fresh connections.
    private async Task<Exception?> RunFreshAsync(
        ComparisonCase c, List<object[]> objectRows, List<BulkRow> typedRows)
    {
        await using var driver = new ClickHouseConnection(_server.ConnectionString);
        await driver.OpenAsync();
        await using var native = new NativeConnection(_server.NativeConnectionString);
        await native.OpenAsync(default);
        return await Record.ExceptionAsync(() =>
            ComparisonRunner.RunAsync(c, driver, native, objectRows, typedRows));
    }

    private static bool LooksLikeTimeout(Exception e)
    {
        var text = Flatten(e);
        return text.Contains("imeout", StringComparison.OrdinalIgnoreCase)
            || text.Contains("exceeded", StringComparison.OrdinalIgnoreCase)
            || text.Contains("159", StringComparison.Ordinal); // TIMEOUT_EXCEEDED
    }

    private static string Flatten(Exception e)
    {
        var text = e.Message;
        for (var inner = e.InnerException; inner is not null; inner = inner.InnerException)
            text += " | " + inner.Message;
        return text;
    }
}
