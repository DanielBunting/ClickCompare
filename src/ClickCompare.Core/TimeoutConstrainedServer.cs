using System.Globalization;
using System.Text;
using Testcontainers.ClickHouse;

namespace ClickCompare.Core;

/// <summary>
/// A separate ClickHouse container whose <c>max_execution_time</c> is baked in <b>at setup</b> via a
/// mounted profile config — not applied at runtime. It's intentionally its own server: a short
/// query timeout would fail every insert in the performance suite, so the resilience test gets a
/// dedicated, timeout-constrained instance.
/// </summary>
public sealed class TimeoutConstrainedServer : IAsyncDisposable
{
    private readonly ClickHouseContainer _container;

    public string ConnectionString { get; }
    public string NativeConnectionString { get; }
    public double MaxExecutionTimeSeconds { get; }

    private TimeoutConstrainedServer(ClickHouseContainer container, double seconds)
    {
        _container = container;
        MaxExecutionTimeSeconds = seconds;
        ConnectionString = ClickHouseFixture.BuildHttpConnectionString(container);
        NativeConnectionString = ClickHouseFixture.BuildNativeConnectionString(container);
    }

    public static async Task<TimeoutConstrainedServer> StartAsync(
        double maxExecutionTimeSeconds, CancellationToken ct = default)
    {
        // Drop a users.d profile override into the image so every query the default user runs is
        // capped at max_execution_time. ClickHouse loads /etc/clickhouse-server/users.d/*.xml on boot.
        var seconds = maxExecutionTimeSeconds.ToString(CultureInfo.InvariantCulture);
        var profileXml =
            "<clickhouse>\n" +
            "  <profiles>\n" +
            "    <default>\n" +
            $"      <max_execution_time>{seconds}</max_execution_time>\n" +
            "    </default>\n" +
            "  </profiles>\n" +
            "</clickhouse>\n";

        var container = new ClickHouseBuilder()
            .WithImage(ClickHouseFixture.Image)
            .WithPassword(ClickHouseFixture.Password)
            .WithResourceMapping(
                Encoding.UTF8.GetBytes(profileXml),
                "/etc/clickhouse-server/users.d/zz-max-execution-time.xml")
            .Build();

        await container.StartAsync(ct);
        return new TimeoutConstrainedServer(container, maxExecutionTimeSeconds);
    }

    public async ValueTask DisposeAsync() => await _container.DisposeAsync();
}
