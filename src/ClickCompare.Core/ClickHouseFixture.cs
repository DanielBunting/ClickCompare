using ClickHouse.Driver.ADO;
using Testcontainers.ClickHouse;

namespace ClickCompare.Core;

/// <summary>
/// Owns a single ClickHouse container for the lifetime of the process and hands out
/// a warm ClickHouse.Driver connection string. Shared by both the BenchmarkDotNet
/// suite and the xUnit correctness tests so every measurement runs against an
/// identical server (clickhouse/clickhouse-server:26.2 — the image the investigation used).
/// </summary>
public static class ClickHouseFixture
{
    // Match ingestion-comparisson.md exactly: same server build, same workload table.
    public const string Image = "clickhouse/clickhouse-server:26.2";
    public const string Password = "test";

    /// <summary>The server user both clients authenticate as (Testcontainers' ClickHouse default).</summary>
    public static string Username => ClickHouseBuilder.DefaultUsername;

    private static ClickHouseContainer? _container;
    private static string? _connectionString;
    private static string? _nativeConnectionString;
    private static readonly SemaphoreSlim Gate = new(1, 1);

    /// <summary>HTTP connection string for the clickhouse-cs client (ClickHouse.Driver, port 8123).</summary>
    public static string ConnectionString =>
        _connectionString ?? throw new InvalidOperationException(
            "ClickHouseFixture.StartAsync() must be awaited before the connection string is read.");

    /// <summary>Native-protocol (TCP, port 9000) connection string for the CH.Native client.</summary>
    public static string NativeConnectionString =>
        _nativeConnectionString ?? throw new InvalidOperationException(
            "ClickHouseFixture.StartAsync() must be awaited before the connection string is read.");

    public static async Task StartAsync(CancellationToken ct = default)
    {
        if (_connectionString is not null) return;
        await Gate.WaitAsync(ct);
        try
        {
            if (_connectionString is not null) return;

            var container = new ClickHouseBuilder()
                .WithImage(Image)
                .WithPassword(Password)
                .Build();

            await container.StartAsync(ct);

            _connectionString = BuildHttpConnectionString(container);
            _nativeConnectionString = BuildNativeConnectionString(container);
            _container = container;
        }
        finally
        {
            Gate.Release();
        }
    }

    /// <summary>HTTP (port 8123) connection string for ClickHouse.Driver — built from the mapped
    /// host port rather than the Testcontainers ADO string (different dialect). Shared with any
    /// other container instance the suite spins up (e.g. the timeout-constrained resilience server).</summary>
    internal static string BuildHttpConnectionString(ClickHouseContainer container) =>
        new ClickHouseConnectionStringBuilder
        {
            Host = container.Hostname,
            Port = container.GetMappedPublicPort(ClickHouseBuilder.HttpPort),
            Username = ClickHouseBuilder.DefaultUsername,
            Password = Password,
            Database = ClickHouseBuilder.DefaultDatabase,
            Protocol = "http",
            Compression = true,
        }.ToString();

    /// <summary>Native binary protocol (port 9000) connection string for CH.Native, with LZ4 wire
    /// compression (its real-world behaviour, per the investigation).</summary>
    internal static string BuildNativeConnectionString(ClickHouseContainer container) =>
        $"Host={container.Hostname};" +
        $"Port={container.GetMappedPublicPort(ClickHouseBuilder.NativePort)};" +
        $"Username={ClickHouseBuilder.DefaultUsername};" +
        $"Password={Password};" +
        $"Database={ClickHouseBuilder.DefaultDatabase};" +
        "Compress=true";

    public static async Task DisposeAsync()
    {
        if (_container is not null)
        {
            await _container.DisposeAsync();
            _container = null;
            _connectionString = null;
            _nativeConnectionString = null;
        }
    }
}
