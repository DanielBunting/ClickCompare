using ClickCompare.Core;
using Xunit;

namespace ClickCompare.Tests;

/// <summary>Starts the shared ClickHouse container once for the whole test assembly.</summary>
public sealed class ContainerFixture : IAsyncLifetime
{
    public async Task InitializeAsync() => await ClickHouseFixture.StartAsync();

    public async Task DisposeAsync() => await ClickHouseFixture.DisposeAsync();
}

[CollectionDefinition(Name)]
public sealed class ClickHouseCollection : ICollectionFixture<ContainerFixture>
{
    public const string Name = "clickhouse";
}
