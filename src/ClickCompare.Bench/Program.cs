using BenchmarkDotNet.Running;
using ClickCompare.Bench;
using ClickCompare.Core;

// Start the shared ClickHouse container once, up front. With the in-process toolchain the
// benchmark code runs in this same process, so the static fixture is visible to it.
Console.WriteLine($"Starting ClickHouse container ({ClickHouseFixture.Image})...");
await ClickHouseFixture.StartAsync();
Console.WriteLine("ClickHouse ready.");

try
{
    // `compare [N]` runs a focused, higher-rep Driver-vs-Native head-to-head at parallelism N
    // (default 4); anything else runs the BenchmarkDotNet suites.
    if (args.Length > 0 && args[0] == "compare")
    {
        var parallelism = args.Length > 1 && int.TryParse(args[1], out var p) ? p : 4;
        var rows = long.TryParse(Environment.GetEnvironmentVariable("WIDE_ROWS"), out var n) && n > 0
            ? n
            : 2_000_000;
        await FocusedComparison.RunAsync(parallelism, rows, warmup: 1, reps: 5);
    }
    // `server-load` reads system.query_log to compare server-side cost (CPU, duration, net-wait) of a
    // single insert across native streaming vs single-file Parquet/RowBinary. Rows via SERVER_ROWS (10M).
    else if (args.Length > 0 && args[0] == "server-load")
    {
        var rows = int.TryParse(Environment.GetEnvironmentVariable("SERVER_ROWS"), out var n) && n > 0
            ? n
            : 10_000_000;
        await ServerLoadComparison.RunAsync(rows, warmup: 1, reps: 3);
    }
    else
    {
        BenchmarkSwitcher.FromAssembly(typeof(MultiStreamBenchmarks).Assembly).Run(args);
    }
}
finally
{
    await ClickHouseFixture.DisposeAsync();
}
