using System.Diagnostics;
using ClickCompare.Core;
using DriverConnection = ClickHouse.Driver.ADO.ClickHouseConnection;

namespace ClickCompare.Bench;

/// <summary>
/// A focused, higher-rep head-to-head at a single fixed parallelism — to cut through the noise of the
/// 3-iteration BenchmarkDotNet sweep when the question is narrow ("Driver vs CH.Native at parallelism
/// N"). Runs only the genuinely N-way configs (no concurrency-stacking) so the comparison is fair:
/// <list type="bullet">
/// <item>Native <c>streams=N</c> — N connections, each one streamed INSERT (Native's only dial).</item>
/// <item>Driver <c>mdop=N</c> — one connection, N concurrent HTTP INSERTs of 100k batches.</item>
/// <item>Driver <c>streams=N, batch=1M, mdop=1</c> — N connections × exactly one INSERT each, the
///   structural mirror of Native's streams=N.</item>
/// </list>
/// Invoke with <c>dotnet run -c Release --project src/ClickCompare.Bench -- compare [N]</c>
/// (row count from <c>WIDE_ROWS</c>, default 2M).
/// </summary>
public static class FocusedComparison
{
    public static async Task RunAsync(int parallelism, long rows, int warmup, int reps)
    {
        await ClickHouseFixture.StartAsync();
        await using var admin = new DriverConnection(ClickHouseFixture.ConnectionString);
        await admin.OpenAsync();
        await WideWorkload.ResetTableAsync(admin);

        var driverConn = ClickHouseFixture.ConnectionString;
        var nativeConn = ClickHouseFixture.NativeConnectionString;

        var cases = new (string Label, Func<Task<long>> Run)[]
        {
            ($"Native streams={parallelism} ({parallelism} conns x 1 INSERT)",
                () => NativeWideInsertRunner.RunAsync(
                    nativeConn, NativeInsertConfigs.Base with { Streams = parallelism }, rows)),

            ($"Driver mdop={parallelism} (1 conn, {parallelism} concurrent, 100k batch)",
                () => DriverWideInsertRunner.RunAsync(
                    driverConn, DriverInsertConfigs.Base with { Mdop = parallelism }, rows)),

            ($"Driver streams={parallelism}, batch=1M ({parallelism} conns x 1 INSERT)",
                () => DriverWideInsertRunner.RunAsync(
                    driverConn,
                    DriverInsertConfigs.Base with { Streams = parallelism, BatchSize = 1_000_000, Mdop = 1 },
                    rows)),
        };

        Console.WriteLine($"\n=== Parallelism {parallelism} | {rows:N0} rows | {warmup} warmup + {reps} reps ===");
        foreach (var (label, run) in cases)
        {
            var times = new List<double>(reps);
            for (var i = 0; i < warmup + reps; i++)
            {
                await WideWorkload.TruncateAsync(admin);
                var sw = Stopwatch.StartNew();
                var written = await run();
                sw.Stop();
                if (written != rows)
                    throw new InvalidOperationException($"{label}: wrote {written}, expected {rows}");
                if (i >= warmup) times.Add(sw.Elapsed.TotalMilliseconds);
            }

            times.Sort();
            var median = times[times.Count / 2];
            var mrowsPerSec = rows / (median / 1000.0) / 1e6;
            Console.WriteLine(
                $"{label,-52} median {median,8:N1} ms   min {times[0],8:N1}   max {times[^1],8:N1}   ({mrowsPerSec,4:N1} M rows/s)");
        }
    }
}
