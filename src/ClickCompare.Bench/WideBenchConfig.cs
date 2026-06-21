using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Order;
using BenchmarkDotNet.Toolchains.InProcess.Emit;

namespace ClickCompare.Bench;

/// <summary>
/// Config for the wide config-variation suites. Each invocation inserts the full row set, so iterations
/// are kept low (1 warmup + 3 measured). Default row count is 10M — large enough that the batching
/// staircase, parallelism scaling and serialization costs all saturate, but ~5× faster to run than 50M
/// over ~28 configs. Set <c>WIDE_ROWS</c> to override (e.g. <c>WIDE_ROWS=50000000</c> for a headline
/// pass, <c>WIDE_ROWS=1000000</c> for a quick smoke). Note: peak memory is bounded by
/// BatchSize × parallelism (the rows stream), so row count affects wall-clock, not RAM. MemoryDiagnoser
/// is deliberately omitted: the streaming generator's allocations dwarf the client's and would only
/// obscure the wall-clock story.
/// </summary>
public sealed class WideBenchConfig : ManualConfig
{
    /// <summary>Rows per insert. Defaults to 10M; override with <c>WIDE_ROWS</c> (e.g. 50000000).</summary>
    public static readonly long RowCount =
        long.TryParse(Environment.GetEnvironmentVariable("WIDE_ROWS"), out var n) && n > 0 ? n : 10_000_000;

    /// <summary>Measured iterations. Defaults to 3 (fast); raise with <c>BENCH_ITERS</c> (e.g. 8) to
    /// tighten the confidence interval on a close comparison.</summary>
    public static readonly int Iterations =
        int.TryParse(Environment.GetEnvironmentVariable("BENCH_ITERS"), out var i) && i > 0 ? i : 3;

    public WideBenchConfig()
    {
        AddJob(Job.Default
            .WithToolchain(InProcessEmitToolchain.Instance)
            .WithStrategy(RunStrategy.Monitoring) // one real op per iteration; no pilot/overhead probing
            .WithInvocationCount(1)
            .WithUnrollFactor(1)
            .WithWarmupCount(1)
            .WithIterationCount(Iterations));

        AddColumn(StatisticColumn.Median, StatisticColumn.Min, StatisticColumn.Max);
        AddExporter(MarkdownExporter.GitHub);
        WithOrderer(new DefaultOrderer(SummaryOrderPolicy.Declared));
        WithOptions(ConfigOptions.JoinSummary);

        var root = FindSolutionRoot();
        if (root is not null)
            WithArtifactsPath(Path.Combine(root, "results"));
    }

    private static string? FindSolutionRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            if (dir.GetFiles("*.sln").Length > 0) return dir.FullName;
            dir = dir.Parent;
        }
        return null;
    }
}
