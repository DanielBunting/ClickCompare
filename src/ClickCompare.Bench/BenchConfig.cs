using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Order;
using BenchmarkDotNet.Toolchains.InProcess.Emit;

namespace ClickCompare.Bench;

/// <summary>
/// Each insert is a slow, I/O-bound operation (~0.1–1 s), so we run one invocation per iteration
/// and use the in-process toolchain — that keeps the shared <c>ClickHouseFixture</c> container
/// (started once in Main) reachable from the benchmark code without cross-process handoff.
/// </summary>
public sealed class BenchConfig : ManualConfig
{
    public BenchConfig()
    {
        AddJob(Job.Default
            .WithToolchain(InProcessEmitToolchain.Instance)
            .WithStrategy(RunStrategy.Monitoring) // one real op per iteration; no pilot/overhead probing
            .WithInvocationCount(1)
            .WithUnrollFactor(1)
            .WithWarmupCount(2)
            .WithIterationCount(10));

        AddDiagnoser(MemoryDiagnoser.Default); // surfaces the object[]/boxing allocation story
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
