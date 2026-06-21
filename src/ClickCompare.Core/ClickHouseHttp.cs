using System.Text;

namespace ClickCompare.Core;

/// <summary>
/// A thin raw-HTTP client for ClickHouse's port-8123 interface — the honest port of the
/// investigation's <c>curl --data-binary</c> legs. ClickHouse.Driver doesn't expose a raw
/// <c>FORMAT Parquet</c> body insert, so the Parquet routes talk to the server directly: a
/// <c>SELECT … FORMAT Parquet</c> to mint fixtures, and an <c>INSERT … FORMAT Parquet</c> to ship them.
/// One <see cref="HttpClient"/> is reused so connection-open cost stays out of the measured path.
/// </summary>
public sealed class ClickHouseHttp : IDisposable
{
    private readonly HttpClient _http;
    private readonly Uri _baseUri;

    public ClickHouseHttp(Uri baseUri, string user, string password)
    {
        _baseUri = baseUri;
        _http = new HttpClient { Timeout = TimeSpan.FromMinutes(5) };
        // Header auth keeps credentials out of the URL/query string.
        _http.DefaultRequestHeaders.Add("X-ClickHouse-User", user);
        _http.DefaultRequestHeaders.Add("X-ClickHouse-Key", password);
    }

    public static ClickHouseHttp ForFixture() =>
        new(ClickHouseFixture.HttpUri, ClickHouseFixture.Username, ClickHouseFixture.Password);

    /// <summary>Run a query whose response body IS the result (e.g. <c>SELECT … FORMAT Parquet</c>) and
    /// return the raw bytes.</summary>
    public async Task<byte[]> QueryBytesAsync(string sql, CancellationToken ct = default)
    {
        using var resp = await _http.PostAsync(BuildUri(sql, null), content: null, ct);
        await EnsureOkAsync(resp, ct);
        return await resp.Content.ReadAsByteArrayAsync(ct);
    }

    /// <summary>POST a pre-serialized payload as the body of an INSERT (e.g.
    /// <c>INSERT INTO t FORMAT Parquet</c>). <paramref name="settings"/> are appended as URL query
    /// settings — the Parquet routes pass case-insensitive column matching here.</summary>
    public async Task PostBodyAsync(
        string sql, byte[] body, IReadOnlyDictionary<string, string>? settings = null, CancellationToken ct = default)
    {
        using var content = new ByteArrayContent(body);
        using var resp = await _http.PostAsync(BuildUri(sql, settings), content, ct);
        await EnsureOkAsync(resp, ct);
    }

    private Uri BuildUri(string sql, IReadOnlyDictionary<string, string>? settings)
    {
        var sb = new StringBuilder("?query=").Append(Uri.EscapeDataString(sql));
        if (settings is not null)
            foreach (var (k, v) in settings)
                sb.Append('&').Append(Uri.EscapeDataString(k)).Append('=').Append(Uri.EscapeDataString(v));
        return new Uri(_baseUri, sb.ToString());
    }

    // Surface ClickHouse's error text (it returns the diagnostic in the body) rather than a bare 500.
    private static async Task EnsureOkAsync(HttpResponseMessage resp, CancellationToken ct)
    {
        if (resp.IsSuccessStatusCode) return;
        var body = await resp.Content.ReadAsStringAsync(ct);
        throw new InvalidOperationException($"ClickHouse HTTP {(int)resp.StatusCode}: {body}");
    }

    public void Dispose() => _http.Dispose();
}
