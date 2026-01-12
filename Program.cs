using System.Data;
using System.Text.Json;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Net.Http;
using System.Net.Http.Headers;
using System.IO;
using System.Buffers;
using System.Threading.Channels;
using System.Linq;
using System.Net;
using System.Text;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Grpc.Core;
using Stress;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using OpenTelemetry.Instrumentation.Runtime;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

var cfg = AppConfig.Load();
var logLevel = Util.GetLogLevel("LOG_LEVEL", LogLevel.Information);

var builder = WebApplication.CreateBuilder(args);
builder.Logging.SetMinimumLevel(logLevel);
builder.WebHost.ConfigureKestrel(options =>
{
    // Allow large payloads up to 60 MB (covers 50 MB payload + overhead)
    options.Limits.MaxRequestBodySize = 60 * 1024 * 1024;
    // HTTP/UI/probes on ListenPort (HTTP/1.1)
    options.Listen(IPAddress.Any, cfg.ListenPort, listenOptions =>
    {
        listenOptions.Protocols = HttpProtocols.Http1;
    });
    // gRPC on GrpcPort (HTTP/2)
    options.Listen(IPAddress.Any, cfg.GrpcPort, listenOptions =>
    {
        listenOptions.Protocols = HttpProtocols.Http2;
    });
});

// Custom metrics / throttling
builder.Services.AddSingleton<MetricsRecorder>();
builder.Services.AddSingleton<SqlThrottle>();

builder.Services.AddCors(options =>
{
    options.AddPolicy("any", policy =>
    {
        policy.AllowAnyOrigin().AllowAnyHeader().AllowAnyMethod();
    });
});

builder.Services.AddGrpc(o =>
{
    // Lift gRPC message limits for large payloads
    o.MaxReceiveMessageSize = 60 * 1024 * 1024;
    o.MaxSendMessageSize = 60 * 1024 * 1024;
});
builder.Services.AddSingleton(cfg);
builder.Services.AddSingleton<TrafficTracker>();
builder.Services.AddSingleton<RingRegistry>();
builder.Services.AddHttpClient();
builder.Services.AddHostedService<HeartbeatService>();
builder.Services.AddSingleton<WriteQueue>();
builder.Services.AddHostedService<SqlWriterService>();

// Configure OpenTelemetry if an OTLP endpoint is provided
var telemetry = builder.Services.AddOpenTelemetry();
telemetry.ConfigureResource(resource => resource.AddService(serviceName: "sql-stress", serviceVersion: "1.0.0"));
if (!string.IsNullOrWhiteSpace(cfg.OtlpEndpoint))
{
    telemetry
        .WithTracing(tracer =>
        {
            tracer.AddAspNetCoreInstrumentation();
            tracer.AddHttpClientInstrumentation();
            tracer.AddSqlClientInstrumentation(options =>
            {
                options.SetDbStatementForText = true;
                options.RecordException = true;
            });
            tracer.AddOtlpExporter(options =>
            {
                options.Endpoint = new Uri(cfg.OtlpEndpoint);
                if (!string.IsNullOrWhiteSpace(cfg.OtlpHeaders))
                {
                    options.Headers = cfg.OtlpHeaders;
                }
                if (cfg.OtlpInsecure)
                {
                    options.HttpClientFactory = () => new HttpClient(new HttpClientHandler
                    {
                        ServerCertificateCustomValidationCallback = HttpClientHandler.DangerousAcceptAnyServerCertificateValidator
                    });
                }
            });
        })
        .WithMetrics(metrics =>
        {
            metrics.AddRuntimeInstrumentation();
            metrics.AddAspNetCoreInstrumentation();
            metrics.AddMeter("sql-stress.custom");
            metrics.AddOtlpExporter(options =>
            {
                options.Endpoint = new Uri(cfg.OtlpEndpoint);
                if (!string.IsNullOrWhiteSpace(cfg.OtlpHeaders))
                {
                    options.Headers = cfg.OtlpHeaders;
                }
                if (cfg.OtlpInsecure)
                {
                    options.HttpClientFactory = () => new HttpClient(new HttpClientHandler
                    {
                        ServerCertificateCustomValidationCallback = HttpClientHandler.DangerousAcceptAnyServerCertificateValidator
                    });
                }
            });
        });
}

var app = builder.Build();
app.UseCors("any");

await EnsureTableAsync(cfg, app.Logger);

var traffic = app.Services.GetRequiredService<TrafficTracker>();

app.MapGet("/healthz", async (CancellationToken ct) =>
{
    await using var conn = new SqlConnection(cfg.ConnectionString);
    await conn.OpenAsync(ct);
    await using var cmd = conn.CreateCommand();
    cmd.CommandText = "SELECT 1";
    cmd.CommandTimeout = cfg.SqlTimeoutSeconds;
    await cmd.ExecuteScalarAsync(ct);
    return Results.Ok("ok");
});

app.MapGet("/status", () =>
{
    var snap = traffic.LocalSnapshot(cfg);
    return Results.Json(new
    {
        status = "ok",
        pod = snap.PodName,
        started = snap.Started,
        totalRequests = snap.TotalRequests,
        inFlight = snap.InFlight,
        utc = DateTime.UtcNow
    });
});

app.MapMethods("/write", new[] { "GET", "POST" }, async (HttpContext context) =>
{
    var metrics = context.RequestServices.GetRequiredService<MetricsRecorder>();
    var ip = context.Connection.RemoteIpAddress?.ToString() ?? "unknown";
    var requestStart = DateTime.UtcNow;

    // Decide payload mode: querystring "mode" overrides env SQL_INSERT_MODE. Supported: "random" (default), "body" (use client payload for POST), "hl7" (synthetic HL7 text).
    var mode = context.Request.Query["mode"].ToString();
    if (string.IsNullOrWhiteSpace(mode)) mode = cfg.SqlInsertMode;
    var useBody = string.Equals(mode, "body", StringComparison.OrdinalIgnoreCase);

    // Fast path when SQL is disabled
    if (cfg.DisableSql)
    {
        traffic.IncrementInFlight();
        try
        {
            await Task.Delay(1, context.RequestAborted);
            var respBody = JsonSerializer.Serialize(new
            {
                bytes_written = 0,
                duration_ms = 1,
                table = cfg.TableName,
                timestamp = DateTime.UtcNow,
                sql_disabled = true
            });
            context.Response.ContentType = "application/json";
            await context.Response.WriteAsync(respBody);
            metrics.RecordHttp("/write", 200, 0, respBody.Length, 1);
            metrics.RecordSql("http", mode, true, 0, 1);
            traffic.Record(ip, 0, 1, true, protocol: "http");
        }
        finally
        {
            traffic.DecrementInFlight();
        }
        return;
    }

    traffic.IncrementInFlight();
    byte[] payload;
    int size;
    if (useBody && string.Equals(context.Request.Method, "POST", StringComparison.OrdinalIgnoreCase))
    {
        var max = cfg.MaxBytes;
        byte[] buffer;
        int length;
        try
        {
            (buffer, length) = await Util.ReadBodyPooledAsync(context.Request, max, context.RequestAborted);
        }
        catch (PayloadTooLargeException)
        {
            context.Response.StatusCode = StatusCodes.Status413PayloadTooLarge;
            await context.Response.WriteAsJsonAsync(new { error = "payload too large", max_bytes = max });
            traffic.DecrementInFlight();
            return;
        }
        payload = buffer;
        size = length;
    }
    else
    {
        var desired = cfg.RandomSize();
        (payload, size, mode) = PayloadFactory.CreatePayload(mode, desired);
    }

    var queue = context.RequestServices.GetRequiredService<WriteQueue>();
    var accepted = queue.TryEnqueue(new WriteJob
    {
        Payload = payload,
        Length = size,
        Mode = mode,
        Protocol = "http",
        Ip = ip,
        RequestStartUtc = requestStart
    });

    if (!accepted)
    {
        ArrayPool<byte>.Shared.Return(payload);
        traffic.DecrementInFlight();
        context.Response.StatusCode = StatusCodes.Status429TooManyRequests;
        await context.Response.WriteAsJsonAsync(new { error = "queue full, retry later" });
        metrics.RecordHttp("/write", 429, size, 0, (DateTime.UtcNow - requestStart).TotalMilliseconds);
        return;
    }

    context.Response.StatusCode = StatusCodes.Status202Accepted;
    await context.Response.WriteAsJsonAsync(new
    {
        status = "queued",
        bytes = size,
        table = cfg.TableName,
        mode,
        sql_disabled = false
    });
    metrics.RecordHttp("/write", 202, size, 0, (DateTime.UtcNow - requestStart).TotalMilliseconds);
});

app.MapGet("/dashboard", (HttpContext context) =>
{
    context.Response.Headers.CacheControl = "no-store, max-age=0";
    context.Response.ContentType = "text/html";
    return context.Response.WriteAsync(DashboardRenderer.Render(cfg.Role));
});

// SQL read dashboard (manual load buttons)
app.MapGet("/sqlread", (HttpContext context) =>
{
    context.Response.Headers.CacheControl = "no-store, max-age=0";
    context.Response.ContentType = "text/html";
    return context.Response.WriteAsync(SqlReadRenderer.Render());
});

app.MapGet("/stats", async (HttpContext context) =>
{
    var scope = context.Request.Query["scope"].ToString();
    var local = traffic.LocalSnapshot(cfg);
    var registry = app.Services.GetRequiredService<RingRegistry>();

    if (string.Equals(scope, "local", StringComparison.OrdinalIgnoreCase))
    {
        return Results.Json(local);
    }

    // Prefer registry snapshots (aggregator) if available
    registry.Upsert(local);
    var registryPods = registry.GetActiveSnapshots();
    List<PodSnapshot> pods;
    if (registryPods.Any())
    {
        pods = registryPods;
    }
    else
    {
        var peers = await traffic.FetchPeerSnapshotsAsync(cfg, context.RequestAborted);
        pods = new List<PodSnapshot> { local };
        pods.AddRange(peers);
    }
    pods = pods.Where(p => p != null && !string.IsNullOrWhiteSpace(p.PodName)).ToList();
    var aggregated = traffic.AggregateSeries(pods);
    var response = new StatsResponse(
        Pods: pods,
        AggregatedSeries: aggregated,
        TotalRequests: pods.Sum(p => p.TotalRequests),
        InFlight: pods.Sum(p => p.InFlight),
        // Only surface "SQL disabled" when every active pod has it disabled.
        SqlDisabled: pods.Any() && pods.All(p => p.SqlDisabled)
    );
    return Results.Json(response);
});

// Latest messages (SQL read)
app.MapGet("/messages", async (HttpContext context) =>
{
    int limit = 50;
    if (int.TryParse(context.Request.Query["limit"], out var parsed))
    {
        limit = Math.Clamp(parsed, 1, 200);
    }
    var mode = context.Request.Query["mode"].ToString().ToLowerInvariant();
    var full = string.Equals(mode, "full", StringComparison.OrdinalIgnoreCase);
    const int maxFullBytes = 5 * 1024 * 1024; // cap full fetch to 5 MB per message to avoid OOM

    var sw = Stopwatch.StartNew();
    var items = new List<object>();
    await using var conn = new SqlConnection(cfg.ConnectionString);
    await conn.OpenAsync(context.RequestAborted);
    await using var cmd = conn.CreateCommand();
    cmd.CommandTimeout = cfg.SqlTimeoutSeconds;
    cmd.CommandText = $"""
SELECT TOP (@lim) id, created_at, payload_size, payload
FROM [dbo].[{cfg.TableName}]
ORDER BY created_at DESC
""";
    cmd.Parameters.Add(new SqlParameter("@lim", SqlDbType.Int) { Value = limit });
    await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess, context.RequestAborted);
    while (await reader.ReadAsync(context.RequestAborted))
    {
        var id = reader.GetGuid(0);
        var created = reader.GetDateTime(1);
        var size = reader.GetInt32(2);
        var msgSw = Stopwatch.StartNew();
        string? payloadUtf8 = null;
        bool truncated = false;
        long readBytes = 0;

        if (full)
        {
            const int chunk = 8192;
            using var ms = new MemoryStream();
            long offset = 0;
            var buffer = new byte[chunk];
            while (true)
            {
                var toRead = (int)Math.Min(chunk, maxFullBytes - ms.Length);
                if (toRead <= 0) break;
                var read = reader.GetBytes(3, offset, buffer, 0, toRead);
                if (read == 0) break;
                ms.Write(buffer, 0, (int)read);
                readBytes += read;
                offset += read;
                if (ms.Length >= maxFullBytes) { truncated = true; break; }
            }
            var bytes = ms.ToArray();
            payloadUtf8 = Util.SanitizeUtf8(bytes);
            if (size > maxFullBytes) truncated = true;
        }
        msgSw.Stop();
        var msgDurationMs = (long)Math.Max(1, Math.Ceiling(msgSw.Elapsed.TotalMilliseconds));

        items.Add(new
        {
            id,
            created_at = created,
            payload_size = size,
            duration_ms = msgDurationMs,
            payload_utf8 = payloadUtf8,
            payload_base64 = (string?)null, // omit base64 to keep response light
            read_bytes = readBytes,
            truncated
        });
    }

    sw.Stop();
    return Results.Json(new { items, note = $"Showing {items.Count} of latest messages", duration_ms = sw.ElapsedMilliseconds });
});

app.MapGrpcService<StressGrpcService>();

// Registry endpoints (aggregator consumption)
app.MapPost("/register", async (RingRegistry registry, HttpContext ctx) =>
{
    try
    {
        var snapshot = await JsonSerializer.DeserializeAsync<PodSnapshot>(ctx.Request.Body, cancellationToken: ctx.RequestAborted);
        if (snapshot != null && !string.IsNullOrWhiteSpace(snapshot.PodName))
        {
            registry.Upsert(snapshot);
        }
        return Results.Ok();
    }
    catch
    {
        return Results.BadRequest();
    }
});

app.Run();

static async Task EnsureTableAsync(AppConfig cfg, ILogger logger)
{
    await using var conn = new SqlConnection(cfg.ConnectionString);
    await conn.OpenAsync();
    await using var cmd = conn.CreateCommand();
    cmd.CommandTimeout = cfg.SqlTimeoutSeconds;
    cmd.CommandText = $"""
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[{cfg.TableName}]') AND type = N'U')
BEGIN
    CREATE TABLE [dbo].[{cfg.TableName}] (
        id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        payload_size INT NOT NULL,
        payload VARBINARY(MAX) NULL
    );
END;
""";
    await cmd.ExecuteNonQueryAsync();
    logger.LogInformation("Ensured table exists: {Table}", cfg.TableName);
}

public sealed class AppConfig
{
    public required string ConnectionString { get; init; }
    public required string TableName { get; init; }
    public required int MinBytes { get; init; }
    public required int MaxBytes { get; init; }
    public required int SqlTimeoutSeconds { get; init; }
    public required int ListenPort { get; init; }
    public required int GrpcPort { get; init; }
    public string Role { get; init; } = "ingester"; // ingester | aggregator | messages
    public int MaxInflightSql { get; init; }
    public int MaxQueueLength { get; init; }
    public string SqlInsertMode { get; init; } = "random";
    public string? RingEndpoint { get; init; }
    public string? OtlpEndpoint { get; init; }
    public string? OtlpHeaders { get; init; }
    public bool OtlpInsecure { get; init; }
    public bool DisableSql { get; init; }
    public required string PodName { get; init; }
    public List<string> PeerDashboardUrls { get; init; } = new();
    public string? PeerServiceName { get; init; }
    public string? PodNamespace { get; init; }
    public int PeerServicePort { get; init; }

    public static AppConfig Load()
    {
        var server = Require("SQL_SERVER");
        var port = GetInt("SQL_PORT", 1433);
        var database = Require("SQL_DATABASE");
        var user = Require("SQL_USER");
        var password = Require("SQL_PASSWORD");
        var encrypt = GetBool("SQL_ENCRYPT", true);
        var trustServerCert = GetBool("SQL_TRUST_SERVER_CERT", false);

        var builder = new SqlConnectionStringBuilder
        {
            DataSource = $"{server},{port}",
            InitialCatalog = database,
            UserID = user,
            Password = password,
            Encrypt = encrypt,
            TrustServerCertificate = trustServerCert,
            ConnectTimeout = GetInt("SQL_CONNECT_TIMEOUT_SECONDS", 30),
            MaxPoolSize = GetInt("SQL_MAX_POOL_SIZE", 200),
            MinPoolSize = GetInt("SQL_MIN_POOL_SIZE", 10),
        };

        var minBytes = GetInt("PAYLOAD_MIN_BYTES", 1024);
        var maxBytes = GetInt("PAYLOAD_MAX_BYTES", 50 * 1024 * 1024);
        if (minBytes <= 0) throw new InvalidOperationException("PAYLOAD_MIN_BYTES must be > 0");
        if (maxBytes < minBytes) throw new InvalidOperationException("PAYLOAD_MAX_BYTES must be >= PAYLOAD_MIN_BYTES");

        return new AppConfig
        {
            ConnectionString = builder.ConnectionString,
            TableName = Get("SQL_TABLE", "stress_writes")!,
            MinBytes = minBytes,
            MaxBytes = maxBytes,
            SqlTimeoutSeconds = GetInt("SQL_TIMEOUT_SECONDS", 60),
            ListenPort = GetInt("APP_PORT", 8080),
            GrpcPort = GetInt("APP_GRPC_PORT", 8081),
            MaxInflightSql = GetInt("MAX_INFLIGHT_SQL", 0),
            MaxQueueLength = GetInt("MAX_QUEUE_LENGTH", 128),
            SqlInsertMode = Get("SQL_INSERT_MODE", "random")!.ToLowerInvariant(),
            Role = Get("APP_ROLE", "ingester")!.ToLowerInvariant(),
            RingEndpoint = Get("RING_ENDPOINT", null),
            OtlpEndpoint = Get("OTLP_ENDPOINT", null),
            OtlpHeaders = Get("OTLP_HEADERS", null),
            OtlpInsecure = GetBool("OTLP_INSECURE", false),
            DisableSql = GetBool("DISABLE_SQL", false),
            PodName = Get("POD_NAME", Environment.GetEnvironmentVariable("HOSTNAME") ?? "unknown")!,
            PeerDashboardUrls = ParseList("PEER_DASHBOARD_URLS"),
            PeerServiceName = Get("PEER_SERVICE_NAME", null),
            PodNamespace = Get("POD_NAMESPACE", Environment.GetEnvironmentVariable("POD_NAMESPACE")),
            PeerServicePort = GetInt("PEER_SERVICE_PORT", 8080)
        };
    }

    public int RandomSize()
    {
        return MinBytes == MaxBytes ? MinBytes : Random.Shared.Next(MinBytes, MaxBytes + 1);
    }

    private static string Require(string key)
    {
        var value = Environment.GetEnvironmentVariable(key);
        if (string.IsNullOrWhiteSpace(value))
            throw new InvalidOperationException($"Missing required env var {key}");
        return value;
    }

    private static string? Get(string key, string? defaultValue)
    {
        var env = Environment.GetEnvironmentVariable(key);
        if (string.IsNullOrWhiteSpace(env))
        {
            return defaultValue;
        }
        return env;
    }

    private static int GetInt(string key, int defaultValue)
    {
        var raw = Environment.GetEnvironmentVariable(key);
        if (string.IsNullOrWhiteSpace(raw)) return defaultValue;
        return int.TryParse(raw, out var value) ? value : defaultValue;
    }

    private static bool GetBool(string key, bool defaultValue)
    {
        var raw = Environment.GetEnvironmentVariable(key);
        if (string.IsNullOrWhiteSpace(raw)) return defaultValue;
        return raw.ToLowerInvariant() switch
        {
            "1" or "true" or "yes" or "on" => true,
            "0" or "false" or "no" or "off" => false,
            _ => defaultValue
        };
    }

    private static List<string> ParseList(string key)
    {
        var raw = Environment.GetEnvironmentVariable(key);
        if (string.IsNullOrWhiteSpace(raw)) return new();
        return raw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToList();
    }
}

public sealed class MetricsRecorder
{
    private readonly Counter<long> _httpReqs;
    private readonly Counter<long> _httpReqBytes;
    private readonly Counter<long> _httpRespBytes;
    private readonly Histogram<double> _httpDuration;
    private readonly Counter<long> _grpcReqs;
    private readonly Counter<long> _grpcReqBytes;
    private readonly Counter<long> _grpcRespBytes;
    private readonly Histogram<double> _grpcDuration;
    private readonly Counter<long> _sqlSuccess;
    private readonly Counter<long> _sqlFailure;
    private readonly Counter<long> _sqlBytes;
    private readonly Histogram<double> _sqlDuration;

    public MetricsRecorder()
    {
        var meter = new Meter("sql-stress.custom", "1.0.0");
        _httpReqs = meter.CreateCounter<long>("sqlstress.http.requests");
        _httpReqBytes = meter.CreateCounter<long>("sqlstress.http.request_bytes");
        _httpRespBytes = meter.CreateCounter<long>("sqlstress.http.response_bytes");
        _httpDuration = meter.CreateHistogram<double>("sqlstress.http.duration_ms");
        _grpcReqs = meter.CreateCounter<long>("sqlstress.grpc.requests");
        _grpcReqBytes = meter.CreateCounter<long>("sqlstress.grpc.request_bytes");
        _grpcRespBytes = meter.CreateCounter<long>("sqlstress.grpc.response_bytes");
        _grpcDuration = meter.CreateHistogram<double>("sqlstress.grpc.duration_ms");
        _sqlSuccess = meter.CreateCounter<long>("sqlstress.sql.success");
        _sqlFailure = meter.CreateCounter<long>("sqlstress.sql.failure");
        _sqlBytes = meter.CreateCounter<long>("sqlstress.sql.bytes");
        _sqlDuration = meter.CreateHistogram<double>("sqlstress.sql.duration_ms");
    }

    public void RecordHttp(string route, int status, long reqBytes, long respBytes, double durationMs)
    {
        var tags = new TagList
        {
            { "route", route },
            { "status", status },
            { "protocol", "http" }
        };
        _httpReqs.Add(1, tags);
        _httpReqBytes.Add(reqBytes, tags);
        _httpRespBytes.Add(respBytes, tags);
        _httpDuration.Record(durationMs, tags);
    }

    public void RecordGrpc(string method, string status, long reqBytes, long respBytes, double durationMs)
    {
        var tags = new TagList
        {
            { "method", method },
            { "status", status },
            { "protocol", "grpc" }
        };
        _grpcReqs.Add(1, tags);
        _grpcReqBytes.Add(reqBytes, tags);
        _grpcRespBytes.Add(respBytes, tags);
        _grpcDuration.Record(durationMs, tags);
    }

    public void RecordSql(string protocol, string mode, bool success, long bytes, double durationMs)
    {
        var tags = new TagList
        {
            { "protocol", protocol },
            { "mode", mode }
        };
        if (success)
        {
            _sqlSuccess.Add(1, tags);
        }
        else
        {
            _sqlFailure.Add(1, tags);
        }
        _sqlBytes.Add(bytes, tags);
        _sqlDuration.Record(durationMs, tags);
    }
}

public sealed class RingRegistry
{
    private readonly ConcurrentDictionary<string, (PodSnapshot Snapshot, DateTime LastSeen)> _entries = new();
    private readonly TimeSpan _ttl = TimeSpan.FromSeconds(15);

    public void Upsert(PodSnapshot snapshot)
    {
        _entries[snapshot.PodName] = (snapshot, DateTime.UtcNow);
    }

    public List<PodSnapshot> GetActiveSnapshots()
    {
        var cutoff = DateTime.UtcNow - _ttl;
        var stale = _entries.Where(kv => kv.Value.LastSeen < cutoff).Select(kv => kv.Key).ToList();
        foreach (var key in stale)
        {
            _entries.TryRemove(key, out _);
        }
        return _entries.Values.Select(v => v.Snapshot).ToList();
    }
}

public sealed class HeartbeatService : BackgroundService
{
    private readonly AppConfig _cfg;
    private readonly TrafficTracker _traffic;
    private readonly IHttpClientFactory _httpFactory;
    private readonly ILogger<HeartbeatService> _logger;

    public HeartbeatService(AppConfig cfg, TrafficTracker traffic, IHttpClientFactory httpFactory, ILogger<HeartbeatService> logger)
    {
        _cfg = cfg;
        _traffic = traffic;
        _httpFactory = httpFactory;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (string.IsNullOrWhiteSpace(_cfg.RingEndpoint))
        {
            _logger.LogInformation("RingEndpoint not set; heartbeat disabled");
            return;
        }

        var client = _httpFactory.CreateClient();
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var snap = _traffic.LocalSnapshot(_cfg);
                var json = JsonSerializer.Serialize(snap);
                using var content = new StringContent(json, Encoding.UTF8, "application/json");
                using var resp = await client.PostAsync(_cfg.RingEndpoint, content, stoppingToken);
                if (!resp.IsSuccessStatusCode)
                {
                    _logger.LogWarning("Heartbeat failed to {Endpoint} with status {Status}", _cfg.RingEndpoint, resp.StatusCode);
                }
            }
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogWarning(ex, "Heartbeat error to {Endpoint}", _cfg.RingEndpoint);
            }

            try
            {
                await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
            }
            catch (TaskCanceledException) { }
        }
    }
}

public class StressGrpcService : StressTest.StressTestBase
{
    private readonly AppConfig _cfg;
    private readonly TrafficTracker _traffic;
    private readonly MetricsRecorder _metrics;

    public StressGrpcService(AppConfig cfg, TrafficTracker traffic, MetricsRecorder metrics)
    {
        _cfg = cfg;
        _traffic = traffic;
        _metrics = metrics;
    }

    public override async Task<WriteResponse> Write(WriteRequest request, ServerCallContext context)
    {
        var size = request.SizeBytes > 0 ? request.SizeBytes : _cfg.RandomSize();
        (var payload, size, var mode) = PayloadFactory.CreatePayload(_cfg.SqlInsertMode, size);

        _traffic.IncrementInFlight();
        var requestStart = DateTime.UtcNow;

        var sw = System.Diagnostics.Stopwatch.StartNew();
        if (!_cfg.DisableSql)
        {
            var gate = context.GetHttpContext()?.RequestServices.GetRequiredService<SqlThrottle>()
                       ?? throw new InvalidOperationException("SqlThrottle not available");
            await using var lease = await gate.WaitAsync(context.CancellationToken);

            await using var conn = new SqlConnection(_cfg.ConnectionString);
            await conn.OpenAsync(context.CancellationToken);
            await using var cmd = conn.CreateCommand();
            cmd.CommandTimeout = _cfg.SqlTimeoutSeconds;
            cmd.CommandText = $"INSERT INTO [dbo].[{_cfg.TableName}] (payload_size, payload) VALUES (@p1, @p2)";
            cmd.Parameters.Add(new SqlParameter("@p1", System.Data.SqlDbType.Int) { Value = size });
            cmd.Parameters.Add(new SqlParameter("@p2", System.Data.SqlDbType.VarBinary, -1) { Value = payload });
            await cmd.ExecuteNonQueryAsync(context.CancellationToken);
        }
        else
        {
            await Task.Delay(1, context.CancellationToken);
        }
        sw.Stop();

        var totalDurationMs = (DateTime.UtcNow - requestStart).TotalMilliseconds;
        _traffic.Record(context.Peer, size, totalDurationMs, success: true, protocol: "grpc");
        _metrics.RecordGrpc("Write", "OK", size, sizeof(long) + _cfg.TableName.Length, totalDurationMs);
        _metrics.RecordSql("grpc", mode, true, size, totalDurationMs);
        _traffic.DecrementInFlight();

        return new WriteResponse
        {
            BytesWritten = size,
            DurationMs = (long)sw.ElapsedMilliseconds,
            Table = _cfg.TableName,
            Timestamp = DateTime.UtcNow.ToString("O")
        };
    }

    public override async Task<HealthResponse> Healthz(HealthRequest request, ServerCallContext context)
    {
        await using var conn = new SqlConnection(_cfg.ConnectionString);
        await conn.OpenAsync(context.CancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1";
        cmd.CommandTimeout = _cfg.SqlTimeoutSeconds;
        await cmd.ExecuteScalarAsync(context.CancellationToken);
        return new HealthResponse { Status = "ok" };
    }
}

public sealed class TrafficTracker
{
    private readonly ConcurrentDictionary<string, IpStats> _byIp = new();
    private long _totalRequests;
    private long _inFlight;
    private long _httpRequests;
    private long _grpcRequests;
    private readonly DateTime _started = DateTime.UtcNow;
    private readonly object _seriesLock = new();
    private readonly Dictionary<DateTime, TimeBucket> _series = new();
    private readonly HttpClient _httpClient = new();
    private long _totalBytes;

    public void Record(string ip, int bytes, double durationMs, bool success, string protocol)
    {
        Interlocked.Increment(ref _totalRequests);
        Interlocked.Add(ref _totalBytes, bytes);
        if (string.Equals(protocol, "grpc", StringComparison.OrdinalIgnoreCase))
        {
            Interlocked.Increment(ref _grpcRequests);
        }
        else
        {
            Interlocked.Increment(ref _httpRequests);
        }
        _byIp.AddOrUpdate(ip,
            _ => new IpStats(ip, 1, bytes, durationMs, durationMs, DateTime.UtcNow),
            (_, existing) => existing with
            {
                Requests = existing.Requests + 1,
                TotalBytes = existing.TotalBytes + bytes,
                TotalDurationMs = existing.TotalDurationMs + durationMs,
                LastDurationMs = durationMs,
                LastSeen = DateTime.UtcNow
            });

        var second = DateTime.UtcNow;
        second = new DateTime(second.Year, second.Month, second.Day, second.Hour, second.Minute, second.Second, DateTimeKind.Utc);
        lock (_seriesLock)
    {
        if (!_series.TryGetValue(second, out var bucket))
        {
            bucket = new TimeBucket(second, 0, 0);
            _series[second] = bucket;
        }
        bucket.Requests += 1;
        bucket.Bytes += bytes;
        if (success) bucket.Success += 1; else bucket.Failures += 1;
        // trim to last 30s
        var cutoff = DateTime.UtcNow.AddSeconds(-30);
        foreach (var key in _series.Keys.Where(k => k < cutoff).ToList())
        {
            _series.Remove(key);
            }
        }
    }

    public void IncrementInFlight() => Interlocked.Increment(ref _inFlight);
    public void DecrementInFlight() => Interlocked.Decrement(ref _inFlight);

    public PodSnapshot LocalSnapshot(AppConfig cfg)
    {
        List<TimeBucket> series;
        lock (_seriesLock)
        {
            series = _series.Values.OrderBy(v => v.Timestamp).ToList();
        }
        return new PodSnapshot(
            PodName: cfg.PodName,
            Started: _started,
            TotalRequests: Interlocked.Read(ref _totalRequests),
            InFlight: Interlocked.Read(ref _inFlight),
            TotalBytes: Interlocked.Read(ref _totalBytes),
            PodIp: GetPodIp(),
            CpuPct: GetCpuPercent(),
            MemBytes: GC.GetTotalMemory(false),
            HttpRequests: Interlocked.Read(ref _httpRequests),
            GrpcRequests: Interlocked.Read(ref _grpcRequests),
            SqlDisabled: cfg.DisableSql,
            PerIp: _byIp.Values.OrderByDescending(v => v.Requests).ToList(),
            Series: series
        );
    }

    private IEnumerable<string> DiscoverPeers(AppConfig cfg)
    {
        if (string.IsNullOrWhiteSpace(cfg.PeerServiceName) || string.IsNullOrWhiteSpace(cfg.PodNamespace))
        {
            return Enumerable.Empty<string>();
        }
        try
        {
            var tokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token";
            if (!File.Exists(tokenPath))
            {
                return Enumerable.Empty<string>();
            }
            var token = File.ReadAllText(tokenPath);
            var handler = new HttpClientHandler
            {
                ServerCertificateCustomValidationCallback = HttpClientHandler.DangerousAcceptAnyServerCertificateValidator
            };
            using var client = new HttpClient(handler);
            client.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", token);
            var url = $"https://kubernetes.default.svc/api/v1/namespaces/{cfg.PodNamespace}/endpoints/{cfg.PeerServiceName}";
            var resp = client.GetAsync(url).GetAwaiter().GetResult();
            if (!resp.IsSuccessStatusCode)
            {
                return Enumerable.Empty<string>();
            }
            var json = resp.Content.ReadAsStringAsync().GetAwaiter().GetResult();
            using var doc = JsonDocument.Parse(json);
            if (!doc.RootElement.TryGetProperty("subsets", out var subsets))
            {
                return Enumerable.Empty<string>();
            }
            var addresses = new List<string>();
            foreach (var subset in subsets.EnumerateArray())
            {
                if (!subset.TryGetProperty("addresses", out var addrs)) continue;
                foreach (var addr in addrs.EnumerateArray())
                {
                    var ip = addr.GetProperty("ip").GetString();
                    if (!string.IsNullOrWhiteSpace(ip))
                    {
                        addresses.Add($"http://{ip}:{cfg.PeerServicePort}");
                    }
                }
            }
            return addresses;
        }
        catch
        {
            return Enumerable.Empty<string>();
        }
    }

    public async Task<List<PodSnapshot>> FetchPeerSnapshotsAsync(AppConfig cfg, CancellationToken ct)
    {
        var endpoints = DiscoverPeers(cfg).Concat(cfg.PeerDashboardUrls ?? new List<string>()).Distinct().ToList();
        var results = new List<PodSnapshot>();
        foreach (var ep in endpoints)
        {
            try
            {
                using var req = new HttpRequestMessage(HttpMethod.Get, $"{ep}/stats?scope=local");
                var resp = await _httpClient.SendAsync(req, ct);
                if (!resp.IsSuccessStatusCode) continue;
                var json = await resp.Content.ReadAsStringAsync(ct);
                var pod = JsonSerializer.Deserialize<PodSnapshot>(json);
                if (pod != null)
                {
                    results.Add(pod);
                }
            }
            catch
            {
                // ignore peer errors
            }
        }
        return results;
    }

    public List<TimeBucket> AggregateSeries(IEnumerable<PodSnapshot> pods)
    {
        if (pods == null)
        {
            return new List<TimeBucket>();
        }
        var map = new Dictionary<DateTime, TimeBucket>();
        foreach (var pod in pods)
        {
            if (pod?.Series == null) continue;
            foreach (var bucket in pod.Series)
            {
                if (!map.TryGetValue(bucket.Timestamp, out var agg))
                {
                    agg = new TimeBucket(bucket.Timestamp, 0, 0);
                    map[bucket.Timestamp] = agg;
                }
                agg.Requests += bucket.Requests;
                agg.Bytes += bucket.Bytes;
                agg.Success += bucket.Success;
                agg.Failures += bucket.Failures;
            }
        }
        return map.Values.OrderBy(v => v.Timestamp).ToList();
    }

    private static string? GetPodIp()
    {
        try
        {
            var host = Dns.GetHostName();
            var entry = Dns.GetHostEntry(host);
            var ip = entry.AddressList.FirstOrDefault(a => a.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork && !IPAddress.IsLoopback(a));
            return ip?.ToString();
        }
        catch
        {
            return null;
        }
    }

    private double GetCpuPercent()
    {
        try
        {
            var proc = Process.GetCurrentProcess();
            var uptime = DateTime.UtcNow - proc.StartTime.ToUniversalTime();
            if (uptime.TotalSeconds <= 0) return 0;
            var pct = (proc.TotalProcessorTime.TotalSeconds / uptime.TotalSeconds) / Environment.ProcessorCount * 100;
            return Math.Round(pct, 2);
        }
        catch
        {
            return 0;
        }
    }
}

public record IpStats(string Ip, long Requests, long TotalBytes, double LastDurationMs, double TotalDurationMs, DateTime LastSeen)
{
    public double AvgDurationMs => Requests == 0 ? 0 : TotalDurationMs / Requests;
}

public class TimeBucket
{
    public DateTime Timestamp { get; }
    public long Requests { get; set; }
    public long Bytes { get; set; }
    public long Success { get; set; }
    public long Failures { get; set; }

    public TimeBucket(DateTime timestamp, long requests, long bytes, long success = 0, long failures = 0)
    {
        Timestamp = timestamp;
        Requests = requests;
        Bytes = bytes;
        Success = success;
        Failures = failures;
    }
}

public record PodSnapshot(
    string PodName,
    DateTime Started,
    long TotalRequests,
    long InFlight,
    long TotalBytes,
    string? PodIp,
    double? CpuPct,
    long? MemBytes,
    long HttpRequests,
    long GrpcRequests,
    bool SqlDisabled,
    List<IpStats> PerIp,
    List<TimeBucket> Series
);

public record StatsResponse(List<PodSnapshot> Pods, List<TimeBucket> AggregatedSeries, long TotalRequests, long InFlight, bool SqlDisabled);

public sealed class SqlThrottle
{
    private readonly SemaphoreSlim? _semaphore;

    public SqlThrottle(AppConfig cfg)
    {
        if (cfg.MaxInflightSql > 0)
        {
            _semaphore = new SemaphoreSlim(cfg.MaxInflightSql);
        }
    }

    public async ValueTask<IAsyncDisposable> WaitAsync(CancellationToken ct)
    {
        if (_semaphore == null)
        {
            return new NoopLease();
        }
        await _semaphore.WaitAsync(ct);
        return new Lease(_semaphore);
    }

    private sealed class Lease : IAsyncDisposable
    {
        private SemaphoreSlim? _sem;
        public Lease(SemaphoreSlim sem) => _sem = sem;
        public ValueTask DisposeAsync()
        {
            _sem?.Release();
            _sem = null;
            return ValueTask.CompletedTask;
        }
    }

    private sealed class NoopLease : IAsyncDisposable
    {
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}

public sealed class WriteJob
{
    public required byte[] Payload { get; init; }
    public required int Length { get; init; }
    public required string Mode { get; init; }
    public required string Protocol { get; init; }
    public required string Ip { get; init; }
    public required DateTime RequestStartUtc { get; init; }
}

public sealed class WriteQueue
{
    private readonly Channel<WriteJob> _channel;
    private readonly AppConfig _cfg;
    private long _count;

    public WriteQueue(AppConfig cfg)
    {
        _cfg = cfg;
        _channel = Channel.CreateBounded<WriteJob>(new BoundedChannelOptions(cfg.MaxQueueLength)
        {
            FullMode = BoundedChannelFullMode.DropNewest,
            SingleReader = true,
            SingleWriter = false
        });
    }

    public bool TryEnqueue(WriteJob job)
    {
        var ok = _channel.Writer.TryWrite(job);
        if (ok) Interlocked.Increment(ref _count);
        return ok;
    }

    public void OnDequeue() => Interlocked.Decrement(ref _count);

    public long Count => Interlocked.Read(ref _count);

    public IAsyncEnumerable<WriteJob> ReadAllAsync(CancellationToken ct) => _channel.Reader.ReadAllAsync(ct);
}

public sealed class SqlWriterService : BackgroundService
{
    private readonly AppConfig _cfg;
    private readonly WriteQueue _queue;
    private readonly IServiceProvider _sp;
    private readonly ILogger<SqlWriterService> _logger;

    public SqlWriterService(AppConfig cfg, WriteQueue queue, IServiceProvider sp, ILogger<SqlWriterService> logger)
    {
        _cfg = cfg;
        _queue = queue;
        _sp = sp;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await foreach (var job in _queue.ReadAllAsync(stoppingToken))
        {
            await ProcessJobAsync(job, stoppingToken);
        }
    }

    private async Task ProcessJobAsync(WriteJob job, CancellationToken ct)
    {
        var metrics = _sp.GetRequiredService<MetricsRecorder>();
        var traffic = _sp.GetRequiredService<TrafficTracker>();
        var throttle = _sp.GetRequiredService<SqlThrottle>();
        var queue = _sp.GetRequiredService<WriteQueue>();

        bool success = false;
        var sw = Stopwatch.StartNew();
        double sqlMs = 0;
        try
        {
            await using var lease = await throttle.WaitAsync(ct);
            await using var conn = new SqlConnection(_cfg.ConnectionString);
            await conn.OpenAsync(ct);
            await using var cmd = conn.CreateCommand();
            cmd.CommandTimeout = _cfg.SqlTimeoutSeconds;
            cmd.CommandText = $"INSERT INTO [dbo].[{_cfg.TableName}] (payload_size, payload) VALUES (@p1, @p2)";
            cmd.Parameters.Add(new SqlParameter("@p1", SqlDbType.Int) { Value = job.Length });
            cmd.Parameters.Add(new SqlParameter("@p2", SqlDbType.VarBinary, -1) { Value = job.Payload });
            await cmd.ExecuteNonQueryAsync(ct);
            success = true;
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            _logger.LogWarning(ex, "SQL write failed for {Bytes} bytes", job.Length);
        }
        finally
        {
            sw.Stop();
            sqlMs = sw.Elapsed.TotalMilliseconds;
            metrics.RecordSql(job.Protocol, job.Mode, success, job.Length, sqlMs);
            var totalDurationMs = (DateTime.UtcNow - job.RequestStartUtc).TotalMilliseconds;
            traffic.Record(job.Ip, job.Length, totalDurationMs, success, job.Protocol);
            traffic.DecrementInFlight();
            ArrayPool<byte>.Shared.Return(job.Payload);
            queue.OnDequeue();
            _logger.LogDebug("SQL write {Result} bytes={Bytes} sql_ms={SqlMs:F1} total_ms={TotalMs:F1} mode={Mode} queue={QueueCount}",
                success ? "OK" : "FAIL", job.Length, sqlMs, totalDurationMs, job.Mode, queue.Count);
        }
    }
}

internal sealed class PayloadTooLargeException : Exception
{
    public int Max { get; }
    public PayloadTooLargeException(int max) : base($"Payload exceeds {max} bytes") => Max = max;
}

public static class PayloadFactory
{
    public static (byte[] Payload, int Size, string ModeUsed) CreatePayload(string? requestedMode, int targetBytes)
    {
        var mode = (requestedMode ?? "random").ToLowerInvariant();
        return mode switch
        {
            "hl7" => CreateHl7(targetBytes),
            _ => CreateRandom(targetBytes)
        };
    }

    private static (byte[] Payload, int Size, string ModeUsed) CreateRandom(int targetBytes)
    {
        var size = Math.Max(1, targetBytes);
        var payload = new byte[size];
        Random.Shared.NextBytes(payload);
        return (payload, size, "random");
    }

    private static (byte[] Payload, int Size, string ModeUsed) CreateHl7(int targetBytes)
    {
        // Build a simple HL7 message and pad with OBX segments to reach targetBytes.
        var now = DateTime.UtcNow;
        var header = $"MSH|^~\\&|SQLSTRESS|LAB|DEST|DESTFAC|{now:yyyyMMddHHmmss}||ADT^A01|MSG{now:yyyyMMddHHmmssfff}|P|2.5\r" +
                     $"PID|1||{Guid.NewGuid()}||DOE^JOHN||19700101|M\r" +
                     $"PV1|1|I|WARD^ROOM^BED\r";

        var sb = new StringBuilder(header);
        var obxIndex = 1;
        var target = Math.Max(targetBytes, header.Length + 10);
        while (Encoding.UTF8.GetByteCount(sb.ToString()) < target)
        {
            var fragment = Guid.NewGuid().ToString("N");
            sb.Append($"OBX|{obxIndex++}|TX|STRESS^{fragment}||Sample text {fragment}||||||F\r");
        }

        var bytes = Encoding.UTF8.GetBytes(sb.ToString());
        if (bytes.Length > target)
        {
            // Trim to target
            var trimmed = new byte[target];
            Array.Copy(bytes, trimmed, target);
            bytes = trimmed;
        }
        return (bytes, bytes.Length, "hl7");
    }
}

internal static class Util
{
    public static string SanitizeUtf8(byte[] data)
    {
        if (data == null || data.Length == 0) return string.Empty;
        try
        {
            var s = Encoding.UTF8.GetString(data);
            // Strip control chars except CR/LF/TAB
            return new string(s.Where(c => c == '\r' || c == '\n' || c == '\t' || !char.IsControl(c)).ToArray());
        }
        catch
        {
            return Convert.ToBase64String(data);
        }
    }

    public static async Task<(byte[] Buffer, int Length)> ReadBodyPooledAsync(HttpRequest request, int maxBytes, CancellationToken ct)
    {
        if (request.ContentLength.HasValue && request.ContentLength.Value > maxBytes)
        {
            throw new PayloadTooLargeException(maxBytes);
        }

        var buffer = ArrayPool<byte>.Shared.Rent(maxBytes);
        int total = 0;
        try
        {
            while (true)
            {
                var read = await request.Body.ReadAsync(buffer.AsMemory(total, Math.Min(8192, maxBytes - total)), ct);
                if (read == 0) break;
                total += read;
                if (total > maxBytes)
                {
                    throw new PayloadTooLargeException(maxBytes);
                }
            }
            return (buffer, total);
        }
        catch
        {
            ArrayPool<byte>.Shared.Return(buffer);
            throw;
        }
    }

    public static LogLevel GetLogLevel(string key, LogLevel defaultLevel)
    {
        var raw = Environment.GetEnvironmentVariable(key);
        if (string.IsNullOrWhiteSpace(raw)) return defaultLevel;
        return Enum.TryParse<LogLevel>(raw, true, out var lvl) ? lvl : defaultLevel;
    }
}

internal static class DashboardRenderer
{
    public static string Render(string role)
    {
        var roleSafe = string.IsNullOrWhiteSpace(role) ? "ingester" : role;
        var template = @"<!DOCTYPE html>
<html lang=""en"">
<head>
  <meta charset=""UTF-8"">
  <title>SQL Stress Dashboard</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 20px; background: #0f172a; color: #e2e8f0; }
    h1 { margin-bottom: 0; }
    .stat { display: inline-block; margin-right: 20px; padding: 10px 14px; background: #1e293b; border-radius: 8px; }
    table { width: 100%; border-collapse: collapse; margin-top: 20px; }
    th, td { padding: 8px 10px; border-bottom: 1px solid #334155; }
    th { text-align: left; background: #1e293b; }
    tr:nth-child(even) { background: #111827; }
  </style>
</head>
<body>
  <h1>SQL Stress Dashboard</h1>
  <p>Aggregated view &bull; build: human-dur-v3</p>
  <div id=""summary""></div>
  <div id=""series""></div>

  <div id=""pods""></div>

  <div id=""messages""></div>

  <table>
    <thead>
      <tr><th>IP</th><th>Requests</th><th>Total Bytes</th><th>Last Duration</th><th>Avg Duration</th><th>Last Seen</th></tr>
    </thead>
    <tbody id=""ipBody"">
      <tr><td colspan='6'>No data</td></tr>
    </tbody>
  </table>
  <script>
    let refreshHandle = null;
    const ROLE = '{{ROLE}}';
    const MSG_LIMIT = ROLE === 'messages' ? 10 : 50;

    function setIntervalMs(ms) {
      if (refreshHandle) clearInterval(refreshHandle);
      refreshHandle = setInterval(load, ms);
    }

    async function fetchStats() {
      const res = await fetch('/stats');
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.json();
    }

    function renderSummary(totalRequests, inFlight, podCount, httpTotal, grpcTotal, disabledCount) {
      const div = document.getElementById('summary');
      const sqlColor = disabledCount > 0 ? (disabledCount === podCount ? '#b91c1c' : '#b45309') : '#1e293b';
      div.innerHTML = `<div class='stat'>Total requests: ${totalRequests}</div>
      <div class='stat'>In flight: ${inFlight}</div>
      <div class='stat'>Pods: ${podCount}</div>
      <div class='stat'>HTTP: ${httpTotal}</div>
      <div class='stat'>gRPC: ${grpcTotal}</div>
      <div class='stat' style='background:${sqlColor};'>SQL disabled: ${disabledCount}/${podCount || 0}</div>`;
    }

    function renderSeries(series) {
      const div = document.getElementById('series');
      if (!series || series.length === 0) { div.innerHTML = '<p>No per-second data.</p>'; return; }
      const fmtBytes = (n) => {
        if (n == null) return '';
        if (n >= 1e9) return (n / 1e9).toFixed(2) + ' GB';
        if (n >= 1e6) return (n / 1e6).toFixed(2) + ' MB';
        if (n >= 1e3) return (n / 1e3).toFixed(2) + ' KB';
        return n + ' B';
      };
      const rows = series.map(b => {
        const ts = b.timestamp || b.Timestamp;
        const dt = ts ? new Date(ts).toISOString() : '';
        const req = b.requests ?? b.Requests ?? 0;
        const ok = b.success ?? b.Success ?? 0;
        const fail = b.failures ?? b.Failures ?? 0;
        const bytes = b.bytes ?? b.Bytes ?? 0;
        return `<tr><td>${dt}</td><td>${req}</td><td>${ok}</td><td>${fail}</td><td>${fmtBytes(bytes)}</td></tr>`;
      }).join('');
      div.innerHTML = `<table style='width:100%; border-collapse: collapse; margin-top: 10px;'>
        <thead><tr><th>Second (UTC)</th><th>Requests</th><th>Success</th><th>Failures</th><th>Total Bytes</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>`;
    }

    function renderPods(pods) {
      const div = document.getElementById('pods');
      if (!pods || pods.length === 0) { div.innerHTML = '<p>No pod data.</p>'; return; }
      const rows = pods.map(p => `<tr><td>${p.podName}</td><td>${p.totalRequests}</td><td>${p.httpRequests ?? 0}</td><td>${p.grpcRequests ?? 0}</td><td>${p.inFlight}</td><td>${(p.perIp||[]).length}</td><td>${p.sqlDisabled ? 'Yes' : 'No'}</td><td>${p.podIp || ''}</td><td>${new Date(p.started).toISOString()}</td></tr>`).join('');
      div.innerHTML = `<table style='width:100%; border-collapse: collapse; margin-top: 10px;'>
        <thead><tr><th>Pod</th><th>Total Requests</th><th>HTTP</th><th>gRPC</th><th>In Flight</th><th>Unique IPs</th><th>SQL Disabled</th><th>Pod IP</th><th>Started</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>`;
    }

    function renderIps(pods) {
      const body = document.getElementById('ipBody');
      if (!pods || pods.length === 0) { body.innerHTML = '<tr><td colspan=\'6\'>No data</td></tr>'; return; }
      const map = new Map();
      for (const p of pods) {
        if (!p.perIp) continue;
        for (const ip of p.perIp) {
          if (!map.has(ip.ip)) map.set(ip.ip, {Requests:0, TotalBytes:0, LastDurationMs:0, AvgDurationMs:0, LastSeen: ip.lastSeen});
          const agg = map.get(ip.ip);
          agg.Requests += ip.requests;
          agg.TotalBytes += ip.totalBytes;
          agg.LastDurationMs = ip.lastDurationMs;
          agg.AvgDurationMs = ip.avgDurationMs;
          agg.LastSeen = ip.lastSeen;
        }
      }
      const fmtBytes = (n) => {
        if (n == null) return '';
        if (n >= 1e9) return (n / 1e9).toFixed(2) + ' GB';
        if (n >= 1e6) return (n / 1e6).toFixed(2) + ' MB';
        if (n >= 1e3) return (n / 1e3).toFixed(2) + ' KB';
        return n + ' B';
      };
      const fmtDuration = (ms) => {
        if (ms == null || isNaN(ms)) return '';
        if (ms >= 60000) return (ms / 60000).toFixed(2) + ' min';
        if (ms >= 1000) return (ms / 1000).toFixed(2) + ' s';
        return ms.toFixed(1) + ' ms';
      };
      const rows = Array.from(map.entries()).map(([key, ip]) => `<tr><td>${key}</td><td>${ip.Requests}</td><td>${fmtBytes(ip.TotalBytes)}</td><td>${fmtDuration(ip.LastDurationMs)}</td><td>${fmtDuration(ip.AvgDurationMs)}</td><td>${ip.LastSeen}</td></tr>`).join('');
      body.innerHTML = rows || '<tr><td colspan=\'6\'>No data</td></tr>';
    }

    async function loadMessages(limitOverride) {
      if (ROLE !== 'messages') return; // only messages role fetches
      const div = document.getElementById('messages');
      try {
        const limit = limitOverride || MSG_LIMIT;
        const res = await fetch(`/messages?limit=${limit}${limit === 5 ? '&mode=full' : ''}`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        const items = Array.isArray(data?.items) ? data.items : [];
        if (items.length === 0) { div.innerHTML = '<p>No messages (SQL disabled or table empty).</p>'; return; }
        const rows = items.map(m => {
          const dur = Number.isFinite(m?.duration_ms) ? `${m.duration_ms} ms` : '';
          const msg = m?.payload_utf8 ? `<pre style='white-space: pre-wrap; margin:0;'>${(m.payload_utf8 || '').substring(0,500)}</pre>` : '';
          return `<tr><td>${m.id}</td><td>${m.created_at}</td><td>${fmtBytes(m.payload_size)}</td><td>${dur}</td>${limit === 5 ? `<td>${msg}</td>` : ''}</tr>`;
        }).join('');
        const duration = Number.isFinite(data?.duration_ms) ? `${data.duration_ms} ms` : '';
        const extraCol = limit === 5 ? '<th>Message</th>' : '';
        div.innerHTML = `<h3>Recent messages (latest ${items.length})</h3>
        <p>Fetch duration: ${duration}</p>
        <table style='width:100%; border-collapse: collapse; margin-top: 10px;'>
          <thead><tr><th>ID</th><th>Created</th><th>Size</th><th>Fetch ms</th>${extraCol}</tr></thead>
          <tbody>${rows}</tbody>
        </table>`;
      } catch (e) {
        div.innerHTML = `<p>Messages load failed: ${e}</p>`;
      }
    }

    async function load() {
      try {
        const data = await fetchStats();
        const pods = Array.isArray(data?.pods) ? data.pods : [];
        const series = Array.isArray(data?.aggregatedSeries) ? data.aggregatedSeries : [];
        const total = Number.isFinite(data?.totalRequests) ? data.totalRequests : 0;
        const inflight = Number.isFinite(data?.inFlight) ? data.inFlight : 0;
        const httpTotal = pods.reduce((s, p) => s + (p.httpRequests ?? 0), 0);
        const grpcTotal = pods.reduce((s, p) => s + (p.grpcRequests ?? 0), 0);
        const disabledCount = pods.filter(p => p.sqlDisabled).length;
        renderSummary(total, inflight, pods.length, httpTotal, grpcTotal, disabledCount);
        renderSeries(series);
        renderPods(pods);
        renderIps(pods);
        const messagesDiv = document.getElementById('messages');
        if (ROLE === 'messages') {
          messagesDiv.innerHTML = `<button onclick='loadMessages(10)'>Load last 10 (size only)</button>
          <button onclick='loadMessages(5)'>Load last 5 (full)</button>`;
        } else if (ROLE === 'aggregator') {
          messagesDiv.innerHTML = '<p>Messages disabled on aggregator role.</p>';
        } else {
          messagesDiv.innerHTML = '<p>Messages not loaded on ingester role.</p>';
        }
      } catch(e) {
        console.error('load failed', e);
        const body = document.getElementById('ipBody');
        if (body) body.innerHTML = `<tr><td colspan='6'>Error loading stats: ${e}</td></tr>`;
      }
    }
    load();
    setIntervalMs(500);
  </script>
</body>
</html>";
        return template.Replace("{{ROLE}}", roleSafe);
    }
}

internal static class SqlReadRenderer
{
    public static string Render()
    {
        const string template = @"<!DOCTYPE html>
<html lang=""en"">
<head>
  <meta charset=""UTF-8"">
  <title>SQL Read Dashboard</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 20px; background: #0f172a; color: #e2e8f0; }
    h1 { margin-bottom: 10px; }
    .stat { display: inline-block; margin-right: 12px; padding: 8px 12px; background: #1e293b; border-radius: 6px; }
    table { width: 100%; border-collapse: collapse; margin-top: 12px; }
    th, td { padding: 8px 10px; border-bottom: 1px solid #334155; }
    th { text-align: left; background: #1e293b; }
    tr:nth-child(even) { background: #111827; }
    button { padding: 8px 12px; margin-right: 10px; background: #1d4ed8; color: white; border: none; border-radius: 6px; cursor: pointer; }
    button:hover { background: #2563eb; }
  </style>
</head>
<body>
  <h1>SQL Read Dashboard</h1>
  <p>Manual fetch to gauge SQL read performance</p>
  <div>
    <button onclick='loadMeta()'>Load last 10 (size + fetch ms)</button>
    <button onclick='loadFull()'>Load last 5 (full message)</button>
  </div>
  <div id='result'></div>

  <script>
    const fmtBytes = (n) => {
      if (n == null) return '';
      if (n >= 1e9) return (n / 1e9).toFixed(2) + ' GB';
      if (n >= 1e6) return (n / 1e6).toFixed(2) + ' MB';
      if (n >= 1e3) return (n / 1e3).toFixed(2) + ' KB';
      return n + ' B';
    };

    async function fetchMessages(limit, full) {
      const mode = full ? '&mode=full' : '';
      const res = await fetch(`/messages?limit=${limit}${mode}`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.json();
    }

    function renderTable(items, full, totalDuration) {
      if (!items || items.length === 0) return '<p>No data.</p>';
      const extraHead = full ? '<th>Message (truncated)</th>' : '';
      const rows = items.map(m => {
        const dur = Number.isFinite(m?.duration_ms) ? `${m.duration_ms} ms` : '';
        const msg = full ? `<pre style='white-space: pre-wrap; margin:0;'>${(m.payload_utf8 || '').substring(0,500)}</pre>` : '';
        return `<tr><td>${m.id}</td><td>${m.created_at}</td><td>${fmtBytes(m.payload_size)}</td><td>${dur}</td>${full ? `<td>${msg}</td>` : ''}</tr>`;
      }).join('');
      const extraDur = Number.isFinite(totalDuration) ? `Total fetch: ${totalDuration} ms` : '';
      return `<p>${extraDur}</p>
        <table>
          <thead><tr><th>ID</th><th>Created</th><th>Size</th><th>Fetch ms</th>${extraHead}</tr></thead>
          <tbody>${rows}</tbody>
        </table>`;
    }

    async function loadMeta() {
      const div = document.getElementById('result');
      div.innerHTML = '<p>Loading...</p>';
      try {
        const data = await fetchMessages(10, false);
        const html = renderTable(data.items, false, data.duration_ms);
        div.innerHTML = `<h3>Last 10 (size + fetch ms)</h3>${html}`;
      } catch (e) {
        div.innerHTML = `<p>Load failed: ${e}</p>`;
      }
    }

    async function loadFull() {
      const div = document.getElementById('result');
      div.innerHTML = '<p>Loading...</p>';
      try {
        const data = await fetchMessages(5, true);
        const html = renderTable(data.items, true, data.duration_ms);
        div.innerHTML = `<h3>Last 5 (full messages)</h3>${html}`;
      } catch (e) {
        div.innerHTML = `<p>Load failed: ${e}</p>`;
      }
    }
  </script>
</body>
</html>";
        return template;
    }
}
