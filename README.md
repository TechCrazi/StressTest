# SQL stress test service (.NET 8)

ASP.NET Core service to pressure-test Traefik, EKS, and SQL Server (RDS SQL Web). Supports HTTP and gRPC writes with random/HL7/passthrough payloads (default 1 KB–50 MB), optional SQL disable, ring-based aggregation, OTLP telemetry (`service.name = sql-stress`), and a separate SQL read dashboard. Kestrel/gRPC limits are raised to 60 MB to allow 50 MB payloads.

## Features
- Three roles via `APP_ROLE`:
  - `ingester`: handles `/write` and gRPC writes; can disable SQL to isolate LB/app behavior.
  - `aggregator`: ring registry + `/dashboard` + `/stats` only (no SQL writes/reads).
  - `messages`: SQL reader only; serves `/messages` JSON and `/sqlread` manual dashboard.
- HTTP `/write` and gRPC `stress.StressTest/Write` insert random/HL7/passthrough payloads (or simulate when SQL is disabled).
- Auto-creates table `stress_writes` on startup if missing.
- Dashboard `/dashboard` + JSON `/stats` shows per-second totals, per-pod stats (HTTP/gRPC splits, SQL disabled flag), per-IP metrics; aggregator pod keeps a ring of active pods even when idle.
- SQL read dashboard `/sqlread` (messages role) has buttons to fetch:
  - Last 10 rows (size + per-row fetch ms, no payload),
  - Last 5 rows (full payload, truncated to 5 MB per row to avoid OOM).
- OTLP tracing/metrics (AspNetCore, HttpClient, SqlClient + custom meters `sqlstress.http.*`, `sqlstress.grpc.*`), `service.name = sql-stress`.
- Ports: 8080 HTTP (write/status/health/dashboards/stats/messages/sqlread), 8081 gRPC (h2c). Traefik IngressRoute splits HTTP vs gRPC and buffers to 64 MB.

## Environment variables
Required DB:
- `SQL_SERVER` – RDS endpoint.
- `SQL_DATABASE`
- `SQL_USER`
- `SQL_PASSWORD`

Optional DB/tuning (defaults in parentheses):
- `SQL_PORT` (1433)
- `SQL_TABLE` (`stress_writes`)
- `SQL_ENCRYPT` (`true`)
- `SQL_TRUST_SERVER_CERT` (`false`)
- `SQL_TIMEOUT_SECONDS` (60) – command timeout
- `SQL_CONNECT_TIMEOUT_SECONDS` (30)
- `SQL_MAX_POOL_SIZE` (200)
- `SQL_MIN_POOL_SIZE` (10)
- `MAX_INFLIGHT_SQL` (0 = unlimited) – cap concurrent SQL writes per pod; use 8–32 to protect RDS

Payload/behavior:
- `PAYLOAD_MIN_BYTES` (1024)
- `PAYLOAD_MAX_BYTES` (52428800) – 50 MB
- `DISABLE_SQL` (`false`) – if `true`, skip DB writes but still count traffic
- `SQL_INSERT_MODE` (`random`) – `random` generates payload; `body` uses HTTP POST body as payload (for full end-to-end tests); `hl7` generates synthetic HL7 text
- `APP_ROLE` (`ingester`) – `ingester` | `aggregator` | `messages`

Ports and identity:
- `APP_PORT` (8080) – HTTP
- `APP_GRPC_PORT` (8081) – gRPC
- `POD_NAME` (defaults to hostname)
- `POD_NAMESPACE` – required for peer discovery when using K8s

Ring/peer aggregation:
- `RING_ENDPOINT` – URL to post heartbeat snapshots (e.g., `http://sql-stress-aggregator.stresstest.svc.cluster.local/register`)
- `PEER_SERVICE_NAME` (for K8s endpoint discovery, e.g., `sql-stress`)
- `PEER_SERVICE_PORT` (8080)
- `PEER_DASHBOARD_URLS` – optional comma-separated base URLs to pull peer `/stats?scope=local`

Telemetry:
- `OTLP_ENDPOINT` – OTLP gRPC/HTTP endpoint (e.g., `http://otel-collector:4317`)
- `OTLP_HEADERS` – extra headers (e.g., `authorization=Bearer abc123`)
- `OTLP_INSECURE` (`false`) – skip TLS validation

Other:
- `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT` (`0` in manifests to keep ICU)
- `SQL_TABLE` (`stress_writes`)

## Endpoints
- `GET /healthz` – DB ping (SQL required).
- `GET /status` – pod status snapshot.
- `GET|POST /write` – single write (or simulated if `DISABLE_SQL=true`).
- `GET /messages?limit=10` – latest rows from SQL; size-only by default.
- `GET /messages?limit=5&mode=full` – full message payload (truncated to 5 MB per row) + per-row fetch duration.
- `GET /sqlread` – manual SQL read dashboard (messages role).
- `GET /dashboard` – HTML dashboard (aggregator role).
- `GET /stats` – JSON used by the dashboard (aggregated ring-aware).
- gRPC `stress.StressTest/Write` – inserts random payload.
- gRPC `stress.StressTest/Healthz` – DB health.
- `POST /register` – heartbeat ingestion (aggregator).

## Build and run locally
```bash
dotnet restore

SQL_SERVER="<rds-endpoint>" \
SQL_USER="user" \
SQL_PASSWORD="pass" \
SQL_DATABASE="dbname" \
dotnet run
```

## Container image
```bash
docker buildx build --platform linux/amd64,linux/arm64 -t ghcr.io/techcrazi/sql-stress:latest . --push
```

## Container Scan via Trivy

#### Install Trivy
```bash
brew install trivy
```

#### Scan Image
```bash
trivy image ghcr.io/techcrazi/sql-stress:latest
```


## Container Scan via Slim

#### Install Slim
```bash
brew install docker-slim
```

#### Run SQL Container for testing
```bash
docker run -d \
  --name sql-test \
  --platform linux/amd64 \
  -e "ACCEPT_EULA=Y" \
  -e "MSSQL_SA_PASSWORD=StrongPass123" \
  -p 1433:1433 \
  mcr.microsoft.com/mssql/server:2022-latest
```


##### Scan Image
```bash
slim build \
  --target ghcr.io/techcrazi/sql-stress:latest \
  --tag sql-stress:slim \
  --publish-port 9080:8080 \
  --http-probe-cmd 'GET:/healthz' \
  --http-probe-cmd 'GET:/status' \
  --http-probe-cmd 'GET:/write' \
  --http-probe-cmd 'GET:/dashboard' \
  --http-probe-cmd 'GET:/messages?limit=10' \
  --http-probe-cmd 'GET:/messages?limit=5&mode=full' \
  --http-probe-cmd 'GET:/sqlread' \
  --continue-after=probe \
  --copy-meta-artifacts ./slim-artifacts \
  --env SQL_SERVER=sql-test \
  --env SQL_DATABASE=stress \
  --env SQL_USER=sa \
  --env SQL_PASSWORD=StrongPass123 \
  --env SQL_ENCRYPT=false \
  --env SQL_TRUST_SERVER_CERT=true \
  --env SQL_CONNECT_TIMEOUT_SECONDS=1
```

##### Image Testing
```bash
slim build \
  --target ghcr.io/techcrazi/sql-stress:latest \
  --tag sql-stress:slim \
  --publish-port 9080:8080 \
  --publish-port 9081:8081 \
  --continue-after=enter \
  --copy-meta-artifacts ./slim-artifacts \
  --env SQL_SERVER=sql-test \
  --env SQL_DATABASE=stress \
  --env SQL_USER=sa \
  --env SQL_PASSWORD=StrongPass123 \
  --env SQL_ENCRYPT=false \
  --env SQL_TRUST_SERVER_CERT=true \
  --env SQL_CONNECT_TIMEOUT_SECONDS=1 \
  --env DISABLE_SQL=true
```


##### Push gRPC message
```bash
grpcurl -vv -plaintext \
  -proto Protos/stress.proto \
  -d '{"sizeBytes":1024}' \
  localhost:9081 stress.StressTest/Write
```


##### Push Slim Image to GHCR
```bash
docker tag sql-stress:slim ghcr.io/techcrazi/sql-stress:slim
docker push ghcr.io/techcrazi/sql-stress:slim
````



## Kubernetes (EKS + Traefik)
1) Edit `k8s/stress-app.yaml` for your image, host, and DB settings, then apply:
```bash
kubectl --kubeconfig k8s/multicare-dev-eks.config apply -f k8s/stress-app.yaml
```
2) Deployments/Services:
   - `sql-stress-ingester` (HTTP `/write`, gRPC) on ports 80->8080 and 81->8081; `APP_ROLE=ingester`.
   - `sql-stress-aggregator` (ring + `/dashboard` + `/stats`) on 80->8080; `APP_ROLE=aggregator`, `DISABLE_SQL=true`.
   - `sql-stress-messages` (SQL reader only; `/messages`, `/sqlread`) on 80->8080; `APP_ROLE=messages`, `DISABLE_SQL=true` (writes blocked; reads allowed).
3) IngressRoute splits:
   - gRPC `/stress.StressTest` → ingester (h2c, buffered).
   - HTTP `/write|/healthz|/status` → ingester.
   - `/dashboard|/stats` → aggregator.
   - `/messages|/sqlread` → messages reader.
   - Traefik middleware `sql-stress-buffer` allows 64 MB request bodies.

## Notes and limits
- Max request size/message size raised to 60 MB to permit 50 MB payloads; ensure Traefik and any proxies allow similar limits.
- Table schema (auto-created): `id UNIQUEIDENTIFIER`, `created_at DATETIME2`, `payload_size INT`, `payload VARBINARY(MAX)`.
- `/messages` full mode truncates per-row payload to 5 MB to avoid large in-memory blobs; size-only mode is lightweight.
- Set `PAYLOAD_MIN_BYTES` = `PAYLOAD_MAX_BYTES` for deterministic size. For heavier loads, tune `MAX_INFLIGHT_SQL`, pool settings, and RDS instance size.

## K6 Testing
- Update \k6\full-loadtest.js file

  - Test intensity
     - `const VUS` → 50                    `number of concurrent virtual users`
     - `const TEST_DURATION` → "1h"        `how long to run the test`

  - Common size units
    - `const KB` → 1024 `file size in KB`
    - `const MB` → 1024 * 1024 `file size in MB`

  - File size buckets (in bytes)
    - `const SMALL_MIN`  → 1 * KB
    - `const SMALL_MAX`  → 100 * KB
    - `const MED_MIN`    → 100 * KB
    - `const MED_MAX`    → 1 * MB
    - `const LARGE_MIN`  → 1 * MB
    - `const LARGE_MAX`  → 10 * MB
    - `const HUGE_MIN`   → 10 * MB
    - `const HUGE_MAX`   → 50 * MB

  - Bucket distribution (must roughly sum to 1.0)
    - `const P_SMALL` → 0.45   `45% of messages in 1–100 KB`
    - `const P_MED`   → 0.45   `45% in 100 KB–1 MB`
    - `const P_LARGE` → 0.05   `05% in 1–10 MB`
    - `const P_HUGE`  → 0.05   `05% in 10–50 MB`

  - Protocol split
    - `const GRPC_RATIO` → 0.8; `0.8 → 80% gRPC, 20% HTTP`

  - Endpoint configuration - Point this to the right ingest FQDN
    - `const BASE_HOST` → "stresstest.mc.dev.testme.com"


- Run K6 load test:
  - .\k6.exe run full-loadtest.js
  - .\k6.exe cloud run full-loadtest.js
