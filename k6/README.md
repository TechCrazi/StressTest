## K6 Testing
- Update full-loadtest.js file

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
