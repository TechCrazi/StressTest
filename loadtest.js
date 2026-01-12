import http from "k6/http";
import grpc from "k6/net/grpc";
import { check } from "k6";
import { Counter, Trend } from "k6/metrics";

/* -----------------------------------------------------------
   EASY-CONFIG SECTION (edit these)
----------------------------------------------------------- */

// Test intensity
const VUS = 100;                    // number of concurrent virtual users
const TEST_DURATION = "30s";        // how long to run the test

// Common size units
const KB = 1024;
const MB = 1024 * 1024;

// File size buckets (in bytes)
const SMALL_MIN  = 1 * KB;
const SMALL_MAX  = 100 * KB;        // roughly matches "1–100 KB"
const MED_MIN    = 100 * KB;
const MED_MAX    = 1 * MB;
const LARGE_MIN  = 1 * MB;
const LARGE_MAX  = 10 * MB;
const HUGE_MIN   = 10 * MB;
const HUGE_MAX   = 50 * MB;

// Bucket distribution (must roughly sum to 1.0)
const P_SMALL = 0.30;   // 30% of messages in 1–100 KB
const P_MED   = 0.30;   // 30% in 100 KB–1 MB
const P_LARGE = 0.30;   // 30% in 1–10 MB
const P_HUGE  = 0.10;   // 10% in 10–50 MB

// Protocol split
const GRPC_RATIO = 0.8;             // 0.8 = 80% gRPC, 20% HTTP

// Endpoint configuration
const BASE_HOST = "stresstest.mc.dev.lkdatacenter.com";
const GRPC_TARGET = `${BASE_HOST}:443`;
const HTTP_URL = `https://${BASE_HOST}/write`;
const HTTP_CONTENT_TYPE = "application/hl7-v2";

// Retries
const MAX_RETRIES = 3;              // extra attempts after first failure


/* -----------------------------------------------------------
   CUSTOM METRICS
----------------------------------------------------------- */

// Per-protocol success/fail (per *message*, not per attempt)
export const grpc_success = new Counter("grpc_success");
export const grpc_fail    = new Counter("grpc_fail");
export const http_success = new Counter("http_success");
export const http_fail    = new Counter("http_fail");

// Per-protocol request counts and logical bytes (per *attempt*)
export const grpc_reqs    = new Counter("grpc_reqs");
export const http_reqs    = new Counter("http_reqs");
// IMPORTANT: for these two we interpret "count" as "total bytes"
export const grpc_bytes   = new Counter("grpc_bytes");
export const http_bytes   = new Counter("http_bytes");

// Retransmission metrics (per *message*)
export const http_retx_attempts = new Counter("http_retx_attempts");
export const http_retx_success  = new Counter("http_retx_success");
export const http_retx_fail     = new Counter("http_retx_fail");

export const grpc_retx_attempts = new Counter("grpc_retx_attempts");
export const grpc_retx_success  = new Counter("grpc_retx_success");
export const grpc_retx_fail     = new Counter("grpc_retx_fail");

// ---------- Size buckets split by protocol (per *attempt*) ----------
// Small (1 KB – 100 KB)
export const bucket_small_http_reqs = new Counter("bucket_small_http_reqs");
export const bucket_small_http_fail = new Counter("bucket_small_http_fail");
export const bucket_small_grpc_reqs = new Counter("bucket_small_grpc_reqs");
export const bucket_small_grpc_fail = new Counter("bucket_small_grpc_fail");

// Medium (100 KB – 1 MB)
export const bucket_med_http_reqs = new Counter("bucket_med_http_reqs");
export const bucket_med_http_fail = new Counter("bucket_med_http_fail");
export const bucket_med_grpc_reqs = new Counter("bucket_med_grpc_reqs");
export const bucket_med_grpc_fail = new Counter("bucket_med_grpc_fail");

// Large (1 MB – 10 MB)
export const bucket_large_http_reqs = new Counter("bucket_large_http_reqs");
export const bucket_large_http_fail = new Counter("bucket_large_http_fail");
export const bucket_large_grpc_reqs = new Counter("bucket_large_grpc_reqs");
export const bucket_large_grpc_fail = new Counter("bucket_large_grpc_fail");

// Huge (10 MB – 50 MB)
export const bucket_huge_http_reqs = new Counter("bucket_huge_http_reqs");
export const bucket_huge_http_fail = new Counter("bucket_huge_http_fail");
export const bucket_huge_grpc_reqs = new Counter("bucket_huge_grpc_reqs");
export const bucket_huge_grpc_fail = new Counter("bucket_huge_grpc_fail");

// Average time per size bucket, split by protocol (per attempt)
export const small_http_time = new Trend("small_http_time");
export const small_grpc_time = new Trend("small_grpc_time");

export const med_http_time   = new Trend("med_http_time");
export const med_grpc_time   = new Trend("med_grpc_time");

export const large_http_time = new Trend("large_http_time");
export const large_grpc_time = new Trend("large_grpc_time");

export const huge_http_time  = new Trend("huge_http_time");
export const huge_grpc_time  = new Trend("huge_grpc_time");


/* -----------------------------------------------------------
   TEST CONFIGURATION
----------------------------------------------------------- */
export const options = {
  insecureSkipTLSVerify: true,
  vus: VUS,
  duration: TEST_DURATION,
};


/* -----------------------------------------------------------
   GRPC SETUP
----------------------------------------------------------- */
const client = new grpc.Client();
client.load(["./Protos"], "stress.proto");


/* -----------------------------------------------------------
   HL7 GENERATION HELPERS
----------------------------------------------------------- */

function randInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function randChoice(arr) {
  return arr[randInt(0, arr.length - 1)];
}

function padWithFiller(base, targetSize) {
  let msg = base;
  if (msg.length >= targetSize) {
    return msg;
  }
  const fillerLen = targetSize - msg.length;
  const filler = "X".repeat(fillerLen);
  return msg + "\rNTE|1|L|" + filler;
}

// Simple-ish ADT^A01 message
function generateADTMessage(targetSizeBytes) {
  const ts = new Date().toISOString().replace(/[-:]/g, "").split(".")[0];
  const sex = randChoice(["M", "F", "U"]);
  const firstNames = ["JOHN", "JANE", "ALEX", "MARIA", "ROBERT", "PRIYA"];
  const lastNames = ["DOE", "SMITH", "JOHNSON", "LEE", "PATEL", "GARCIA"];

  const fn = randChoice(firstNames);
  const ln = randChoice(lastNames);
  const pidNum = randInt(100000, 999999);
  const dob = randInt(1950, 2015) + "0101";

  const msh =
    `MSH|^~\\&|K6APP|K6FAC|HOSPITAL|FACILITY|${ts}||ADT^A01|MSG${pidNum}|P|2.8`;
  const pid =
    `PID|1||${pidNum}^^^K6MRN||${ln}^${fn}||${dob}|${sex}|||123 MAIN ST^^METROPOLIS^NJ^07001||5551234567`;
  const pv1 = `PV1|1|I|ER^01^01||||1234^ATTEND^PHYSICIAN|||||||||||V`;
  const obx = `OBX|1|TX|NOTE^ADMISSION NOTE||Patient admitted for testing purposes|||N`;

  let msg = [msh, pid, pv1, obx].join("\r");
  return padWithFiller(msg, targetSizeBytes);
}

// ORM message with “PDF” stored in OBX as ED data type
function generateORMWithPdfMessage(targetSizeBytes) {
  const ts = new Date().toISOString().replace(/[-:]/g, "").split(".")[0];
  const orderId = randInt(1000000, 9999999);
  const procCode = randChoice(["XRAYCHEST", "CTHEAD", "MRIBRAIN", "USABD"]);
  const patientId = randInt(100000, 999999);

  const msh =
    `MSH|^~\\&|K6APP|K6FAC|LIS|LAB|${ts}||ORM^O01|ORD${orderId}|P|2.8`;
  const pid =
    `PID|1||${patientId}^^^K6MRN||DOE^TESTPATIENT||19750101|U|||456 TEST ST^^TESTVILLE^TX^75001||5559876543`;
  const orc =
    `ORC|NW|${orderId}||${orderId}|||1^ONCE^^^^S||${ts}||||1234^ORDERING^PROVIDER`;
  const obr =
    `OBR|1|${orderId}|${orderId}|${procCode}^${procCode} DESC^L||${ts}||||||||1234^ORDERING^PROVIDER`;

  const pdfHeader = "OBX|1|ED|PDF^REPORT||^Base64^PDF^";
  const footer = "|||N";

  const baseWithoutPdf =
    [msh, pid, orc, obr].join("\r") + "\r" + pdfHeader + footer;
  const remaining = Math.max(targetSizeBytes - baseWithoutPdf.length, 100);

  const pdfBlob = "A".repeat(remaining);
  const obx = `${pdfHeader}${pdfBlob}${footer}`;

  let msg = [msh, pid, orc, obr, obx].join("\r");
  return msg;
}

// Randomly choose ADT vs ORM+PDF
function generateHL7Message(targetSizeBytes) {
  const useADT = Math.random() < 0.5;
  if (useADT) {
    return generateADTMessage(targetSizeBytes);
  }
  return generateORMWithPdfMessage(targetSizeBytes);
}

// Choose file size based on bucket distribution
function chooseTargetSize() {
  const p = Math.random();

  if (p < P_SMALL) {
    return randInt(SMALL_MIN, SMALL_MAX);
  }
  if (p < P_SMALL + P_MED) {
    return randInt(MED_MIN, MED_MAX);
  }
  if (p < P_SMALL + P_MED + P_LARGE) {
    return randInt(LARGE_MIN, LARGE_MAX);
  }
  // else huge
  return randInt(HUGE_MIN, HUGE_MAX);
}


/* -----------------------------------------------------------
   MAIN TEST LOOP
----------------------------------------------------------- */
export default function () {
  const targetSize = chooseTargetSize();

  const hl7 = generateHL7Message(targetSize);
  const sizeBytes = hl7.length;

  const useGrpc = Math.random() < GRPC_RATIO;

  if (useGrpc) {
    doGrpc(sizeBytes);
  } else {
    doHttp(sizeBytes, hl7);
  }
}


/* -----------------------------------------------------------
   SIZE BUCKET RECORDER (per attempt, all protocols)
----------------------------------------------------------- */
function recordBucket(size, ok, protocol, durationMs) {
  const isHttp = protocol === "http";

  const bucket = (reqs, fail) => {
    reqs.add(1);
    if (!ok) fail.add(1);
  };

  if (size < 100 * KB) {
    if (isHttp) {
      bucket(bucket_small_http_reqs, bucket_small_http_fail);
      small_http_time.add(durationMs);
    } else {
      bucket(bucket_small_grpc_reqs, bucket_small_grpc_fail);
      small_grpc_time.add(durationMs);
    }
    return;
  }

  if (size < 1 * MB) {
    if (isHttp) {
      bucket(bucket_med_http_reqs, bucket_med_http_fail);
      med_http_time.add(durationMs);
    } else {
      bucket(bucket_med_grpc_reqs, bucket_med_grpc_fail);
      med_grpc_time.add(durationMs);
    }
    return;
  }

  if (size < 10 * MB) {
    if (isHttp) {
      bucket(bucket_large_http_reqs, bucket_large_http_fail);
      large_http_time.add(durationMs);
    } else {
      bucket(bucket_large_grpc_reqs, bucket_large_grpc_fail);
      large_grpc_time.add(durationMs);
    }
    return;
  }

  // Huge (10MB – 50MB)
  if (isHttp) {
    bucket(bucket_huge_http_reqs, bucket_huge_http_fail);
    huge_http_time.add(durationMs);
  } else {
    bucket(bucket_huge_grpc_reqs, bucket_huge_grpc_fail);
    huge_grpc_time.add(durationMs);
  }
}


/* -----------------------------------------------------------
   GRPC REQUEST with retries (message-level success/fail)
----------------------------------------------------------- */
function doGrpc(sizeBytes) {
  let attempts = 0;
  let success = false;

  while (attempts === 0 || (attempts <= MAX_RETRIES && !success)) {
    attempts++;

    grpc_reqs.add(1);
    grpc_bytes.add(sizeBytes);  // "count" of this Counter is total bytes

    let res;
    const start = Date.now();

    try {
      client.connect(GRPC_TARGET);
      res = client.invoke("stress.StressTest/Write", { sizeBytes });
      success = !!(res && res.status === grpc.StatusOK);
      check(res || { status: 0 }, { "gRPC status is OK": () => success });
    } catch (e) {
      success = false;
      check(null, { "gRPC status is OK": () => false });
    } finally {
      const duration = Date.now() - start;
      recordBucket(sizeBytes, success, "grpc", duration);
      client.close();
    }

    if (success) break;
  }

  // Message-level accounting (once per logical message)
  if (success) {
    grpc_success.add(1);
    if (attempts > 1) {
      grpc_retx_attempts.add(attempts - 1);
      grpc_retx_success.add(1);
    }
  } else {
    grpc_fail.add(1);
    if (attempts > 1) {
      grpc_retx_attempts.add(attempts - 1);
      grpc_retx_fail.add(1);
    }
  }
}


/* -----------------------------------------------------------
   HTTP REQUEST with retries (message-level success/fail)
----------------------------------------------------------- */
function doHttp(sizeBytes, hl7Body) {
  let attempts = 0;
  let success = false;

  while (attempts === 0 || (attempts <= MAX_RETRIES && !success)) {
    attempts++;

    http_reqs.add(1);
    http_bytes.add(sizeBytes);   // "count" = total bytes across attempts

    const start = Date.now();
    let res;
    try {
      res = http.post(
        HTTP_URL,
        hl7Body,
        { headers: { "Content-Type": HTTP_CONTENT_TYPE } }
      );
      success = res.status >= 200 && res.status < 300; // accept 2xx (202 from queued writes)
      check(res, { "HTTP status is 2xx": () => success });
    } catch (e) {
      success = false;
      check(null, { "HTTP status is 2xx": () => false });
    } finally {
      const duration = Date.now() - start;
      recordBucket(sizeBytes, success, "http", duration);
    }

    if (success) break;
  }

  if (success) {
    http_success.add(1);
    if (attempts > 1) {
      http_retx_attempts.add(attempts - 1);
      http_retx_success.add(1);
    }
  } else {
    http_fail.add(1);
    if (attempts > 1) {
      http_retx_attempts.add(attempts - 1);
      http_retx_fail.add(1);
    }
  }
}


/* -----------------------------------------------------------
   UTIL FUNCTIONS
----------------------------------------------------------- */
function formatBytes(bytes) {
  if (!bytes || bytes <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return (bytes / Math.pow(1024, i)).toFixed(2) + " " + units[i];
}

function get(data, name, key) {
  const m = data.metrics[name];
  return m && m.values && typeof m.values[key] === "number"
    ? m.values[key]
    : 0;
}

const pct = (n, d) => (d ? ((n / d) * 100).toFixed(1) : "0.0");


/* -----------------------------------------------------------
   SUMMARY SECTION
----------------------------------------------------------- */
export function handleSummary(data) {
  const lines = [];

  const getCount = (name) => get(data, name, "count");

  // Message-level totals
  const grpcSucc = getCount("grpc_success");
  const grpcFail = getCount("grpc_fail");
  const httpSucc = getCount("http_success");
  const httpFail = getCount("http_fail");

  const grpcMsgs = grpcSucc + grpcFail;
  const httpMsgs = httpSucc + httpFail;
  const totalMsgs = grpcMsgs + httpMsgs;

  // Attempt-level totals
  const grpcReqs = getCount("grpc_reqs");
  const httpReqs = getCount("http_reqs");
  const totalReqs = grpcReqs + httpReqs;

  // Bytes (for Counter we use "count" as accumulated value)
  const grpcBytes = getCount("grpc_bytes");
  const httpBytes = getCount("http_bytes");
  const totalBytes = grpcBytes + httpBytes;

  // Retransmission totals
  const httpRtxAtt  = getCount("http_retx_attempts");
  const httpRtxSucc = getCount("http_retx_success");
  const httpRtxFail = getCount("http_retx_fail");

  const grpcRtxAtt  = getCount("grpc_retx_attempts");
  const grpcRtxSucc = getCount("grpc_retx_success");
  const grpcRtxFail = getCount("grpc_retx_fail");

  // Bucket values (HTTP + gRPC)
  const b = (name) => getCount(name);

  // Avg durations per size bucket, split by protocol
  const avgSmallHttp = get(data, "small_http_time", "avg");
  const avgSmallGrpc = get(data, "small_grpc_time", "avg");

  const avgMedHttp   = get(data, "med_http_time", "avg");
  const avgMedGrpc   = get(data, "med_grpc_time", "avg");

  const avgLargeHttp = get(data, "large_http_time", "avg");
  const avgLargeGrpc = get(data, "large_grpc_time", "avg");

  const avgHugeHttp  = get(data, "huge_http_time", "avg");
  const avgHugeGrpc  = get(data, "huge_grpc_time", "avg");

  /* -----------------------------------------------------------
     1) SIMPLE TEST SUMMARY
  ----------------------------------------------------------- */

  lines.push("\n\n========== SIMPLE TEST SUMMARY ==========");
  lines.push(`Total requests sent (logical messages): ${totalMsgs}`);
  lines.push(
    `gRPC requests: ${grpcMsgs} (success: ${grpcSucc}, failed: ${grpcFail})`
  );
  lines.push(
    `HTTP requests: ${httpMsgs} (success: ${httpSucc}, failed: ${httpFail})`
  );
  lines.push(`Total attempts (including retries): ${totalReqs}`);
  lines.push(
    `Total data pushed (logical HL7 size across all attempts): ${formatBytes(totalBytes)}`
  );
  lines.push("=========================================\n");

  /* -----------------------------------------------------------
     2) RETRANSMISSIONS
  ----------------------------------------------------------- */
  lines.push("========== RETRANSMISSIONS ==========");
  lines.push(
    `HTTP: ${httpRtxSucc + httpRtxFail} messages retried ` +
      `(${httpRtxAtt} extra attempts in total); ` +
      `${httpRtxSucc} eventually succeeded, ${httpRtxFail} still failed.`
  );
  lines.push(
    `gRPC: ${grpcRtxSucc + grpcRtxFail} messages retried ` +
      `(${grpcRtxAtt} extra attempts in total); ` +
      `${grpcRtxSucc} eventually succeeded, ${grpcRtxFail} still failed.`
  );
  lines.push("======================================\n");

  /* -----------------------------------------------------------
     3) FAILURE RATE BY REQUEST SIZE (HTTP vs gRPC)
  ----------------------------------------------------------- */

  lines.push("========== FAILURE RATE BY REQUEST SIZE (HTTP vs gRPC) ==========");

  const sizeGroup = (label, httpReq, httpFail, grpcReq, grpcFail) => {
    lines.push(`\n${label}`);
    lines.push(
      `  HTTP: ${httpReq} requests, ${httpFail} failed (${pct(
        httpFail,
        httpReq
      )}% failure)`
    );
    lines.push(
      `  gRPC: ${grpcReq} requests, ${grpcFail} failed (${pct(
        grpcFail,
        grpcReq
      )}% failure)`
    );
  };

  sizeGroup(
    "Small (1 KB – 100 KB):",
    b("bucket_small_http_reqs"), b("bucket_small_http_fail"),
    b("bucket_small_grpc_reqs"), b("bucket_small_grpc_fail")
  );

  sizeGroup(
    "Medium (100 KB – 1 MB):",
    b("bucket_med_http_reqs"), b("bucket_med_http_fail"),
    b("bucket_med_grpc_reqs"), b("bucket_med_grpc_fail")
  );

  sizeGroup(
    "Large (1 MB – 10 MB):",
    b("bucket_large_http_reqs"), b("bucket_large_http_fail"),
    b("bucket_large_grpc_reqs"), b("bucket_large_grpc_fail")
  );

  sizeGroup(
    "Huge (10 MB – 50 MB):",
    b("bucket_huge_http_reqs"), b("bucket_huge_http_fail"),
    b("bucket_huge_grpc_reqs"), b("bucket_huge_grpc_fail")
  );

  lines.push("\n==============================================================\n");

  /* -----------------------------------------------------------
     4) AVERAGE TIME PER SIZE BUCKET, SPLIT BY PROTOCOL
  ----------------------------------------------------------- */

  lines.push(
    "===== Average transmit time per message size (per attempt, HTTP vs gRPC) ====="
  );
  lines.push(
    `Small (1 KB – 100 KB): HTTP ${avgSmallHttp.toFixed(1)} ms | gRPC ${avgSmallGrpc.toFixed(1)} ms`
  );
  lines.push(
    `Medium (100 KB – 1 MB): HTTP ${avgMedHttp.toFixed(1)} ms | gRPC ${avgMedGrpc.toFixed(1)} ms`
  );
  lines.push(
    `Large (1 MB – 10 MB): HTTP ${avgLargeHttp.toFixed(1)} ms | gRPC ${avgLargeGrpc.toFixed(1)} ms`
  );
  lines.push(
    `Huge (10 MB – 50 MB): HTTP ${avgHugeHttp.toFixed(1)} ms | gRPC ${avgHugeGrpc.toFixed(1)} ms`
  );
  lines.push(
    "===============================================================================\n"
  );

  return { stdout: lines.join("\n") };
}
