# Route Selection Benchmarks

Micro-benchmarks for `http::route::select_best_route` using
[divan](https://github.com/nvzqz/divan).

## Running

```bash
cargo bench -p agentgateway -F internal_benches -- bench
```

Integration-level throughput (measures the full proxy stack, not just routing):

```bash
cargo test -p agentgateway --test integration --release \
  -- throughput_bench --nocapture --ignored
```

---

## Results (first batch of optimizations)

All micro-benchmark numbers below are **median** latency on an Apple M-series
machine, release profile with `debug = "line-tables-only"`.

### Path matching (`bench`)

Each route has a unique `PathPrefix`; the request targets the last route,
forcing a full scan.

| Routes | Baseline (main) | Optimized | Δ |
|-------:|----------------|-----------|---|
| 1 | 46.95 ns | 82.74 ns | ≈ (noisy at 1 route) |
| 100 | 1.218 µs | 1.062 µs | **−13%** |
| 1000 | 12.91 µs | 11.70 µs | **−9%** |

### Query matching (`bench_with_query`)

Each route matches on a unique query parameter; the request targets the last
route, so every candidate is checked before a match is found.

| Routes | Baseline (main) | Optimized | Δ |
|-------:|----------------|-----------|---|
| 1 | 122.1 ns | 97.4 ns | −20% |
| 100 | 8.374 µs | 2.520 µs | **−70%** |
| 1000 | 74.66 µs | 25.49 µs | **−66%** |

### Integration throughput (`--release`, logging suppressed)

64 concurrent clients, 10 s window, 1000 routes with path+query constraints,
each client cycles through all routes.

| Metric | Baseline | Optimized |
|--------|----------|-----------|
| Requests/sec | 10,915 | 10,491 |
| P50 latency | 6.80 ms | 6.85 ms |
| P99 latency | 7.81 ms | 7.93 ms |

At 64 concurrency the gateway is throughput-limited by the client/wiremock
stack (~7 ms round trip), not by routing. Both branches sit at the same
wall — the routing gain is sub-microsecond relative to a 7 ms round trip.

---

## What improved and why

### 1 — Lazy `ParsedQuery` (largest win)

**Before:** `matches_request` called
`url::form_urlencoded::parse(q.as_bytes()).collect::<HashMap<_,_>>()`
on every candidate route, even when the previous N−1 candidates had already
parsed the identical query string.

**After:** `ParsedQuery` is an enum (`None` / `Unparsed(&str)` /
`Parsed(HashMap<…>)`) created once before the scan loop and passed as `&mut`
to each `matches_request` call. It parses the query string on the first
candidate that has a query constraint, then reuses the map for the rest.

**Impact:** O(N) allocations per request → O(1). At 1000 routes the saving
is 999 avoided `HashMap` allocations and parse passes. This is the dominant
improvement in the query-matching bench (−66–70%).

### 2 — `RequestMatchCtx` precomputed once before the scan loop (smaller win)

**Before:** each `matches_request` call re-derived:
- `request.uri().path()` — a method call returning `&str`
- `path.trim_end_matches('/')` — a new slice per call
- `request.method().as_str()` — a method call

**After:** these are computed once into `RequestMatchCtx` before the loop.

**Impact:** no allocations involved, so the saving is purely call overhead
avoided N times. Accounts for the ~9–13% improvement in path-only matching.

### 3 — `select_best_route` takes `&Stores` instead of `Stores` (near-zero)

**Before:** callers cloned `Stores` (a struct of several `Arc` fields) and
passed ownership into `select_best_route`. Each `Arc::clone` is an atomic
increment.

**After:** the function takes a shared reference; callers pass `&stores`.

**Impact:** `bench_stores_clone` measures the clone at **~3 ns**. At 1000
routes this is < 0.03% of the 12 µs scan — effectively noise. The change is
still correct (removes unnecessary shared ownership) but is not what drove
the benchmark improvements.

### 4 — Batched RwLock reads in `httpproxy.rs` (concurrency benefit, not visible in micro-bench)

**Before:** two separate `stores.read_binds()` acquisitions to fetch listener
policies and gateway policies.

**After:** both fetched under a single lock acquisition.

**Impact:** `bench_rwlock_read` shows a single uncontended read at ~3 ns.
Under concurrent load this reduces lock pressure, but the saving is invisible
in single-threaded micro-benchmarks.

### 5 — `.take()` instead of `.clone()` for `Option<Arc<…>>` policy fields

Several `Option<Arc<T>>` fields in `RoutePolicies` / `GatewayPolicies` were
`.clone()`d into `ResponsePolicies`. Since they are not used after the move,
`.take()` replaces the clone with a move (no atomic increment).

**Impact:** a few nanoseconds per field, not individually measurable.
