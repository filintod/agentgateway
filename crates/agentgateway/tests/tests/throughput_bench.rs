use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use agentgateway::http::Body;
use http::Method;
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioTimer};
use wiremock::{Mock, ResponseTemplate};

use crate::common::gateway::AgentGateway;

fn get_rss_bytes() -> u64 {
	#[cfg(target_os = "macos")]
	{
		use std::mem;
		let mut info: libc::mach_task_basic_info = unsafe { mem::zeroed() };
		let mut count = (mem::size_of::<libc::mach_task_basic_info>()
			/ mem::size_of::<libc::natural_t>()) as u32;
		let ret = unsafe {
			libc::task_info(
				libc::mach_task_self(),
				libc::MACH_TASK_BASIC_INFO,
				&mut info as *mut _ as *mut i32,
				&mut count,
			)
		};
		if ret == libc::KERN_SUCCESS {
			info.resident_size as u64
		} else {
			0
		}
	}
	#[cfg(target_os = "linux")]
	{
		// Read from /proc/self/statm (pages)
		if let Ok(statm) = std::fs::read_to_string("/proc/self/statm") {
			let rss_pages: u64 = statm
				.split_whitespace()
				.nth(1)
				.and_then(|s| s.parse().ok())
				.unwrap_or(0);
			let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as u64;
			rss_pages * page_size
		} else {
			0
		}
	}
	#[cfg(not(any(target_os = "macos", target_os = "linux")))]
	{
		0
	}
}

fn build_config(backend_addr: &str, n_routes: usize) -> String {
	let mut lines = vec![
		"config: {}".to_string(),
		"binds:".to_string(),
		"- port: $PORT".to_string(),
		"  listeners:".to_string(),
		"  - name: default".to_string(),
		"    protocol: HTTP".to_string(),
		"    routes:".to_string(),
	];
	for i in 0..n_routes {
		lines.push(format!("    - name: svc-{i:04}"));
		lines.push(      "      matches:".to_string());
		lines.push(      "      - path:".to_string());
		lines.push(format!("          pathPrefix: /svc-{i:04}"));
		lines.push(      "        query:".to_string());
		lines.push(      "        - name: version".to_string());
		lines.push(      "          value:".to_string());
		lines.push(format!("            exact: v{i}"));
		lines.push(      "      backends:".to_string());
		lines.push(format!("        - host: {backend_addr}"));
	}
	// Default catch-all route at the end
	lines.push(    "    - name: default".to_string());
	lines.push(    "      backends:".to_string());
	lines.push(format!("        - host: {backend_addr}"));
	lines.join("\n")
}

/// `routes` is the list of (path, query) pairs to cycle through; empty = single path "/".
async fn run_bench(label: &str, config: String, routes: &[(&str, &str)], concurrency: usize, duration: Duration) -> anyhow::Result<()> {
	// Suppress per-request INFO logs so log I/O doesn't inflate latency measurements.
	// The "request" target is checked via telemetry::enabled() before each log emission,
	// so disabling it here skips the log entirely rather than just discarding after formatting.
	agent_core::telemetry::testing::setup_test_logging();
	let _ = agent_core::telemetry::set_level(false, "request=off");

	let gw = AgentGateway::new(config).await?;
	let port = gw.port();

	let client = Client::builder(TokioExecutor::new())
		.timer(TokioTimer::new())
		.pool_max_idle_per_host(256)
		.build_http::<Body>();

	// Build pre-computed URL list to cycle through (avoids format! on hot path)
	let urls: Vec<String> = if routes.is_empty() {
		vec![format!("http://127.0.0.1:{port}/")]
	} else {
		routes
			.iter()
			.map(|(path, query)| format!("http://127.0.0.1:{port}{path}?{query}"))
			.collect()
	};
	let urls = Arc::new(urls);

	// Warmup — cycle through all routes
	for i in 0..2000 {
		let req = http::Request::builder()
			.method(Method::GET)
			.uri(&urls[i % urls.len()])
			.body(Body::empty())
			.unwrap();
		let _ = client.request(req).await;
	}

	let rss_before = get_rss_bytes();
	let total_requests = Arc::new(AtomicU64::new(0));
	let total_errors = Arc::new(AtomicU64::new(0));
	let latencies = Arc::new(std::sync::Mutex::new(Vec::with_capacity(500_000)));
	let peak_rss = Arc::new(AtomicU64::new(rss_before));

	let peak_rss_sampler = peak_rss.clone();
	let rss_task = tokio::spawn(async move {
		loop {
			let current = get_rss_bytes();
			peak_rss_sampler.fetch_max(current, Ordering::Relaxed);
			tokio::time::sleep(Duration::from_millis(50)).await;
		}
	});

	let start = Instant::now();
	let mut handles = Vec::new();

	for task_id in 0..concurrency {
		let client = client.clone();
		let total_requests = total_requests.clone();
		let total_errors = total_errors.clone();
		let latencies = latencies.clone();
		let urls = urls.clone();

		handles.push(tokio::spawn(async move {
			let mut local_latencies = Vec::with_capacity(10_000);
			let mut idx = task_id; // each task starts at a different offset so they don't all hit the same route
			while start.elapsed() < duration {
				let req_start = Instant::now();
				let req = http::Request::builder()
					.method(Method::GET)
					.uri(&urls[idx % urls.len()])
					.body(Body::empty())
					.unwrap();
				idx = idx.wrapping_add(1);
				match client.request(req).await {
					Ok(resp) => {
						if !resp.status().is_success() {
							total_errors.fetch_add(1, Ordering::Relaxed);
						}
						let _ = http_body_util::BodyExt::collect(resp.into_body()).await;
					},
					Err(_) => {
						total_errors.fetch_add(1, Ordering::Relaxed);
					},
				}
				total_requests.fetch_add(1, Ordering::Relaxed);
				local_latencies.push(req_start.elapsed());
			}
			latencies.lock().unwrap().extend(local_latencies);
		}));
	}

	for h in handles {
		h.await?;
	}
	rss_task.abort();

	let elapsed = start.elapsed();
	let total = total_requests.load(Ordering::Relaxed);
	let errors = total_errors.load(Ordering::Relaxed);
	let rps = total as f64 / elapsed.as_secs_f64();
	let rss_peak = peak_rss.load(Ordering::Relaxed);

	let mut lats = latencies.lock().unwrap();
	lats.sort();
	let p50 = lats[lats.len() / 2];
	let p99 = lats[lats.len() * 99 / 100];
	let p999 = lats[lats.len() * 999 / 1000];
	let max = *lats.last().unwrap();

	eprintln!();
	eprintln!("=== {label} ===");
	eprintln!("Duration:     {elapsed:?}");
	eprintln!("Concurrency:  {concurrency}");
	eprintln!("Total reqs:   {total}  Errors: {errors}");
	eprintln!("Requests/sec: {rps:.0}");
	eprintln!("Latency P50:  {p50:?}  P99: {p99:?}  P999: {p999:?}  Max: {max:?}");
	eprintln!("RSS peak:     {:.2} MB", rss_peak as f64 / 1024.0 / 1024.0);
	eprintln!();

	gw.shutdown().await;
	Ok(())
}

/// Single route — baseline cost of the proxy with no route-matching overhead.
///
/// Run with:
///   cargo test -p agentgateway --test integration -- throughput_bench --nocapture --ignored
#[tokio::test]
#[ignore]
async fn throughput_bench_1_route() -> anyhow::Result<()> {
	let mock = wiremock::MockServer::start().await;
	Mock::given(wiremock::matchers::path_regex("/.*"))
		.respond_with(move |_: &wiremock::Request| ResponseTemplate::new(200).set_body_string("ok"))
		.mount(&mock)
		.await;
	let addr = mock.address().to_string();
	let cfg = build_config(&addr, 0); // 0 named routes → only the catch-all
	run_bench("1 route (catch-all only)", cfg, &[], 64, Duration::from_secs(10)).await
}

/// 100 routes with query constraints — traffic cycles through ALL 100 routes.
#[tokio::test]
#[ignore]
async fn throughput_bench_100_routes() -> anyhow::Result<()> {
	let mock = wiremock::MockServer::start().await;
	Mock::given(wiremock::matchers::path_regex("/.*"))
		.respond_with(move |_: &wiremock::Request| ResponseTemplate::new(200).set_body_string("ok"))
		.mount(&mock)
		.await;
	let addr = mock.address().to_string();
	let n = 100usize;
	let cfg = build_config(&addr, n);
	let route_pairs: Vec<(String, String)> = (0..n)
		.map(|i| (format!("/svc-{i:04}"), format!("version=v{i}")))
		.collect();
	let route_refs: Vec<(&str, &str)> = route_pairs.iter().map(|(p, q)| (p.as_str(), q.as_str())).collect();
	run_bench("100 routes (all hit, cycling)", cfg, &route_refs, 64, Duration::from_secs(10)).await
}

/// 1000 routes with query constraints — traffic cycles through ALL 1000 routes.
/// This exercises the full route-scan path for every request variant.
#[tokio::test]
#[ignore]
async fn throughput_bench_1000_routes() -> anyhow::Result<()> {
	let mock = wiremock::MockServer::start().await;
	Mock::given(wiremock::matchers::path_regex("/.*"))
		.respond_with(move |_: &wiremock::Request| ResponseTemplate::new(200).set_body_string("ok"))
		.mount(&mock)
		.await;
	let addr = mock.address().to_string();
	let n = 1000usize;
	let cfg = build_config(&addr, n);
	let route_pairs: Vec<(String, String)> = (0..n)
		.map(|i| (format!("/svc-{i:04}"), format!("version=v{i}")))
		.collect();
	let route_refs: Vec<(&str, &str)> = route_pairs.iter().map(|(p, q)| (p.as_str(), q.as_str())).collect();
	run_bench("1000 routes (all hit, cycling)", cfg, &route_refs, 64, Duration::from_secs(10)).await
}
