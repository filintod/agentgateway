use std::collections::HashMap;

use ::http::Method;
use agent_core::strng::Strng;
use http_body_util::BodyExt as _;
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::*;
use crate::test_helpers::proxymock::*;
use crate::types::agent::{
	BackendPolicy, BackendReference, BackendTarget, Listener, ListenerProtocol, ListenerSet,
	PathMatch, PolicyTarget, PolicyType, Route, RouteBackendReference, RouteMatch, RouteName,
	RouteSet, SimpleBackendReference, Target, TargetedPolicy,
};
use crate::types::discovery::{
	Endpoint, HealthStatus, NamespacedHostname, NetworkAddress, NetworkMode, Service, Workload,
};

fn activation_service(svc_name: &str, svc_namespace: &str, backend_port: u16) -> Service {
	let svc_hostname = format!("{svc_name}.{svc_namespace}.svc.cluster.local");
	Service {
		name: Strng::from(svc_name),
		namespace: Strng::from(svc_namespace),
		hostname: Strng::from(svc_hostname),
		vips: vec![NetworkAddress {
			network: Strng::new(),
			address: [10, 0, 0, 1].into(),
		}],
		ports: HashMap::from([(8080, backend_port)]),
		..Default::default()
	}
}

fn activation_route(svc_namespace: &str, svc_hostname: &str) -> Route {
	let svc_ref = NamespacedHostname {
		namespace: Strng::from(svc_namespace),
		hostname: Strng::from(svc_hostname),
	};
	Route {
		key: "route".into(),
		name: RouteName {
			name: "route".into(),
			namespace: Default::default(),
			rule_name: None,
			kind: None,
		},
		hostnames: Default::default(),
		matches: vec![RouteMatch {
			headers: vec![],
			path: PathMatch::PathPrefix("/".into()),
			method: None,
			query: vec![],
		}],
		inline_policies: Default::default(),
		backends: vec![RouteBackendReference {
			weight: 1,
			backend: BackendReference::Service {
				name: svc_ref,
				port: 8080,
			},
			inline_policies: Default::default(),
		}],
	}
}

fn activation_bind(route: Route) -> crate::types::agent::Bind {
	crate::types::agent::Bind {
		key: BIND_KEY,
		address: "127.0.0.1:0".parse().unwrap(),
		listeners: ListenerSet::from_list([Listener {
			key: LISTENER_KEY,
			name: Default::default(),
			hostname: Default::default(),
			protocol: ListenerProtocol::HTTP,
			tcp_routes: Default::default(),
			routes: RouteSet::from_list(vec![route]),
		}]),
		protocol: crate::types::agent::BindProtocol::http,
		tunnel_protocol: Default::default(),
	}
}

fn activation_targeted_policy(
	svc_hostname: &str,
	svc_namespace: &str,
	scaler_addr: std::net::SocketAddr,
	ready_timeout: std::time::Duration,
) -> TargetedPolicy {
	TargetedPolicy {
		key: "activation".into(),
		name: None,
		target: PolicyTarget::Backend(BackendTarget::Service {
			hostname: Strng::from(svc_hostname),
			namespace: Strng::from(svc_namespace),
			port: Some(8080),
		}),
		policy: PolicyType::Backend(BackendPolicy::Activation(
			crate::types::agent::ActivationPolicy {
				backend: SimpleBackendReference::InlineBackend(Target::Address(scaler_addr)),
				ready_timeout,
			},
		)),
	}
}

/// Test that activate_and_wait calls the scaler and succeeds once endpoints appear.
///
/// Flow:
///   1. Request arrives at proxy for a Service backend with 0 endpoints
///   2. build_service_call returns NoHealthyEndpoints
///   3. activate_and_wait calls the scaler POST /scale-up
///   4. Background task adds an endpoint + workload to the store
///   5. Poll loop in activate_and_wait detects the new endpoint
///   6. Request is forwarded to the backend and returns 200
#[tokio::test]
async fn activation_scales_up_and_forwards_request() {
	let scaler = MockServer::start().await;
	let backend = MockServer::start().await;

	// Scaler responds 200 to POST /scale-up
	Mock::given(method("POST"))
		.and(path("/scale-up"))
		.respond_with(ResponseTemplate::new(200))
		.expect(1)
		.mount(&scaler)
		.await;

	// Backend responds 200
	Mock::given(method("GET"))
		.respond_with(ResponseTemplate::new(200).set_body_string("activated"))
		.mount(&backend)
		.await;

	let backend_port = backend.address().port();
	let svc_name = "test-svc";
	let svc_namespace = "test-ns";
	let svc_hostname = format!("{svc_name}.{svc_namespace}.svc.cluster.local");

	// Create service with ports but no endpoints
	let service = activation_service(svc_name, svc_namespace, backend_port);

	// Build proxy and insert service into its discovery store
	let tb = setup_proxy_test("{}").unwrap();
	let pi = tb.inputs();
	pi.stores
		.discovery
		.sync_local(vec![service], vec![], Default::default())
		.unwrap();

	// Get the Arc<Service> from the store - this is the same Arc the proxy will use
	let svc_arc = pi
		.stores
		.read_discovery()
		.services
		.get_by_vip(&NetworkAddress {
			network: Strng::new(),
			address: [10, 0, 0, 1].into(),
		})
		.expect("service should be in store");

	// Attach activation policy via TargetedPolicy
	let mut tb = tb.with_bind(activation_bind(activation_route(
		svc_namespace,
		&svc_hostname,
	)));

	tb.with_policy(activation_targeted_policy(
		&svc_hostname,
		svc_namespace,
		*scaler.address(),
		std::time::Duration::from_secs(5),
	));

	// Background task: after a short delay, add endpoint + workload
	let svc_for_bg = svc_arc.clone();
	let stores_for_bg = pi.stores.clone();
	let svc_hostname_bg = svc_hostname.clone();
	tokio::spawn(async move {
		// Wait for the scaler to be called
		tokio::time::sleep(std::time::Duration::from_millis(200)).await;

		// Insert endpoint into the service's EndpointSet (atomic, &self)
		svc_for_bg.endpoints.insert(Endpoint {
			workload_uid: Strng::from("test-workload"),
			port: HashMap::from([(8080, backend_port)]),
			status: HealthStatus::Healthy,
		});

		// Insert matching workload into the discovery store via sync_local
		let workload = Workload {
			uid: Strng::from("test-workload"),
			name: Strng::from(svc_name),
			namespace: Strng::from(svc_namespace),
			service_account: Strng::from("default"),
			network_mode: NetworkMode::Standard,
			workload_ips: vec![[127, 0, 0, 1].into()],
			services: vec![NamespacedHostname {
				hostname: Strng::from(svc_hostname_bg.as_str()),
				namespace: Strng::from(svc_namespace),
			}],
			..Default::default()
		};
		let local_workload = crate::store::LocalWorkload {
			workload,
			services: HashMap::from([(svc_hostname_bg, HashMap::from([(8080, backend_port)]))]),
		};
		stores_for_bg
			.discovery
			.sync_local(vec![], vec![local_workload], Default::default())
			.unwrap();
	});

	let io = tb.serve_http(BIND_KEY);
	let res = send_request(io, Method::GET, "http://lo").await;

	assert_eq!(res.status(), 200);
}

#[tokio::test]
async fn access_log_uses_final_transformed_response_body() {
	let mut bind = setup_proxy_test(
		r#"
config:
  logging:
    fields:
      add:
        body: string(response.body)
"#,
	)
	.unwrap()
	.with_bind(simple_bind(basic_route("127.0.0.1:1".parse().unwrap())));
	bind
		.attach_route_policy(json!({
			"directResponse": {
				"body": "before",
				"status": 200,
			},
			"transformations": {
				"response": {
					"body": "'after'",
				},
			},
		}))
		.await;

	let io = bind.serve_http(BIND_KEY);
	let r = rand::rng().random::<u128>();
	let path = format!("/access-log-final-body-{r}");
	let res = send_request(io, Method::GET, &format!("http://lo{path}")).await;
	assert_eq!(res.status(), 200);
	assert_eq!(
		res.into_body().collect().await.unwrap().to_bytes().as_ref(),
		b"after"
	);

	let log =
		agent_core::telemetry::testing::eventually_find(&[("scope", "request"), ("http.path", &path)])
			.await
			.unwrap();
	assert_eq!(log["body"].as_str(), Some("after"));
}
