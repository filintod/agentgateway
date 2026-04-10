use std::borrow::Cow;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use agent_core::strng;

use crate::http::Request;
use crate::types::agent;
use crate::types::agent::{
	BackendReference, HeaderMatch, HeaderValueMatch, Listener, PathMatch, QueryValueMatch, Route,
	RouteBackendReference, RouteMatch, RouteName,
};
use crate::*;

#[cfg(any(test, feature = "internal_benches"))]
#[path = "route_test.rs"]
mod tests;

/// Lazily-parsed query string. Parses at most once per request, only when a
/// route actually has query matchers.
enum ParsedQuery<'a> {
	/// No query string in the URI — matches nothing.
	None,
	/// Has a raw query string but hasn't been parsed yet.
	Unparsed(&'a str),
	/// Already parsed into a map.
	Parsed(HashMap<Cow<'a, str>, Cow<'a, str>>),
}

impl<'a> ParsedQuery<'a> {
	fn from_request(request: &'a Request) -> Self {
		match request.uri().query() {
			Some(q) => ParsedQuery::Unparsed(q),
			None => ParsedQuery::None,
		}
	}

	fn get(&mut self, key: &str) -> Option<&Cow<'a, str>> {
		match self {
			ParsedQuery::None => None,
			ParsedQuery::Unparsed(raw) => {
				let map: HashMap<Cow<'a, str>, Cow<'a, str>> =
					url::form_urlencoded::parse(raw.as_bytes()).collect();
				*self = ParsedQuery::Parsed(map);
				match self {
					ParsedQuery::Parsed(m) => m.get(key),
					_ => unreachable!(),
				}
			},
			ParsedQuery::Parsed(map) => map.get(key),
		}
	}
}

/// Per-request state precomputed once before the route-scan loop.
/// Passed to `matches_request` so each candidate doesn't re-derive the same values.
struct RequestMatchCtx<'a> {
	/// `request.uri().path()`
	path: &'a str,
	/// `path.trim_end_matches('/')` — used by every PathPrefix check
	path_trimmed: &'a str,
	/// `request.method().as_str()` — used when a route has a method constraint
	method: &'a str,
	/// Lazily-parsed query string, shared across all candidates
	parsed_query: ParsedQuery<'a>,
}

impl<'a> RequestMatchCtx<'a> {
	fn from_request(request: &'a Request) -> Self {
		let path = request.uri().path();
		Self {
			path,
			path_trimmed: path.trim_end_matches('/'),
			method: request.method().as_str(),
			parsed_query: ParsedQuery::from_request(request),
		}
	}
}

/// Check if a RouteMatch matches the given request (path, method, headers, query).
/// `ctx` holds per-request values precomputed once before the route-scan loop.
fn matches_request(m: &RouteMatch, ctx: &mut RequestMatchCtx<'_>, request: &Request) -> bool {
	let path_matches = match &m.path {
		PathMatch::Exact(p) => ctx.path == p.as_str(),
		PathMatch::Regex(r) => r
			.find(ctx.path)
			.map(|m| m.start() == 0 && m.end() == ctx.path.len())
			.unwrap_or(false),
		PathMatch::PathPrefix(p) => {
			let p = p.trim_end_matches('/');
			let Some(suffix) = ctx.path_trimmed.strip_prefix(p) else {
				return false;
			};
			// TODO this is not right!!
			suffix.is_empty() || suffix.starts_with('/')
		},
	};
	if !path_matches {
		return false;
	}

	if let Some(method) = &m.method
		&& ctx.method != method.method.as_str()
	{
		return false;
	}
	for HeaderMatch { name, value } in &m.headers {
		let Some(have) = http::get_pseudo_or_header_value(name, request) else {
			return false;
		};
		match value {
			HeaderValueMatch::Exact(want) => {
				if have.as_ref() != *want {
					return false;
				}
			},
			HeaderValueMatch::Regex(want) => {
				let Some(have_str) = have.to_str().ok() else {
					return false;
				};
				let Some(m) = want.find(have_str) else {
					return false;
				};
				if !(m.start() == 0 && m.end() == have_str.len()) {
					return false;
				}
			},
		}
	}
	for agent::QueryMatch { name, value } in &m.query {
		let Some(have) = ctx.parsed_query.get(name.as_str()) else {
			return false;
		};
		match value {
			QueryValueMatch::Exact(want) => {
				if have.as_ref() != want.as_str() {
					return false;
				}
			},
			QueryValueMatch::Regex(want) => {
				let Some(m) = want.find(have) else {
					return false;
				};
				if !(m.start() == 0 && m.end() == have.len()) {
					return false;
				}
			},
		}
	}
	true
}

pub fn select_best_route(
	stores: &Stores,
	dst: SocketAddr,
	listener: &Listener,
	request: &Request,
) -> Option<(Arc<Route>, PathMatch)> {
	// Order:
	// * "Exact" path match.
	// * "Prefix" path match with largest number of characters.
	// * Method match.
	// * Largest number of header matches.
	// * Largest number of query param matches.
	//
	// If ties still exist across multiple Routes, matching precedence MUST be
	// determined in order of the following criteria, continuing on ties:
	//
	//  * The oldest Route based on creation timestamp.
	//  * The Route appearing first in alphabetical order by "{namespace}/{name}".
	//
	// If ties still exist within an HTTPRoute, matching precedence MUST be granted
	// to the FIRST matching rule (in list order) with a match meeting the above
	// criteria.

	let host = http::get_host(request).ok()?;
	// Precompute per-request values once; shared across all route-match candidates.
	let mut ctx = RequestMatchCtx::from_request(request);

	let (default_response, host) =
		if let Some(wps) = request.extensions().get::<crate::proxy::WaypointService>() {
			// When routes are attached to a Service via parentRef, they take priority
			// over listener-attached routes. If service routes exist but none match,
			// the request is rejected (per GAMMA spec).
			let svc = wps.as_ref();
			let svc_nh = svc.namespaced_hostname();
			let (has_svc_routes, svc_route_match) = {
				let binds = stores.read_binds();
				match binds.get_service_routes(&svc_nh) {
					Some(svc_routes) => {
						let mut result = None;
						for hnm in agent::HostnameMatch::all_matches(&svc.hostname) {
							result = svc_routes
								.get_hostname(&hnm)
								.find(|(_, m)| matches_request(m, &mut ctx, request))
								.map(|(route, matcher)| (route.clone(), matcher.path.clone()));
							if result.is_some() {
								break;
							}
						}
						(true, result)
					},
					None => (false, None),
				}
			};
			if let Some(result) = svc_route_match {
				return Some(result);
			}
			if has_svc_routes {
				// GAMMA: service routes exist but none matched -> reject
				return None;
			}

			// No service-keyed routes: fall through to hostname matching with default route
			let default_route = Route {
				key: strng::literal!("_waypoint-default"),
				service_key: Some(svc.namespaced_hostname()),
				name: RouteName {
					name: strng::literal!("_waypoint-default"),
					namespace: svc.namespace.clone(),
					rule_name: None,
					kind: None,
				},
				hostnames: vec![],
				matches: vec![],
				inline_policies: vec![],
				backends: vec![RouteBackendReference {
					weight: 1,
					backend: BackendReference::Service {
						name: svc.namespaced_hostname(),
						port: dst.port(), // TODO: get from req
					},
					inline_policies: Vec::new(),
				}],
			};
			let def = Some((
				Arc::new(default_route),
				PathMatch::PathPrefix(strng::new("/")),
			));
			(def, Cow::Owned(svc.hostname.to_string()))
		} else {
			(None, Cow::Borrowed(host))
		};
	for hnm in agent::HostnameMatch::all_matches(&host) {
		let mut candidates = listener.routes.get_hostname(&hnm);
		let best_match = candidates.find(|(_, m)| matches_request(m, &mut ctx, request));
		if let Some((route, matcher)) = best_match {
			return Some((route.clone(), matcher.path.clone()));
		}
	}
	default_response
}
