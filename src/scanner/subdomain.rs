use std::{
    net::IpAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use async_stream::stream;
use futures_util::{Stream, StreamExt, stream::FuturesUnordered};
use reqwest::Client;
use serde::Serialize;
use tokio::{sync::Semaphore, time::timeout};
use trust_dns_resolver::{
    TokioAsyncResolver,
    config::{ResolverConfig, ResolverOpts},
};

use crate::wordlists::SUBDOMAINS;

#[derive(Clone, Debug)]
pub struct SubdomainScanOptions {
    pub domain: String,
    pub concurrency: usize,
    pub timeout: Duration,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum SubdomainScanUpdate {
    Started {
        total: usize,
    },
    Result {
        result: SubdomainResult,
    },
    Progress {
        progress: ScanProgress,
    },
    Finished {
        checked: usize,
        found: usize,
        elapsed_ms: u128,
    },
    Error {
        message: String,
    },
}

#[derive(Clone, Debug, Serialize)]
pub struct SubdomainResult {
    pub subdomain: String,
    pub ip: String,
    pub status: Option<u16>,
    pub response_time_ms: Option<u128>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ScanProgress {
    pub checked: usize,
    pub found: usize,
    pub total: usize,
    pub elapsed_ms: u128,
    pub rate: f64,
}

pub fn scan(
    options: SubdomainScanOptions,
) -> impl Stream<Item = SubdomainScanUpdate> + Send + 'static {
    stream! {
        let domain = normalize_domain(&options.domain);
        if domain.is_empty() || !domain.contains('.') {
            yield SubdomainScanUpdate::Error { message: "Enter a valid root domain, for example example.com.".to_owned() };
            return;
        }

        let resolver = TokioAsyncResolver::tokio(ResolverConfig::default(), ResolverOpts::default());
        let client = match Client::builder()
            .user_agent("catppuccin-domain-scanner/1.0")
            .redirect(reqwest::redirect::Policy::limited(5))
            .timeout(options.timeout)
            .build()
        {
            Ok(client) => client,
            Err(err) => {
                yield SubdomainScanUpdate::Error { message: format!("Failed to create HTTP client: {err}") };
                return;
            }
        };

        let started = Instant::now();
        let total = SUBDOMAINS.len();
        let checked = Arc::new(AtomicUsize::new(0));
        let found = Arc::new(AtomicUsize::new(0));
        let semaphore = Arc::new(Semaphore::new(options.concurrency.max(1)));
        let mut tasks = FuturesUnordered::new();

        yield SubdomainScanUpdate::Started { total };

        // The semaphore keeps the built-in wordlist from overwhelming the local
        // resolver or the target's HTTP edge while preserving async throughput.
        for label in SUBDOMAINS {
            let resolver = resolver.clone();
            let client = client.clone();
            let semaphore = semaphore.clone();
            let checked = checked.clone();
            let found = found.clone();
            let host = format!("{label}.{domain}");
            let request_timeout = options.timeout;

            tasks.push(tokio::spawn(async move {
                let _permit = semaphore.acquire_owned().await.ok()?;
                let ips = resolve_ips(&resolver, &host, request_timeout).await;
                let result = if let Some(ip) = ips.first().copied() {
                    let probe = probe_http(&client, &host).await;
                    found.fetch_add(1, Ordering::Relaxed);
                    Some(SubdomainResult {
                        subdomain: host,
                        ip: ip.to_string(),
                        status: probe.map(|item| item.0),
                        response_time_ms: probe.map(|item| item.1),
                    })
                } else {
                    None
                };
                checked.fetch_add(1, Ordering::Relaxed);
                result
            }));
        }

        while let Some(joined) = tasks.next().await {
            match joined {
                Ok(Some(result)) => yield SubdomainScanUpdate::Result { result },
                Ok(None) => {}
                Err(err) => yield SubdomainScanUpdate::Error { message: format!("Worker failed: {err}") },
            }

            yield SubdomainScanUpdate::Progress { progress: progress(
                checked.load(Ordering::Relaxed),
                found.load(Ordering::Relaxed),
                total,
                started,
            ) };
        }

        yield SubdomainScanUpdate::Finished {
            checked: checked.load(Ordering::Relaxed),
            found: found.load(Ordering::Relaxed),
            elapsed_ms: started.elapsed().as_millis(),
        };
    }
}

async fn resolve_ips(
    resolver: &TokioAsyncResolver,
    host: &str,
    request_timeout: Duration,
) -> Vec<IpAddr> {
    match timeout(request_timeout, resolver.lookup_ip(host)).await {
        Ok(Ok(lookup)) => lookup.iter().collect(),
        _ => Vec::new(),
    }
}

async fn probe_http(client: &Client, host: &str) -> Option<(u16, u128)> {
    // Prefer HTTPS, then fall back to HTTP for legacy hosts.
    for scheme in ["https", "http"] {
        let url = format!("{scheme}://{host}/");
        let started = Instant::now();
        if let Ok(response) = client.get(&url).send().await {
            return Some((response.status().as_u16(), started.elapsed().as_millis()));
        }
    }
    None
}

fn progress(checked: usize, found: usize, total: usize, started: Instant) -> ScanProgress {
    let elapsed = started.elapsed();
    let seconds = elapsed.as_secs_f64().max(0.001);
    ScanProgress {
        checked,
        found,
        total,
        elapsed_ms: elapsed.as_millis(),
        rate: checked as f64 / seconds,
    }
}

fn normalize_domain(domain: &str) -> String {
    domain
        .trim()
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .trim_end_matches('/')
        .to_ascii_lowercase()
}
