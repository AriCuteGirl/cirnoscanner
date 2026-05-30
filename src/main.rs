mod scanner;
mod wordlists;

use std::{convert::Infallible, net::SocketAddr, time::Duration};

use axum::{
    Json, Router,
    extract::Query,
    response::{
        IntoResponse,
        sse::{Event, KeepAlive, Sse},
    },
    routing::get,
};
use futures_util::Stream;
use scanner::{
    files::{FileScanOptions, FileScanUpdate},
    subdomain::{SubdomainScanOptions, SubdomainScanUpdate},
};
use serde::{Deserialize, Serialize};
use tower_http::{services::ServeDir, trace::TraceLayer};

#[derive(Serialize)]
struct Health {
    status: &'static str,
}

#[derive(Debug, Deserialize)]
struct SubdomainQuery {
    domain: String,
}

#[derive(Debug, Deserialize)]
struct FileQuery {
    url: String,
    presets: Option<String>,
    custom: Option<String>,
    everything: Option<bool>,
    brute: Option<bool>,
    crawl: Option<bool>,
    max_depth: Option<usize>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let app = Router::new()
        .route(
            "/api/health",
            get(|| async { Json(Health { status: "ok" }) }),
        )
        .route("/api/scan/subdomains", get(subdomain_sse))
        .route("/api/scan/files", get(files_sse))
        .fallback_service(ServeDir::new("static").append_index_html_on_directories(true))
        .layer(TraceLayer::new_for_http());

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    println!("Domain scanner listening on http://{addr}");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn subdomain_sse(
    Query(query): Query<SubdomainQuery>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    // Each SSE connection owns one scan, so closing the tab drops the stream
    // and lets in-flight workers finish without shared global state.
    let options = SubdomainScanOptions {
        domain: query.domain,
        concurrency: 64,
        timeout: Duration::from_secs(6),
    };

    Sse::new(update_stream(scanner::subdomain::scan(options))).keep_alive(KeepAlive::default())
}

async fn files_sse(Query(query): Query<FileQuery>) -> impl IntoResponse {
    // Query strings keep the frontend dependency-free while still allowing
    // scan settings to be replayed from the browser.
    let options = FileScanOptions {
        base_url: query.url,
        presets: split_csv(query.presets),
        custom_extensions: split_csv(query.custom),
        scan_everything: query.everything.unwrap_or(false),
        brute_force: query.brute.unwrap_or(true),
        crawl: query.crawl.unwrap_or(true),
        max_depth: query.max_depth.unwrap_or(2).clamp(0, 5),
        concurrency: 48,
        timeout: Duration::from_secs(8),
    };

    Sse::new(update_stream(scanner::files::scan(options))).keep_alive(KeepAlive::default())
}

fn update_stream<T>(
    stream: impl Stream<Item = T> + Send + 'static,
) -> impl Stream<Item = Result<Event, Infallible>>
where
    T: Serialize,
{
    async_stream::stream! {
        futures_util::pin_mut!(stream);
        while let Some(update) = futures_util::StreamExt::next(&mut stream).await {
            let event = match serde_json::to_string(&update) {
                Ok(payload) => Event::default().data(payload),
                Err(err) => Event::default().event("error").data(err.to_string()),
            };
            yield Ok(event);
        }
    }
}

fn split_csv(value: Option<String>) -> Vec<String> {
    value
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

#[allow(dead_code)]
fn _assert_updates_are_serializable(_: SubdomainScanUpdate, _: FileScanUpdate) {}
