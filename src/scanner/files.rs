use std::collections::hash_map::DefaultHasher;
use std::{
    collections::{HashSet, VecDeque},
    hash::{Hash, Hasher},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_stream::stream;
use futures_util::{Stream, StreamExt, stream::FuturesUnordered};
use reqwest::{Client, header};
use scraper::{Html, Selector};
use serde::Serialize;
use tokio::sync::Semaphore;
use url::Url;

use crate::wordlists::{FILE_PATHS, FILE_PRESETS, MEDIA_DIRECTORIES};

#[derive(Clone, Debug)]
pub struct FileScanOptions {
    pub base_url: String,
    pub presets: Vec<String>,
    pub custom_extensions: Vec<String>,
    pub scan_everything: bool,
    pub brute_force: bool,
    pub crawl: bool,
    pub max_depth: usize,
    pub concurrency: usize,
    pub timeout: Duration,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum FileScanUpdate {
    Started {
        total: usize,
    },
    Result {
        result: FileResult,
    },
    Progress {
        progress: FileProgress,
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
pub struct FileResult {
    pub url: String,
    pub status: u16,
    pub content_type: String,
    pub size: Option<u64>,
    pub extension: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct FileProgress {
    pub checked: usize,
    pub found: usize,
    pub total: usize,
    pub elapsed_ms: u128,
    pub rate: f64,
}

pub fn scan(options: FileScanOptions) -> impl Stream<Item = FileScanUpdate> + Send + 'static {
    stream! {
        let base = match normalize_base_url(&options.base_url) {
            Ok(url) => url,
            Err(message) => {
                yield FileScanUpdate::Error { message };
                return;
            }
        };

        let extensions = selected_extensions(&options);
        let client = match Client::builder()
            .user_agent("catppuccin-file-scanner/1.0")
            .redirect(reqwest::redirect::Policy::limited(8))
            .timeout(options.timeout)
            .build()
        {
            Ok(client) => client,
            Err(err) => {
                yield FileScanUpdate::Error { message: format!("Failed to create HTTP client: {err}") };
                return;
            }
        };

        let started = Instant::now();
        let mut planned = Vec::new();
        if options.brute_force {
            planned.extend(brute_force_urls(&base, &extensions, options.scan_everything));
        }
        let soft_404 = Arc::new(build_soft_404_profile(&client, &base).await);

        let total_hint = planned.len();
        yield FileScanUpdate::Started { total: total_hint };

        let checked = Arc::new(AtomicUsize::new(0));
        let found = Arc::new(AtomicUsize::new(0));
        let mut seen = HashSet::new();
        let mut inferred_directories = HashSet::new();

        if options.crawl {
            // Crawling stays on the original origin and only descends into HTML;
            // file-like links are queued for metadata checks.
            let mut queue = VecDeque::from([(base.clone(), 0usize)]);
            let link_selector = Selector::parse("a[href], link[href], script[src], img[src], source[src], video[src], audio[src], iframe[src]").expect("static selector is valid");

            while let Some((page_url, depth)) = queue.pop_front() {
                if !seen.insert(page_url.as_str().to_owned()) {
                    continue;
                }

                match client.get(page_url.clone()).send().await {
                    Ok(response) => {
                        let status = response.status();
                        let content_type = response
                            .headers()
                            .get(header::CONTENT_TYPE)
                            .and_then(|value| value.to_str().ok())
                            .unwrap_or("")
                            .to_owned();
                        let size = response.content_length();
                        checked.fetch_add(1, Ordering::Relaxed);

                        if should_emit_file(&page_url, &extensions, options.scan_everything, &content_type) {
                            found.fetch_add(1, Ordering::Relaxed);
                            yield FileScanUpdate::Result { result: file_result(page_url.clone(), status.as_u16(), content_type.clone(), size) };
                        }

                        if depth <= options.max_depth
                            && status.is_success()
                            && should_parse_for_links(&page_url, &content_type)
                        {
                            if let Ok(body) = response.text().await {
                                let mut links = extract_links(&page_url, &body, &link_selector);
                                links.extend(extract_text_references(&page_url, &body));
                                links.extend(extract_manifest_media(&base, &page_url, &body));

                                for link in links {
                                    if same_origin(&base, &link) && !seen.contains(link.as_str()) {
                                        if is_api_endpoint(&link)
                                            && inferred_directories.insert(link.as_str().to_owned())
                                        {
                                            found.fetch_add(1, Ordering::Relaxed);
                                            yield FileScanUpdate::Result {
                                                result: inferred_api_result(link.clone()),
                                            };
                                        }

                                        if let Some(directory) = parent_directory_url(&link) {
                                            if is_media_url(&link)
                                                && directory != base
                                                && inferred_directories.insert(directory.as_str().to_owned())
                                            {
                                                found.fetch_add(1, Ordering::Relaxed);
                                                yield FileScanUpdate::Result {
                                                    result: inferred_directory_result(directory),
                                                };
                                            }
                                        }

                                        if should_crawl_url(&link) && depth + 1 <= options.max_depth {
                                            queue.push_back((link, depth + 1));
                                        } else if should_emit_file(
                                            &link,
                                            &extensions,
                                            options.scan_everything,
                                            "",
                                        ) {
                                            planned.push(link.clone());
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(err) => {
                        checked.fetch_add(1, Ordering::Relaxed);
                        yield FileScanUpdate::Error { message: format!("Failed to fetch {page_url}: {err}") };
                    }
                }

                yield FileScanUpdate::Progress { progress: progress(
                    checked.load(Ordering::Relaxed),
                    found.load(Ordering::Relaxed),
                    total_hint.max(checked.load(Ordering::Relaxed)),
                    started,
                ) };
            }
        }

        let mut unique_candidates = Vec::new();
        for url in planned {
            if seen.insert(url.as_str().to_owned()) {
                unique_candidates.push(url);
            }
        }

        let semaphore = Arc::new(Semaphore::new(options.concurrency.max(1)));
        let mut tasks = FuturesUnordered::new();
        for url in unique_candidates {
            let client = client.clone();
            let semaphore = semaphore.clone();
            let checked = checked.clone();
            let found = found.clone();
            let extensions = extensions.clone();
            let soft_404 = soft_404.clone();
            let scan_everything = options.scan_everything;

            tasks.push(tokio::spawn(async move {
                let _permit = semaphore.acquire_owned().await.ok()?;
                let result =
                    check_candidate(&client, url, &extensions, &soft_404, scan_everything).await;
                checked.fetch_add(1, Ordering::Relaxed);
                if result.is_some() {
                    found.fetch_add(1, Ordering::Relaxed);
                }
                result
            }));
        }

        while let Some(joined) = tasks.next().await {
            match joined {
                Ok(Some(result)) => yield FileScanUpdate::Result { result },
                Ok(None) => {}
                Err(err) => yield FileScanUpdate::Error { message: format!("Worker failed: {err}") },
            }

            yield FileScanUpdate::Progress { progress: progress(
                checked.load(Ordering::Relaxed),
                found.load(Ordering::Relaxed),
                total_hint.max(checked.load(Ordering::Relaxed)),
                started,
            ) };
        }

        yield FileScanUpdate::Finished {
            checked: checked.load(Ordering::Relaxed),
            found: found.load(Ordering::Relaxed),
            elapsed_ms: started.elapsed().as_millis(),
        };
    }
}

#[derive(Clone, Debug, Default)]
struct Soft404Profile {
    samples: Vec<Soft404Sample>,
}

#[derive(Clone, Debug)]
struct Soft404Sample {
    status: u16,
    content_type_family: String,
    body_len: usize,
    body_signature: u64,
}

impl Soft404Profile {
    fn matches(
        &self,
        status: u16,
        content_type: &str,
        body_len: usize,
        body_signature: u64,
    ) -> bool {
        let family = content_type_family(content_type);
        self.samples.iter().any(|sample| {
            sample.status == status
                && sample.content_type_family == family
                && sample.body_signature == body_signature
                && sample.body_len.abs_diff(body_len) <= 32
        })
    }
}

async fn build_soft_404_profile(client: &Client, base: &Url) -> Soft404Profile {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let probes = [
        format!(".mocha-scanner-missing-{nonce}.txt"),
        format!("mocha-scanner/missing-{nonce}.json"),
    ];
    let mut samples = Vec::new();

    for probe in probes {
        let Ok(url) = base.join(&probe) else {
            continue;
        };
        let Ok(response) = client.get(url).send().await else {
            continue;
        };
        let status = response.status().as_u16();
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .to_owned();
        let Ok(bytes) = response.bytes().await else {
            continue;
        };

        samples.push(Soft404Sample {
            status,
            content_type_family: content_type_family(&content_type),
            body_len: bytes.len(),
            body_signature: body_signature(&bytes),
        });
    }

    Soft404Profile { samples }
}

async fn check_candidate(
    client: &Client,
    url: Url,
    extensions: &[String],
    soft_404: &Soft404Profile,
    scan_everything: bool,
) -> Option<FileResult> {
    let response = client
        .get(url.clone())
        .header(header::RANGE, "bytes=0-16383")
        .send()
        .await
        .ok()?;
    let status = response.status();
    let response_status = status.as_u16();
    if response_status == 404 || response_status == 416 || status.is_server_error() {
        return None;
    }

    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_owned();
    let content_range = response
        .headers()
        .get(header::CONTENT_RANGE)
        .and_then(|value| value.to_str().ok())
        .and_then(parse_content_range_total);
    let header_size = response.content_length();
    let bytes = response.bytes().await.ok()?;
    let size = content_range.or(header_size).or(Some(bytes.len() as u64));
    let body_signature = body_signature(&bytes);

    if soft_404.matches(response_status, &content_type, bytes.len(), body_signature) {
        return None;
    }

    if !should_emit_file(&url, extensions, scan_everything, &content_type) {
        return None;
    }
    if !content_type_matches_url(&url, &content_type, scan_everything) {
        return None;
    }

    let display_status = if response_status == 206 {
        200
    } else {
        response_status
    };
    Some(file_result(url, display_status, content_type, size))
}

fn brute_force_urls(base: &Url, extensions: &[String], scan_everything: bool) -> Vec<Url> {
    let mut urls = Vec::new();
    // Paths containing {ext} are expanded against selected filters, or against
    // every built-in extension when "Everything" is enabled.
    let extensions: Vec<String> = if scan_everything || extensions.is_empty() {
        FILE_PRESETS
            .iter()
            .flat_map(|preset| preset.extensions.iter())
            .map(|ext| ext.to_string())
            .collect()
    } else {
        extensions.to_vec()
    };

    for path in FILE_PATHS {
        if path.contains("{ext}") {
            for extension in &extensions {
                let clean_ext = extension.trim_start_matches('.');
                if let Ok(url) = base.join(&path.replace("{ext}", clean_ext)) {
                    urls.push(url);
                }
            }
        } else if let Ok(url) = base.join(path) {
            urls.push(url);
        }
    }
    urls
}

fn extract_links(base: &Url, body: &str, selector: &Selector) -> Vec<Url> {
    let document = Html::parse_document(body);
    document
        .select(selector)
        .filter_map(|node| {
            node.value()
                .attr("href")
                .or_else(|| node.value().attr("src"))
        })
        .filter_map(|href| base.join(href).ok())
        .collect()
}

fn extract_text_references(base: &Url, body: &str) -> Vec<Url> {
    body.split(|ch: char| {
        ch.is_whitespace()
            || matches!(
                ch,
                '"' | '\'' | '`' | '<' | '>' | '(' | ')' | '[' | ']' | '{' | '}' | ',' | ';'
            )
    })
    .filter_map(|token| {
        let cleaned = token
            .trim()
            .trim_matches('\\')
            .trim_matches('.')
            .trim_matches(':');
        if !looks_like_reference(cleaned) {
            return None;
        }
        base.join(cleaned).ok()
    })
    .collect()
}

fn extract_manifest_media(origin: &Url, current: &Url, body: &str) -> Vec<Url> {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(body) else {
        return Vec::new();
    };

    let mut filenames = Vec::new();
    collect_media_filenames(&value, &mut filenames);

    let mut urls = Vec::new();
    for filename in filenames {
        if let Ok(url) = current.join(&filename) {
            urls.push(url);
        }
        for companion in companion_media_filenames(&filename) {
            if let Ok(url) = current.join(&companion) {
                urls.push(url);
            }
        }
        if is_bare_filename(&filename) {
            for directory in MEDIA_DIRECTORIES {
                if let Ok(url) = origin.join(&format!("{directory}/{filename}")) {
                    urls.push(url);
                }
                for companion in companion_media_filenames(&filename) {
                    if let Ok(url) = origin.join(&format!("{directory}/{companion}")) {
                        urls.push(url);
                    }
                }
            }
        }
    }
    urls
}

fn collect_media_filenames(value: &serde_json::Value, output: &mut Vec<String>) {
    match value {
        serde_json::Value::String(value) => {
            if url_extension_from_path(value)
                .map(|extension| is_media_extension(&extension))
                .unwrap_or(false)
            {
                output.push(value.to_owned());
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                collect_media_filenames(item, output);
            }
        }
        serde_json::Value::Object(map) => {
            for item in map.values() {
                collect_media_filenames(item, output);
            }
        }
        _ => {}
    }
}

fn selected_extensions(options: &FileScanOptions) -> Vec<String> {
    let mut extensions = HashSet::new();
    for preset in &options.presets {
        if let Some(found) = FILE_PRESETS
            .iter()
            .find(|item| item.key.eq_ignore_ascii_case(preset))
        {
            for extension in found.extensions {
                extensions.insert(normalize_extension(extension));
            }
        }
    }
    for extension in &options.custom_extensions {
        extensions.insert(normalize_extension(extension));
    }
    extensions.into_iter().collect()
}

fn should_emit_file(
    url: &Url,
    extensions: &[String],
    scan_everything: bool,
    content_type: &str,
) -> bool {
    if scan_everything {
        return has_file_extension(url) || !content_type.starts_with("text/html");
    }

    let Some(extension) = url_extension(url) else {
        return false;
    };
    extensions.iter().any(|item| item == &extension)
}

fn content_type_matches_url(url: &Url, content_type: &str, scan_everything: bool) -> bool {
    let family = content_type_family(content_type);
    if family.is_empty() || family == "application/octet-stream" {
        return true;
    }

    let Some(extension) = url_extension(url) else {
        return scan_everything && !family.starts_with("text/html");
    };

    match extension.as_str() {
        "webm" | "mp4" | "mkv" | "avi" | "mov" => family.starts_with("video/"),
        "mp3" | "wav" | "ogg" | "flac" => {
            family.starts_with("audio/") || family == "application/ogg"
        }
        "jpg" | "jpeg" | "png" | "gif" | "webp" | "svg" => family.starts_with("image/"),
        "zip" => family.contains("zip"),
        "rar" => family.contains("rar"),
        "tar" => family.contains("tar"),
        "gz" => family.contains("gzip") || family == "application/x-gzip",
        "7z" => family.contains("7z"),
        "pdf" => family == "application/pdf",
        "txt" => family.starts_with("text/plain"),
        "json" => family == "application/json" || family.ends_with("+json"),
        "xml" => family == "application/xml" || family == "text/xml" || family.ends_with("+xml"),
        "csv" => family == "text/csv" || family == "application/csv",
        "php" | "html" => family.starts_with("text/html"),
        "js" => {
            family == "application/javascript"
                || family == "text/javascript"
                || family == "application/x-javascript"
        }
        "env" | "config" => {
            family.starts_with("text/plain")
                || family == "application/json"
                || family == "application/xml"
                || family == "text/xml"
        }
        _ => scan_everything && !family.starts_with("text/html"),
    }
}

fn should_parse_for_links(url: &Url, content_type: &str) -> bool {
    let family = content_type_family(content_type);
    family.starts_with("text/")
        || family == "application/json"
        || family.ends_with("+json")
        || family == "application/javascript"
        || family == "application/x-javascript"
        || family == "application/xml"
        || family.ends_with("+xml")
        || url_extension(url)
            .map(|extension| {
                matches!(
                    extension.as_str(),
                    "html" | "js" | "css" | "json" | "xml" | "txt"
                )
            })
            .unwrap_or(false)
}

fn should_crawl_url(url: &Url) -> bool {
    match url_extension(url).as_deref() {
        None => true,
        Some("html" | "htm" | "js" | "css" | "json" | "xml" | "txt") => true,
        Some(_) => false,
    }
}

fn looks_like_reference(value: &str) -> bool {
    if value.len() < 3 || value.starts_with("data:") || value.starts_with('#') {
        return false;
    }
    if value.starts_with("http://") || value.starts_with("https://") || value.starts_with('/') {
        return true;
    }
    url_extension_from_path(value).is_some()
}

fn file_result(url: Url, status: u16, content_type: String, size: Option<u64>) -> FileResult {
    let extension = url_extension(&url).unwrap_or_else(|| "unknown".to_owned());
    FileResult {
        url: url.to_string(),
        status,
        content_type: if content_type.is_empty() {
            "unknown".to_owned()
        } else {
            content_type
        },
        size,
        extension,
    }
}

fn inferred_directory_result(url: Url) -> FileResult {
    FileResult {
        url: url.to_string(),
        status: 0,
        content_type: "inferred directory".to_owned(),
        size: None,
        extension: "dir".to_owned(),
    }
}

fn inferred_api_result(url: Url) -> FileResult {
    FileResult {
        url: url.to_string(),
        status: 0,
        content_type: "discovered API endpoint".to_owned(),
        size: None,
        extension: "api".to_owned(),
    }
}

fn content_type_family(content_type: &str) -> String {
    content_type
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase()
}

fn body_signature(bytes: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    let sample_len = bytes.len().min(16 * 1024);
    bytes[..sample_len].hash(&mut hasher);
    bytes.len().hash(&mut hasher);
    hasher.finish()
}

fn parse_content_range_total(value: &str) -> Option<u64> {
    let (_, total) = value.rsplit_once('/')?;
    if total == "*" {
        return None;
    }
    total.parse().ok()
}

fn progress(checked: usize, found: usize, total: usize, started: Instant) -> FileProgress {
    let elapsed = started.elapsed();
    let seconds = elapsed.as_secs_f64().max(0.001);
    FileProgress {
        checked,
        found,
        total,
        elapsed_ms: elapsed.as_millis(),
        rate: checked as f64 / seconds,
    }
}

fn normalize_base_url(input: &str) -> Result<Url, String> {
    let trimmed = input.trim();
    let with_scheme = if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        trimmed.to_owned()
    } else {
        format!("https://{trimmed}")
    };
    Url::parse(&with_scheme).map_err(|err| format!("Enter a valid URL: {err}"))
}

fn same_origin(left: &Url, right: &Url) -> bool {
    left.scheme() == right.scheme()
        && left.domain() == right.domain()
        && left.port_or_known_default() == right.port_or_known_default()
}

fn has_file_extension(url: &Url) -> bool {
    url_extension(url).is_some()
}

fn url_extension(url: &Url) -> Option<String> {
    url_extension_from_path(url.path())
}

fn url_extension_from_path(path: &str) -> Option<String> {
    let path = path.split('?').next().unwrap_or(path);
    let path = path.rsplit('/').next()?;
    let (_, ext) = path.rsplit_once('.')?;
    if ext.is_empty() || ext.len() > 12 {
        return None;
    }
    Some(normalize_extension(ext))
}

fn is_bare_filename(value: &str) -> bool {
    !value.contains('/') && !value.contains('\\') && !value.starts_with("http")
}

fn is_media_extension(extension: &str) -> bool {
    matches!(
        extension,
        "webm" | "mp4" | "mkv" | "avi" | "mov" | "mp3" | "wav" | "ogg" | "flac"
    )
}

fn is_media_url(url: &Url) -> bool {
    url_extension(url)
        .map(|extension| is_media_extension(&extension))
        .unwrap_or(false)
}

fn is_api_endpoint(url: &Url) -> bool {
    url.path().starts_with("/api/")
}

fn parent_directory_url(url: &Url) -> Option<Url> {
    let mut directory = url.clone();
    let mut segments = directory.path_segments_mut().ok()?;
    segments.pop();
    segments.pop_if_empty();
    segments.push("");
    drop(segments);
    Some(directory)
}

fn companion_media_filenames(filename: &str) -> Vec<String> {
    let Some((stem, extension)) = filename.rsplit_once('.') else {
        return Vec::new();
    };

    match normalize_extension(extension).as_str() {
        "webm" | "mp4" | "mov" | "mkv" | "avi" => {
            vec![
                format!("{stem}.webp"),
                format!("{stem}.png"),
                format!("{stem}.jpg"),
            ]
        }
        _ => Vec::new(),
    }
}

fn normalize_extension(extension: &str) -> String {
    extension
        .trim()
        .trim_start_matches('.')
        .to_ascii_lowercase()
}
