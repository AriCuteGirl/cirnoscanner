const state = {
  mode: "subdomain",
  source: null,
  subdomains: [],
  files: [],
  total: 0,
};
const FILE_OPTIONS_KEY = "mochaScanner.fileOptions";

const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => [...document.querySelectorAll(selector)];

const els = {
  tabs: $$(".mode-tab"),
  subdomainPanel: $("#subdomainPanel"),
  filesPanel: $("#filesPanel"),
  fileFilters: $("#fileFilters"),
  subdomainTable: $("#subdomainTable"),
  filesTable: $("#filesTable"),
  empty: $("#emptyState"),
  scanState: $("#scanState"),
  checked: $("#checked"),
  found: $("#found"),
  total: $("#total"),
  rate: $("#rate"),
  elapsed: $("#elapsed"),
  progressBar: $("#progressBar"),
};

els.tabs.forEach((tab) => {
  tab.addEventListener("click", () => setMode(tab.dataset.mode));
});

$("#subdomainScan").addEventListener("click", () => startSubdomainScan());
$("#fileScan").addEventListener("click", () => startFileScan());
$("#stopScan").addEventListener("click", () => stopScan("Stopped"));
$$("[data-export]").forEach((button) => {
  button.addEventListener("click", () => exportResults(button.dataset.export));
});
$$("[data-export-media]").forEach((button) => {
  button.addEventListener("click", () => exportMediaUrls(button.dataset.exportMedia));
});

$("#domainInput").addEventListener("keydown", submitOnEnter(startSubdomainScan));
$("#urlInput").addEventListener("keydown", submitOnEnter(startFileScan));
bindFileOptionPersistence();

function submitOnEnter(fn) {
  return (event) => {
    if (event.key === "Enter") fn();
  };
}

function setMode(mode) {
  state.mode = mode;
  els.tabs.forEach((tab) => tab.classList.toggle("active", tab.dataset.mode === mode));
  els.subdomainPanel.classList.toggle("active", mode === "subdomain");
  els.filesPanel.classList.toggle("active", mode === "files");
  els.subdomainTable.classList.toggle("active", mode === "subdomain");
  els.filesTable.classList.toggle("active", mode === "files");
  els.fileFilters.style.display = mode === "files" ? "grid" : "none";
  updateEmpty();
}

function startSubdomainScan() {
  const domain = $("#domainInput").value.trim();
  if (!domain) return;
  resetScan();
  state.subdomains = [];
  els.subdomainTable.querySelector("tbody").replaceChildren();
  connect(`/api/scan/subdomains?domain=${encodeURIComponent(domain)}`);
}

function startFileScan() {
  const url = $("#urlInput").value.trim();
  if (!url) return;
  saveFileOptions();
  resetScan();
  state.files = [];
  els.filesTable.querySelector("tbody").replaceChildren();

  const presets = $$("#fileFilters input[type='checkbox'][value]:checked").map((input) => input.value);
  const params = new URLSearchParams({
    url,
    presets: presets.join(","),
    custom: $("#customExt").value,
    everything: $("#everything").checked,
    brute: $("#brute").checked,
    crawl: $("#crawl").checked,
    max_depth: $("#depth").value,
  });
  connect(`/api/scan/files?${params.toString()}`);
}

function bindFileOptionPersistence() {
  restoreFileOptions();
  [
    "#urlInput",
    "#customExt",
    "#everything",
    "#crawl",
    "#brute",
    "#depth",
    "#fileFilters input[type='checkbox'][value]",
  ].forEach((selector) => {
    $$(selector).forEach((input) => {
      input.addEventListener("input", saveFileOptions);
      input.addEventListener("change", saveFileOptions);
    });
  });
}

function restoreFileOptions() {
  let saved;
  try {
    saved = JSON.parse(localStorage.getItem(FILE_OPTIONS_KEY) || "null");
  } catch {
    return;
  }
  if (!saved) return;

  $("#urlInput").value = saved.url || "";
  $("#customExt").value = saved.custom || "";
  $("#everything").checked = !!saved.everything;
  $("#crawl").checked = saved.crawl ?? true;
  $("#brute").checked = saved.brute ?? true;
  $("#depth").value = saved.depth || "2";

  if (Array.isArray(saved.presets)) {
    const selected = new Set(saved.presets);
    $$("#fileFilters input[type='checkbox'][value]").forEach((input) => {
      input.checked = selected.has(input.value);
    });
  }
}

function saveFileOptions() {
  const options = {
    url: $("#urlInput").value,
    custom: $("#customExt").value,
    everything: $("#everything").checked,
    crawl: $("#crawl").checked,
    brute: $("#brute").checked,
    depth: $("#depth").value,
    presets: $$("#fileFilters input[type='checkbox'][value]:checked").map((input) => input.value),
  };
  localStorage.setItem(FILE_OPTIONS_KEY, JSON.stringify(options));
}

function connect(path) {
  stopScan("Starting");
  els.scanState.textContent = "Running";
  state.source = new EventSource(path);
  state.source.onmessage = (event) => handleUpdate(JSON.parse(event.data));
  state.source.onerror = () => {
    if (els.scanState.textContent === "Running") {
      els.scanState.textContent = "Connection closed";
    }
    closeSource();
  };
}

function handleUpdate(update) {
  if (update.type === "started") {
    state.total = update.total || 0;
    setProgress({ checked: 0, found: 0, total: state.total, elapsed_ms: 0, rate: 0 });
    return;
  }

  if (update.type === "result") {
    if (state.mode === "subdomain") {
      state.subdomains.push(update.result);
      appendSubdomain(update.result);
    } else {
      state.files.push(update.result);
      appendFile(update.result);
    }
    updateEmpty();
    return;
  }

  if (update.type === "progress") {
    setProgress(update.progress);
    return;
  }

  if (update.type === "finished") {
    setProgress({ ...update, total: state.total || update.checked, rate: 0 });
    els.scanState.textContent = "Finished";
    closeSource();
    return;
  }

  if (update.type === "error") {
    els.scanState.textContent = update.message;
  }
}

function appendSubdomain(result) {
  const row = document.createElement("tr");
  row.className = rowClass(result.status);
  row.innerHTML = `
    <td>${escapeHtml(result.subdomain)}</td>
    <td>${escapeHtml(result.ip)}</td>
    <td class="${statusClass(result.status)}">${result.status ?? "no http"}</td>
    <td>${result.response_time_ms == null ? "-" : `${result.response_time_ms} ms`}</td>
  `;
  els.subdomainTable.querySelector("tbody").prepend(row);
}

function appendFile(result) {
  const row = document.createElement("tr");
  row.className = rowClass(result.status);
  row.innerHTML = `
    <td class="url-cell">${escapeHtml(result.url)}</td>
    <td class="${statusClass(result.status)}">${displayStatus(result.status)}</td>
    <td>${escapeHtml(result.content_type)}</td>
    <td>${formatSize(result.size)}</td>
    <td><span class="badge">${escapeHtml(result.extension)}</span></td>
  `;
  els.filesTable.querySelector("tbody").prepend(row);
}

function setProgress(progress) {
  const checked = progress.checked || 0;
  const found = progress.found || 0;
  const total = progress.total || state.total || checked;
  const percent = total > 0 ? Math.min(100, (checked / total) * 100) : 0;
  state.total = total;
  els.checked.textContent = checked;
  els.found.textContent = found;
  els.total.textContent = total;
  els.rate.textContent = Number(progress.rate || 0).toFixed(1);
  els.elapsed.textContent = `${((progress.elapsed_ms || 0) / 1000).toFixed(1)}s`;
  els.progressBar.style.width = `${percent}%`;
}

function resetScan() {
  closeSource();
  state.total = 0;
  els.scanState.textContent = "Preparing";
  setProgress({ checked: 0, found: 0, total: 0, elapsed_ms: 0, rate: 0 });
  updateEmpty();
}

function stopScan(label = "Idle") {
  closeSource();
  els.scanState.textContent = label;
}

function closeSource() {
  if (state.source) {
    state.source.close();
    state.source = null;
  }
}

function updateEmpty() {
  const count = state.mode === "subdomain" ? state.subdomains.length : state.files.length;
  els.empty.classList.toggle("hidden", count > 0);
}

function exportResults(format) {
  const data = state.mode === "subdomain" ? state.subdomains : state.files;
  if (!data.length) return;

  let body;
  let mime;
  if (format === "json") {
    body = JSON.stringify(data, null, 2);
    mime = "application/json";
  } else if (format === "csv") {
    body = toCsv(data);
    mime = "text/csv";
  } else {
    body = data.map((row) => Object.values(row).join("\t")).join("\n");
    mime = "text/plain";
  }

  downloadBlob(body, mime, `${state.mode}-results.${format}`);
}

function exportMediaUrls(kind) {
  if (state.mode !== "files") return;
  const urls = state.files
    .filter((row) => row.status !== 0)
    .filter((row) => (kind === "video" ? isMediaFile(row) : isImageFile(row)))
    .map((row) => row.url);
  const uniqueUrls = [...new Set(urls)].sort((a, b) => a.localeCompare(b));
  if (!uniqueUrls.length) {
    els.scanState.textContent = kind === "video" ? "No media URLs to export" : "No image URLs to export";
    return;
  }

  downloadBlob(uniqueUrls.join("\n"), "text/plain", `${kind}-urls.txt`);
}

function downloadBlob(body, mime, filename) {
  const link = document.createElement("a");
  link.href = URL.createObjectURL(new Blob([body], { type: mime }));
  link.download = filename;
  link.click();
  URL.revokeObjectURL(link.href);
}

function isMediaFile(row) {
  const ext = String(row.extension || "").toLowerCase();
  const type = String(row.content_type || "").toLowerCase();
  return ["webm", "mp4", "mkv", "avi", "mov", "mp3", "wav", "ogg", "flac"].includes(ext)
    || type.startsWith("video/")
    || type.startsWith("audio/");
}

function isImageFile(row) {
  const ext = String(row.extension || "").toLowerCase();
  const type = String(row.content_type || "").toLowerCase();
  return ["jpg", "jpeg", "png", "gif", "webp", "svg", "ico"].includes(ext) || type.startsWith("image/");
}

function toCsv(rows) {
  const headers = Object.keys(rows[0]);
  const lines = rows.map((row) => headers.map((key) => csvCell(row[key])).join(","));
  return [headers.join(","), ...lines].join("\n");
}

function csvCell(value) {
  const text = value == null ? "" : String(value);
  return `"${text.replaceAll('"', '""')}"`;
}

function rowClass(status) {
  if (status === 0) return "";
  if (!status) return "";
  if (status >= 200 && status < 300) return "ok";
  if (status >= 300 && status < 400) return "redirect";
  if (status >= 400) return "error";
  return "";
}

function statusClass(status) {
  if (status === 0) return "";
  if (!status) return "";
  return `status-${String(status).charAt(0)}`;
}

function displayStatus(status) {
  return status === 0 ? "inferred" : status;
}

function formatSize(size) {
  if (size == null) return "-";
  if (size < 1024) return `${size} B`;
  if (size < 1024 * 1024) return `${(size / 1024).toFixed(1)} KB`;
  return `${(size / 1024 / 1024).toFixed(1)} MB`;
}

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#039;",
  }[char]));
}

setMode("subdomain");
