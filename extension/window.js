// Standalone window: read Chrome's YouTube cookies, hand them to the local server,
// then show the server's web UI (in an iframe) pre-filled with the current video.

// The server tries these ports in order; the extension probes the same set.
const PORTS = [8080, 8081, 8082];
let SERVER = null;

const statusEl = document.getElementById("status");
const appFrame = document.getElementById("app");
const errEl = document.getElementById("err");
const refreshBtn = document.getElementById("refresh");

function setStatus(text, kind) {
  statusEl.textContent = text;
  statusEl.className = kind || "";
}

function showError(html) {
  appFrame.style.display = "none";
  errEl.style.display = "block";
  errEl.innerHTML = html;
}

function hideError() {
  errEl.style.display = "none";
  appFrame.style.display = "block";
}

// Convert chrome.cookies entries to Netscape cookies.txt (what yt-dlp expects).
function toNetscape(cookies) {
  let out = "# Netscape HTTP Cookie File\n";
  for (const c of cookies) {
    const domain = c.domain || "";
    const includeSub = domain.startsWith(".") ? "TRUE" : "FALSE";
    const path = c.path || "/";
    const secure = c.secure ? "TRUE" : "FALSE";
    const expiry = c.expirationDate ? Math.floor(c.expirationDate) : 0;
    out += [domain, includeSub, path, secure, expiry, c.name, c.value].join("\t") + "\n";
  }
  return out;
}

async function collectCookies() {
  const domains = ["youtube.com", "google.com"];
  let all = [];
  for (const d of domains) {
    try { all = all.concat(await chrome.cookies.getAll({ domain: d })); } catch (e) { /* ignore */ }
  }
  const seen = new Set();
  const uniq = [];
  for (const c of all) {
    const key = c.domain + "|" + c.path + "|" + c.name;
    if (!seen.has(key)) { seen.add(key); uniq.push(c); }
  }
  return uniq;
}

async function sendCookies() {
  setStatus("Читаю cookies…");
  const cookies = await collectCookies();
  const txt = toNetscape(cookies);
  try {
    const res = await fetch(SERVER + "/cookies", {
      method: "POST",
      headers: { "Content-Type": "text/plain" },
      body: txt,
    });
    const data = await res.json();
    setStatus("Cookies отправлены: " + (data.cookies ?? cookies.length), "ok");
    return true;
  } catch (e) {
    setStatus("Сервер недоступен", "err");
    showError(
      "Не удалось связаться с сервером <code>" + SERVER + "</code>.<br><br>" +
      "Запустите <code>downloader.exe</code> (порт 8080) и нажмите <b>↻ cookies</b>."
    );
    return false;
  }
}

async function loadApp() {
  // Start on a clean page — no auto "Check video" on the last watched clip.
  appFrame.src = SERVER + "/";
  hideError();
}

// Find which of the candidate ports the server is actually listening on.
async function findServer() {
  for (const p of PORTS) {
    const base = "http://localhost:" + p;
    try {
      const r = await fetch(base + "/health", { method: "GET" });
      if (r.ok) return base;
    } catch (e) { /* nothing listening on this port */ }
  }
  return null;
}

async function init() {
  setStatus("Поиск сервера (порты " + PORTS.join(", ") + ")…");
  SERVER = await findServer();
  if (!SERVER) {
    setStatus("Сервер не найден", "err");
    showError(
      "Сервер не найден на портах <code>" + PORTS.join(", ") + "</code>.<br><br>" +
      "Запустите <code>downloader.exe</code> и нажмите <b>↻ cookies</b>."
    );
    return;
  }
  const ok = await sendCookies();
  if (ok) await loadApp();
}

refreshBtn.addEventListener("click", init);
init();
