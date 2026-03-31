const TRANSIENT_NETWORK_RETRY_DELAYS_MS = [300, 800, 1500];

function sleep(ms) {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}

function normalizeMethod(method) {
  const text = String(method || "GET").trim().toUpperCase();
  return text || "GET";
}

function isAbortError(err) {
  return err?.name === "AbortError";
}

function isTransientNetworkFailure(err) {
  if (!err || isAbortError(err)) return false;
  const text = String(err?.message || err || "").trim().toLowerCase();
  if (!text) return false;
  return [
    "err_network_changed",
    "network changed",
    "failed to fetch",
    "networkerror",
    "load failed",
    "the network connection was lost",
  ].some((pattern) => text.includes(pattern));
}

function markTransientNetworkError(err, url) {
  const tagged = err instanceof Error ? err : new Error(String(err || "网络请求失败"));
  tagged.isTransientNetworkError = true;
  tagged.code = tagged.code || "transient_network";
  tagged.requestUrl = String(url || "");
  return tagged;
}

export function isTransientNetworkError(err) {
  return Boolean(err?.isTransientNetworkError || isTransientNetworkFailure(err));
}

export async function apiJson(url, options = {}) {
  const {
    headers,
    retryTransientNetworkErrors,
    ...fetchOptions
  } = options || {};
  const method = normalizeMethod(fetchOptions.method);
  const retryCount = Number.isInteger(retryTransientNetworkErrors)
    ? Math.max(0, Number.parseInt(String(retryTransientNetworkErrors), 10) || 0)
    : retryTransientNetworkErrors === false
      ? 0
      : (method === "GET" || method === "HEAD" ? TRANSIENT_NETWORK_RETRY_DELAYS_MS.length : 0);
  let lastTransientError = null;

  for (let attempt = 0; attempt <= retryCount; attempt += 1) {
    try {
      const resp = await fetch(url, {
        headers: { "Content-Type": "application/json", ...(headers || {}) },
        ...fetchOptions,
      });
      if (!resp.ok) {
        const txt = await resp.text();
        const httpError = new Error(txt || `HTTP ${resp.status}`);
        httpError.httpStatus = resp.status;
        httpError.responseText = txt || "";
        httpError.requestUrl = String(url || "");
        throw httpError;
      }
      return await resp.json();
    } catch (err) {
      if (!isTransientNetworkFailure(err) || attempt >= retryCount) {
        throw lastTransientError || err;
      }
      lastTransientError = markTransientNetworkError(err, url);
      await sleep(TRANSIENT_NETWORK_RETRY_DELAYS_MS[attempt] || 300);
    }
  }

  throw lastTransientError || new Error("网络请求失败");
}
