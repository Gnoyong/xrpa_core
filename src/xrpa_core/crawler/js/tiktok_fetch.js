const config = arguments[0] || {};

return (async () => {
  try {
    const headers = config.headers || { "content-type": "application/json" };
    const init = {
      method: config.method || "GET",
      headers,
      credentials: config.credentials || "include",
    };

    if (config.body !== undefined && config.body !== null && init.method !== "GET") {
      init.body = typeof config.body === "string" ? config.body : JSON.stringify(config.body);
    }

    const resp = await fetch(config.url, init);
    const respHeaders = {};
    resp.headers.forEach((value, key) => {
      respHeaders[key] = value;
    });

    const contentType = resp.headers.get("content-type") || "";
    const responseText = await resp.text();

    return {
      ok: resp.ok,
      status: resp.status,
      statusText: resp.statusText,
      headers: respHeaders,
      contentType,
      responseIsJson: contentType.toLowerCase().includes("application/json"),
      response: responseText,
    };
  } catch (error) {
    return {
      ok: false,
      step: "fetch",
      error: error?.message || String(error),
      name: error?.name || "Error",
      code: error?.name === "AbortError" ? "FETCH_TIMEOUT" : "FETCH_NETWORK_ERROR",
    };
  }
})();
