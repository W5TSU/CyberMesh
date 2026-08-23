// Bump this whenever a cached shell route/template changes, or clients keep
// seeing stale HTML served from cache before the background refetch lands.
const CACHE = "cybermesh-shell-v32";
const SHELL = [
  "/",
  "/messages",
  "/bbs",
  "/range",
  "/config",
  "/channels",
  "/static/leaflet/leaflet.js",
  "/static/leaflet/leaflet-heat.js",
  "/static/leaflet/leaflet.css",
  "/static/manifest.json",
  "/static/icon-192.png",
  "/static/icon-512.png",
  "/static/apple-touch-icon.png",
];

self.addEventListener("install", (event) => {
  event.waitUntil(caches.open(CACHE).then((c) => c.addAll(SHELL)));
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((k) => k !== CACHE).map((k) => caches.delete(k)))
    )
  );
  self.clients.claim();
});

self.addEventListener("fetch", (event) => {
  const url = new URL(event.request.url);

  // Never cache live API data — always go to the network.
  if (url.pathname.startsWith("/api/")) {
    return;
  }

  event.respondWith(
    caches.match(event.request).then((cached) => {
      const fetchPromise = fetch(event.request)
        .then((response) => {
          if (response.ok) {
            const copy = response.clone();
            caches.open(CACHE).then((c) => c.put(event.request, copy));
          }
          return response;
        })
        .catch(() => cached);
      return cached || fetchPromise;
    })
  );
});
