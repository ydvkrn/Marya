// Map: extension -> MIME
const MIME = {
  jpg: "image/jpeg", jpeg: "image/jpeg", png: "image/png", gif: "image/gif", webp: "image/webp", svg: "image/svg+xml",
  mp4: "video/mp4", webm: "video/webm", mkv: "video/x-matroska", mov: "video/quicktime",
  mp3: "audio/mpeg", m4a: "audio/mp4", wav: "audio/wav", flac: "audio/flac", aac: "audio/aac", ogg: "audio/ogg",
  pdf: "application/pdf", txt: "text/plain", json: "application/json", csv: "text/csv",
  zip: "application/zip", rar: "application/vnd.rar", "7z": "application/x-7z-compressed"
};

function mimeFromSlug(slug) {
  const ext = (slug.split(".").pop() || "").toLowerCase();
  return MIME[ext] || "application/octet-stream";
}

export async function onRequest(context) {
  const { request, env, params } = context;
  const slug = params.slug;

  try {
    // 1) KV से Telegram direct URL निकालें
    const directUrl = await env.FILES_KV.get(slug, "text");
    if (!directUrl) return new Response("File not found", { status: 404 }); // Not in KV [14]

    // 2) वीडियो seek काम करे: क्लाइंट की Range हेडर फॉरवर्ड करें
    const range = request.headers.get("Range");
    const upstream = await fetch(directUrl, { headers: range ? { Range: range } : {} }); // Forward range [13]

    if (!upstream.ok) return new Response("Upstream error", { status: upstream.status }); // Propagate status [14]

    // 3) जरूरी हेडर कॉपी करें
    const h = new Headers();
    for (const [k, v] of upstream.headers.entries()) {
      const lk = k.toLowerCase();
      if (["content-type", "content-length", "content-range", "accept-ranges", "etag", "last-modified"].includes(lk)) {
        h.set(k, v);
      }
    }

    // 4) अगर Telegram ने Content-Type न दिया या octet-stream दिया, तो slug से सही MIME सेट करें
    const currentCT = h.get("Content-Type") || "";
    if (!currentCT || currentCT.toLowerCase() === "application/octet-stream") {
      h.set("Content-Type", mimeFromSlug(slug));
    } // Force correct MIME to allow inline rendering [2][4][11]

    // 5) CORS/Cache/Range हेडर
    h.set("Access-Control-Allow-Origin", "*");
    h.set("Accept-Ranges", "bytes");
    h.set("Cache-Control", "public, max-age=31536000, immutable"); // Long cache for static files [14]

    // 6) View vs Download
    const isDownload = new URL(request.url).searchParams.has("dl");
    const ct = (h.get("Content-Type") || "").toLowerCase();
    if (isDownload) {
      h.set("Content-Disposition", `attachment; filename="${slug}"`);
    } else {
      // इमेज/वीडियो/ऑडियो/PDF को inline दिखाएं
      if (ct.startsWith("image/") || ct.startsWith("video/") || ct.startsWith("audio/") || ct === "application/pdf" || ct.startsWith("text/")) {
        h.set("Content-Disposition", "inline");
      } else {
        h.set("Content-Disposition", `attachment; filename="${slug}"`);
      }
    } // Inline for view, attachment only for download [4][14]

    return new Response(upstream.body, { status: upstream.status, headers: h }); // Stream with corrected headers [14]
  } catch (e) {
    return new Response(`Server error: ${e.message}`, { status: 500 }); // Surface error cleanly [14]
  }
}
