const MIME = { jpg:"image/jpeg",jpeg:"image/jpeg",png:"image/png",gif:"image/gif",webp:"image/webp",svg:"image/svg+xml",
  mp4:"video/mp4",webm:"video/webm",mkv:"video/x-matroska",mov:"video/quicktime",
  mp3:"audio/mpeg",m4a:"audio/mp4",wav:"audio/wav",flac:"audio/flac",aac:"audio/aac",ogg:"audio/ogg",
  pdf:"application/pdf",txt:"text/plain",json:"application/json",csv:"text/csv",
  zip:"application/zip",rar:"application/vnd.rar","7z":"application/x-7z-compressed" };
const mimeFromSlug = s => MIME[(s.split(".").pop()||"").toLowerCase()] || "application/octet-stream";

export async function onRequest({ request, env, params }) {
  const slug = params.slug;
  try {
    const url = await env.FILES_KV.get(slug, "text");
    if (!url) return new Response("File not found", { status: 404 });

    const range = request.headers.get("Range");
    const upstream = await fetch(url, { headers: range ? { Range: range } : {} });
    if (!upstream.ok) return new Response("Upstream error", { status: upstream.status });

    const h = new Headers();
    for (const [k, v] of upstream.headers.entries()) {
      const lk = k.toLowerCase();
      if (["content-type","content-length","content-range","accept-ranges","etag","last-modified"].includes(lk)) h.set(k, v);
    }
    const current = (h.get("Content-Type") || "").toLowerCase();
    if (!current || current === "application/octet-stream") h.set("Content-Type", mimeFromSlug(slug));

    h.set("Access-Control-Allow-Origin", "*");
    h.set("Accept-Ranges", "bytes");
    h.set("Cache-Control", "public, max-age=31536000, immutable");

    const isDownload = new URL(request.url).searchParams.has("dl");
    const ct = (h.get("Content-Type") || "").toLowerCase();
    if (isDownload) {
      h.set("Content-Disposition", `attachment; filename="${slug}"`);
    } else {
      if (ct.startsWith("image/") || ct.startsWith("video/") || ct.startsWith("audio/") || ct === "application/pdf" || ct.startsWith("text/")) {
        h.set("Content-Disposition", "inline");
      } else {
        h.set("Content-Disposition", `attachment; filename="${slug}"`);
      }
    }
    return new Response(upstream.body, { status: upstream.status, headers: h });
  } catch (e) {
    return new Response(`Server error: ${e.message}`, { status: 500 });
  }
}
