export async function onRequest(context) {
  const { request, env } = context;
  const cors = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type"
  };
  if (request.method === "OPTIONS") return new Response(null, { headers: cors });

  try {
    const { name, size, type } = await request.json();
    const ext = name.includes(".") ? "." + name.split(".").pop().toLowerCase() : "";
    const slug = `id${Math.random().toString(36).slice(2, 10)}${Date.now().toString(36)}${ext}`;
    const objectKey = `tmp/${slug}`;

    // Generate a signed URL to upload to R2 bucket (binding: R2_BUCKET)
    const expiry = 15 * 60; // 15 minutes
    const url = await env.R2_BUCKET.createPresignedUrl({ method: "PUT", key: objectKey, expires: expiry, customHeaders: { "Content-Type": type || "application/octet-stream" } });

    const base = new URL(request.url).origin;
    return Response.json({
      success: true,
      slug,
      uploadUrl: url,
      objectKey,
      finalizeUrl: `${base}/r2/finalize`
    }, { headers: cors });
  } catch (e) {
    return Response.json({ success: false, error: e.message }, { status: 500, headers: cors });
  }
}
