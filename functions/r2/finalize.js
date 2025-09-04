export async function onRequest(context) {
  const { request, env } = context;
  const cors = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type"
  };
  if (request.method === "OPTIONS") return new Response(null, { headers: cors });

  try {
    const BOT_TOKEN = env.BOT_TOKEN;
    const CHANNEL_ID = env.CHANNEL_ID;

    const { slug, objectKey, name, size, type } = await request.json();

    // Make the object temporarily public (signed URL) so Telegram can fetch it
    const readUrl = await env.R2_BUCKET.createPresignedUrl({ method: "GET", key: objectKey, expires: 15 * 60 });

    // Telegram can accept an HTTP URL for document parameter
    const tg = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/sendDocument`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ chat_id: CHANNEL_ID, document: readUrl })
    });
    if (!tg.ok) return Response.json({ success: false, error: `Telegram ${tg.status}` }, { status: 502, headers: cors });
    const tj = await tg.json();
    if (!tj.ok || !tj.result?.document?.file_id) return Response.json({ success: false, error: tj.description || "Telegram failed" }, { status: 502, headers: cors });

    const fid = tj.result.document.file_id;
    const gf = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/getFile?file_id=${encodeURIComponent(fid)}`);
    if (!gf.ok) return Response.json({ success: false, error: `getFile ${gf.status}` }, { status: 502, headers: cors });
    const gfj = await gf.json();
    if (!gfj.ok || !gfj.result?.file_path) return Response.json({ success: false, error: gfj.description || "getFile failed" }, { status: 502, headers: cors });

    const directUrl = `https://api.telegram.org/file/bot${BOT_TOKEN}/${gfj.result.file_path}`;

    await env.FILES_KV.put(slug, directUrl, {
      metadata: { filename: name, size, contentType: type, uploadedAt: Date.now() }
    });

    // Optionally delete temp from R2 to save space
    await env.R2_BUCKET.delete(objectKey).catch(() => {});

    const base = new URL(request.url).origin;
    return Response.json({
      success: true,
      filename: name,
      size,
      contentType: type,
      url: `${base}/btfstorage/file/${slug}`,
      download: `${base}/btfstorage/file/${slug}?dl=1`
    }, { headers: cors });
  } catch (e) {
    return Response.json({ success: false, error: e.message }, { status: 500, headers: cors });
  }
}
