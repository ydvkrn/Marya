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
    const MAX_DIRECT = 100 * 1024 * 1024; // 100 MB cap at Cloudflare edge
    const form = await request.formData();
    const file = form.get("file");
    if (!file) return Response.json({ success: false, error: "No file" }, { status: 400, headers: cors });

    if (file.size > MAX_DIRECT) {
      // Tell client to use R2 flow
      return Response.json({ success: true, needR2: true, size: file.size }, { headers: cors });
    }

    const tgForm = new FormData();
    tgForm.append("chat_id", CHANNEL_ID);
    tgForm.append("document", file, file.name);
    const tgResp = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/sendDocument`, { method: "POST", body: tgForm });
    if (!tgResp.ok) return Response.json({ success: false, error: `Telegram ${tgResp.status}` }, { status: 502, headers: cors });
    const tgJson = await tgResp.json();
    if (!tgJson.ok || !tgJson.result?.document?.file_id) return Response.json({ success: false, error: tgJson.description || "Telegram failed" }, { status: 502, headers: cors });

    const fid = tgJson.result.document.file_id;
    const gf = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/getFile?file_id=${encodeURIComponent(fid)}`);
    if (!gf.ok) return Response.json({ success: false, error: `getFile ${gf.status}` }, { status: 502, headers: cors });
    const gfJson = await gf.json();
    if (!gfJson.ok || !gfJson.result?.file_path) return Response.json({ success: false, error: gfJson.description || "getFile failed" }, { status: 502, headers: cors });

    const directUrl = `https://api.telegram.org/file/bot${BOT_TOKEN}/${gfJson.result.file_path}`;
    const ext = file.name.includes(".") ? "." + file.name.split(".").pop().toLowerCase() : "";
    const slug = `id${Math.random().toString(36).slice(2, 10)}${Date.now().toString(36)}${ext}`;
    await env.FILES_KV.put(slug, directUrl, {
      metadata: { filename: file.name, size: file.size, contentType: file.type, uploadedAt: Date.now() }
    });

    const base = new URL(request.url).origin;
    return Response.json({
      success: true,
      filename: file.name,
      size: file.size,
      contentType: file.type,
      url: `${base}/btfstorage/file/${slug}`,
      download: `${base}/btfstorage/file/${slug}?dl=1`
    }, { headers: cors });
  } catch (e) {
    return Response.json({ success: false, error: e.message }, { status: 500, headers: cors });
  }
}
