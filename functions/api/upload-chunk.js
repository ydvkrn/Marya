export async function onRequest(context) {
  const { request, env } = context;
  if (request.method !== "POST")
    return new Response("Method Not Allowed", { status: 405 });
  const form = await request.formData();
  const chunk = form.get("chunk");
  const id = form.get("id"), idx = +form.get("idx"), total = +form.get("total");
  const filename = form.get("filename");

  // Store chunk: key = id_chunk_idx
  await env.FILES_KV.put(`${id}_chunk_${idx}`, await chunk.arrayBuffer());
  // Store metadata on idx==0
  if (idx === 0)
    await env.FILES_KV.put(`${id}_meta`, JSON.stringify({filename, total, uploadedAt: Date.now()}));
  // Mark assembled-file KV on last chunk (for browser simplicity you can check on GET, see below)
  return new Response(JSON.stringify({ success: true }));
}
