export async function onRequest({ request, env }) {
  if (request.method !== "POST")
    return new Response("Method Not Allowed", { status: 405 });
  const { url } = await request.json();
  // Auto-generate id and filename
  const id = Math.random().toString(36).slice(2,12) + Date.now().toString(36);
  const filename = url.split('/').pop().split('?')[0];
  // Download from original url in chunks and store each in KV
  const res = await fetch(url);
  if (!res.ok) return new Response("URL Download failed", { status: 500 });
  const reader = res.body.getReader();
  let idx = 0, total = 0, buffers = [], chunkCount = 0;
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    await env.FILES_KV.put(`${id}_chunk_${idx}`, value.buffer);
    idx++; total += value.length;
  }
  chunkCount = idx;
  // Store meta
  await env.FILES_KV.put(`${id}_meta`, JSON.stringify({filename, total:chunkCount, uploadedAt: Date.now()}));
  return new Response(JSON.stringify({ link: `/btfstorage/file/${id}_${filename}` }));
}
