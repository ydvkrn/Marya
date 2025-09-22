export async function onRequest(context) {
  const { request, env } = context;

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST,OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type'
  };

  if (request.method === 'OPTIONS')
    return new Response(null, { headers: corsHeaders });

  if (request.method !== 'POST')
    return new Response(JSON.stringify({ error: 'POST only' }), { status: 405, headers: corsHeaders });

  // Handles <form> files and big raw files
  const form = await request.formData();
  const file = form.get('file');
  if (!file) return new Response(JSON.stringify({ error: 'Missing file' }), { status: 400, headers: corsHeaders });

  // --- KV distribution ---
  const kvList = [
    { kv: env.FILES_KV, name: 'FILES_KV' },
    { kv: env.FILES_KV2, name: 'FILES_KV2' },
    { kv: env.FILES_KV3, name: 'FILES_KV3' },
    { kv: env.FILES_KV4, name: 'FILES_KV4' },
    { kv: env.FILES_KV5, name: 'FILES_KV5' },
    { kv: env.FILES_KV6, name: 'FILES_KV6' },
    { kv: env.FILES_KV7, name: 'FILES_KV7' }
  ].filter(i => i.kv);

  const CHUNK_SIZE = 20 * 1024 * 1024;
  const chunks = Math.ceil(file.size / CHUNK_SIZE);

  if (chunks > kvList.length)
    return new Response(JSON.stringify({ error: 'Not enough KV namespaces' }), { status: 400, headers: corsHeaders });

  // Split, Telegram upload & meta to KV
  const meta = { chunks: [], filename: file.name, size: file.size, uploadedAt: Date.now() };
  for (let i = 0; i < chunks; i++) {
    const start = i * CHUNK_SIZE, end = Math.min(start + CHUNK_SIZE, file.size);
    const chunk = file.slice(start, end);
    // Upload to Telegram
    const fd = new FormData();
    fd.append('chat_id', env.CHANNEL_ID);
    fd.append('document', chunk, file.name + `.part${i}`);
    const tgRes = await fetch(`https://api.telegram.org/bot${env.BOT_TOKEN}/sendDocument`, { method: 'POST', body: fd });
    const tg = await tgRes.json();
    if (!tg.ok) return new Response(JSON.stringify({ error: 'Telegram upload failed', details: tg }), { status: 500, headers: corsHeaders });
    const fileId = tg.result.document.file_id, chunkKey = file.name + `_chunk_${i}`;
    // Telegram URL
    const getFile = await fetch(`https://api.telegram.org/bot${env.BOT_TOKEN}/getFile?file_id=${fileId}`).then(r => r.json());
    const tUrl = `https://api.telegram.org/file/bot${env.BOT_TOKEN}/${getFile.result.file_path}`;
    // KV meta
    const chunkMeta = { telegramFileId: fileId, directUrl: tUrl, size: chunk.size, index: i, kvNamespace: kvList[i].name };
    await kvList[i].kv.put(chunkKey, JSON.stringify(chunkMeta));
    meta.chunks.push({ keyName: chunkKey, ...chunkMeta });
  }
  // Put master meta to main KV
  const fileIdKey = 'MSM' + Date.now().toString(36) + Math.random().toString(36).slice(2,8);
  await env.FILES_KV.put(fileIdKey, JSON.stringify(meta));
  const extension = file.name.match(/.w+$/) ? file.name.slice(file.name.lastIndexOf('.')) : '';
  const base = new URL(request.url).origin;
  const playUrl = `${base}/btfstorage/file/${fileIdKey}${extension}`;
  return new Response(JSON.stringify({ id: fileIdKey, chunks, url: playUrl }), { headers: corsHeaders });
}