export async function onRequest({ request, env }) {
  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST,OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type'
  };
  if (request.method === 'OPTIONS')
    return new Response(null, { headers: corsHeaders });
  if (request.method !== 'POST')
    return new Response(JSON.stringify({ error: 'POST only' }), { status: 405, headers: corsHeaders });

  // { telegramFileId, filename } → import in your DB
  const { telegramFileId, filename } = await request.json();
  if (!telegramFileId) return new Response(JSON.stringify({ error: 'No telegramFileId' }), { status: 400, headers: corsHeaders });
  // Telegram direct URL
  const getFile = await fetch(`https://api.telegram.org/bot${env.BOT_TOKEN}/getFile?file_id=${encodeURIComponent(telegramFileId)}`).then(r=>r.json());
  if (!getFile.ok) return new Response(JSON.stringify({ error: 'GetFile failed', tg: getFile }), { status: 400, headers: corsHeaders });
  const tUrl = `https://api.telegram.org/file/bot${env.BOT_TOKEN}/${getFile.result.file_path}`;
  // Save to KV for consistency
  const fileIdKey = 'MSM' + Date.now().toString(36) + Math.random().toString(36).slice(2,8);
  await env.FILES_KV.put(fileIdKey, JSON.stringify({ telegramFileId, filename, tUrl, uploadedAt: Date.now() }));
  return new Response(JSON.stringify({ id: fileIdKey, filename, url: tUrl }), { headers: corsHeaders });
}