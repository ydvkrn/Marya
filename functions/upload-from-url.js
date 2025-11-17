// functions/upload-from-url.js
// MARYA VAULT URL UPLOAD v3.0 → 625 MB MAX | 25 KV | Cloudflare Pages Ready

export async function onRequestPost({ request, env }) {
  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Max-Age': '86400',
  };

  if (request.method === 'OPTIONS') return new Response(null, { headers: corsHeaders });

  try {
    // ====== 25 KV BINDINGS (tune already bind kar diye hain) ======
    const kvNamespaces = [
      env.FILES_KV,  env.FILES_KV2,  env.FILES_KV3,  env.FILES_KV4,  env.FILES_KV5,
      env.FILES_KV6,  env.FILES_KV7,  env.FILES_KV8,  env.FILES_KV9,  env.FILES_KV10,
      env.FILES_KV11, env.FILES_KV12, env.FILES_KV13, env.FILES_KV14, env.FILES_KV15,
      env.FILES_KV16, env.FILES_KV17, env.FILES_KV18, env.FILES_KV19, env.FILES_KV20,
      env.FILES_KV21, env.FILES_KV22, env.FILES_KV23, env.FILES_KV24, env.FILES_KV25
    ].filter(Boolean); // jo bind nahi hoga wo skip

    if (kvNamespaces.length < 10) throw new Error('Kam se kam 10 KV bind karo bhai!');

    const BOT_TOKEN = env.BOT_TOKEN;
    const CHANNEL_ID = env.CHANNEL_ID;
    if (!BOT_TOKEN || !CHANNEL_ID) throw new Error('BOT_TOKEN ya CHANNEL_ID missing hai');

    // ====== MAX LIMITS (25 KV = 625 MB possible) ======
    const MAX_FILE_SIZE = 625 * 1024 * 1024;  // 625 MB
    const CHUNK_SIZE = 25 * 1024 * 1024;     // 25 MB (KV ka full limit)

    // ====== Parse JSON body ======
    const body = await request.json();
    const url = body.url || body.fileUrl;
    const customName = body.filename || null;

    if (!url) return new Response(JSON.stringify({ success: false, error: 'Bhai URL toh daal!' }), { status: 400, headers: corsHeaders });

    // ====== Fetch file from URL ======
    const res = await fetch(url, { redirect: 'follow' });
    if (!res.ok || !res.body) throw new Error('URL se file nahi aayi');

    const blob = await res.blob();
    if (blob.size === 0) throw new Error('File khali hai');
    if (blob.size > MAX_FILE_SIZE) throw new Error(`File bohot bada hai: ${ (blob.size/1024/1024).toFixed(1) } MB (Max 625 MB)`);

    let filename = customName || url.split('/').pop().split('?')[0] || 'unknown_file';
    filename = decodeURIComponent(filename.replace(/[<>:"|?*]/g, '_')).substring(0, 200);

    const file = new File([blob], filename, { type: blob.type || 'application/octet-stream' });

    // ====== Chunking ======
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);
    if (totalChunks > kvNamespaces.length) {
      throw new Error(`File ko ${totalChunks} chunks chahiye, lekin sirf ${kvNamespaces.length} KV available hain`);
    }

    const fileId = `url_${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}`;
    const ext = filename.includes('.') ? filename.slice(filename.lastIndexOf('.')) : '';

    const uploadStart = Date.now();

    // ====== Upload all chunks ======
    const results = await Promise.all(
      Array.from({ length: totalChunks }, async (_, i) => {
        const start = i * CHUNK_SIZE;
        const end = Math.min(start + CHUNK_SIZE, file.size);
        const chunk = file.slice(start, end);
        const chunkFile = new File([chunk], `${filename}.part${i}`, { type: file.type });

        const kv = kvNamespaces[i % kvNamespaces.length];

        // Upload to Telegram
        const form = new FormData();
        form.append('chat_id', CHANNEL_ID);
        form.append('document', chunkFile);

        const tgRes = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/sendDocument`, {
          method: 'POST',
          body: form
        });
        const tgData = await tgRes.json();
        if (!tgData.ok) throw new Error(tgData.description || 'Telegram fail');

        const fileIdTg = tgData.result.document.file_id;

        // Get direct link
        const pathRes = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/getFile?file_id=${fileIdTg}`);
        const pathData = await pathRes.json();
        if (!pathData.ok) throw new Error('getFile failed');
        const directUrl = `https://api.telegram.org/file/bot${BOT_TOKEN}/${pathData.result.file_path}`;

        // Save metadata in KV
        const key = `${fileId}_chunk_${i}`;
        await kv.put(key, JSON.stringify({
          telegramFileId: fileIdTg,
          directUrl,
          size: chunk.size,
          uploadedAt: Date.now()
        }));

        return { kvName: kv === env.FILES_KV ? 'FILES_KV' : `FILES_KV${kvNamespaces.indexOf(kv)+1}`, size: chunk.size };
      })
    );

    // ====== Save master metadata ======
    await kvNamespaces[0].put(fileId, JSON.stringify({
      filename, size: file.size, type: file.type, ext,
      totalChunks, uploadedAt: Date.now(), sourceUrl: url
    }));

    const base = new URL(request.url).origin;
    const link = `${base}/btfstorage/file/${fileId}${ext}`;

    return new Response(JSON.stringify({
      success: true,
      message: "URL se upload ho gaya bhai!",
      data: {
        id: fileId,
        filename,
        size: file.size,
        size_mb: (file.size/1024/1024).toFixed(2) + ' MB',
        chunks: totalChunks,
        time_taken: Math.round((Date.now() - uploadStart)/1000) + ' sec',
        urls: {
          view: link,
          download: link + '?dl=1',
          stream: link + '?stream=1'
        },
        max_limit: "625 MB (25 KV)"
      }
    }, null, 2), {
      status: 200,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (err) {
    return new Response(JSON.stringify({
      success: false,
      error: err.message || 'Kuch toh gadbad hai daya'
    }, null, 2), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}

// Helper
function formatBytes(b) {
  const units = ['B', 'KB', 'MB', 'GB'];
  let i = 0;
  while (b >= 1024 && i < units.length-1) { b /= 1024; i++; }
  return `${b.toFixed(1)} ${units[i]}`;
}