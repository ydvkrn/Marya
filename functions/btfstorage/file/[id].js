// ULTRA-SIMPLE VIDEO STREAMING - NO LIMITS HIT
// सिर्फ जरूरी chunks load करता है, बाकी browser को छोड़ देता है

const MIME = {
  'mp4':'video/mp4', 'mkv':'video/mp4', 'avi':'video/mp4', 'mov':'video/mp4',
  'm4v':'video/mp4', 'wmv':'video/mp4', 'flv':'video/mp4', '3gp':'video/mp4',
  'webm':'video/webm', 'mp3':'audio/mpeg', 'wav':'audio/wav', 'aac':'audio/mp4',
  'm4a':'audio/mp4', 'ogg':'audio/ogg'
};

export async function onRequest({ request, env, params }) {
  const fileId = params.id;
  const id = fileId.includes('.') ? fileId.slice(0, fileId.lastIndexOf('.')) : fileId;
  const ext = fileId.includes('.') ? fileId.slice(fileId.lastIndexOf('.')+1).toLowerCase() : '';
  
  if (!id.startsWith('MSM')) return new Response('404', {status: 404});

  console.log(`🔥 Ultra-simple request: ${fileId}`);

  try {
    // Get metadata
    const meta = JSON.parse(await env.FILES_KV.get(id) || '{}');
    if (!meta.size) return new Response('404', {status: 404});

    const mime = MIME[ext] || 'application/octet-stream';
    const chunks = meta.chunks || [];
    const size = meta.size;
    const chunkSize = meta.chunkSize || Math.ceil(size / chunks.length);

    console.log(`📁 ${meta.filename}: ${chunks.length} chunks, ${Math.round(size/1024/1024)}MB`);

    const range = request.headers.get('Range');
    const url = new URL(request.url);
    const dl = url.searchParams.get('dl') === '1';

    // CRITICAL: Handle Range requests ONLY (for video seeking)
    if (range && !dl) {
      return await handleRange(range, size, chunks, chunkSize, mime, env);
    }

    // For non-range: Send SMALL initial chunk to force browser into range mode
    console.log(`🎯 Sending initial 2MB to force range mode...`);
    const initialSize = Math.min(2 * 1024 * 1024, size - 1); // 2MB max
    return await handleRange(`bytes=0-${initialSize - 1}`, size, chunks, chunkSize, mime, env);

  } catch (error) {
    console.error('❌ Error:', error);
    return new Response(`Error: ${error.message}`, {status: 500});
  }
}

// Handle Range - ONLY load chunks needed for this range
async function handleRange(rangeHeader, totalSize, chunks, chunkSize, mime, env) {
  const match = rangeHeader.match(/bytes=(\d+)-(\d*)/);
  if (!match) return new Response('Bad range', {status: 416});

  const start = parseInt(match[1], 10);
  const end = match[2] ? parseInt(match[2], 10) : totalSize - 1;
  
  if (start >= totalSize || end >= totalSize || start > end) {
    return new Response('Range not satisfiable', {
      status: 416, 
      headers: {'Content-Range': `bytes */${totalSize}`}
    });
  }

  const rangeSize = end - start + 1;
  console.log(`📺 Range: ${start}-${end} (${Math.round(rangeSize/1024)}KB)`);

  // Find chunks that overlap with this range
  const startChunk = Math.floor(start / chunkSize);
  const endChunk = Math.floor(end / chunkSize);
  const neededChunks = chunks.slice(startChunk, endChunk + 1);

  console.log(`📦 Need chunks: ${startChunk}-${endChunk} (${neededChunks.length})`);

  // CRITICAL: Load only these chunks (max 5-6 chunks to stay under limits)
  if (neededChunks.length > 8) {
    console.log('⚠️ Too many chunks needed, reducing range...');
    // Reduce to first 8 chunks to stay under limits
    const reducedChunks = neededChunks.slice(0, 8);
    const reducedEndChunk = startChunk + 7;
    const reducedEnd = Math.min(end, (reducedEndChunk + 1) * chunkSize - 1);
    const reducedSize = reducedEnd - start + 1;
    
    const combinedData = await combineChunks(reducedChunks, env, startChunk);
    const exactData = combinedData.slice(start % chunkSize, start % chunkSize + reducedSize);
    
    return new Response(exactData, {
      status: 206,
      headers: {
        'Content-Type': mime,
        'Content-Length': reducedSize.toString(),
        'Content-Range': `bytes ${start}-${reducedEnd}/${totalSize}`,
        'Accept-Ranges': 'bytes',
        'Access-Control-Allow-Origin': '*'
      }
    });
  }

  // Load and combine needed chunks
  const combinedData = await combineChunks(neededChunks, env, startChunk);
  
  // Extract exact range
  const rangeStartInData = start - (startChunk * chunkSize);
  const exactData = combinedData.slice(rangeStartInData, rangeStartInData + rangeSize);

  console.log(`✅ Range response: ${exactData.byteLength} bytes`);

  return new Response(exactData, {
    status: 206,
    headers: {
      'Content-Type': mime,
      'Content-Length': exactData.byteLength.toString(),
      'Content-Range': `bytes ${start}-${end}/${totalSize}`,
      'Accept-Ranges': 'bytes',
      'Access-Control-Allow-Origin': '*',
      'Content-Disposition': 'inline'
    }
  });
}

// Combine chunks efficiently
async function combineChunks(chunkInfos, env, startIndex) {
  const parts = [];
  let totalSize = 0;

  // Load chunks sequentially (avoid parallel overload)
  for (let i = 0; i < chunkInfos.length; i++) {
    const info = chunkInfos[i];
    const actualIndex = startIndex + i;
    
    console.log(`📥 Loading chunk ${actualIndex + 1}...`);
    
    try {
      const data = await loadChunk(info, env);
      parts.push(new Uint8Array(data));
      totalSize += data.byteLength;
      
      console.log(`✅ Chunk ${actualIndex + 1}: ${Math.round(data.byteLength/1024)}KB`);
    } catch (err) {
      console.error(`❌ Chunk ${actualIndex + 1} failed:`, err.message);
      throw new Error(`Chunk ${actualIndex + 1} load failed`);
    }
  }

  // Combine all parts
  const combined = new Uint8Array(totalSize);
  let offset = 0;
  
  for (const part of parts) {
    combined.set(part, offset);
    offset += part.byteLength;
  }

  console.log(`🔗 Combined ${parts.length} chunks: ${Math.round(totalSize/1024)}KB`);
  return combined;
}

// Load single chunk with bot fallback
async function loadChunk(info, env) {
  const kv = env[info.kvNamespace] || env.FILES_KV;
  const metaStr = await kv.get(info.keyName);
  if (!metaStr) throw new Error(`Chunk meta not found: ${info.keyName}`);
  
  const meta = JSON.parse(metaStr);
  let url = meta.directUrl;

  // Try direct URL
  let res = await fetch(url, {signal: AbortSignal.timeout(30000)});
  if (res.ok) return res.arrayBuffer();

  console.log(`🔄 URL expired, refreshing...`);

  // Refresh URL with bot token
  const bots = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);
  if (bots.length === 0) throw new Error('No bot tokens');

  const bot = bots[0]; // Use first available bot
  
  try {
    const getFileRes = await fetch(
      `https://api.telegram.org/bot${bot}/getFile?file_id=${encodeURIComponent(meta.telegramFileId)}`,
      {signal: AbortSignal.timeout(15000)}
    );

    const getFileData = await getFileRes.json();
    if (!getFileData.ok || !getFileData.result?.file_path) {
      throw new Error('getFile failed');
    }

    const freshUrl = `https://api.telegram.org/file/bot${bot}/${getFileData.result.file_path}`;
    res = await fetch(freshUrl, {signal: AbortSignal.timeout(30000)});
    
    if (!res.ok) throw new Error(`Fresh URL failed: ${res.status}`);

    // Update KV async
    kv.put(info.keyName, JSON.stringify({
      ...meta, 
      directUrl: freshUrl, 
      refreshed: Date.now()
    })).catch(() => {});

    return res.arrayBuffer();

  } catch (refreshError) {
    throw new Error(`Refresh failed: ${refreshError.message}`);
  }
}
