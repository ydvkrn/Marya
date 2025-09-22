// SAFONE.CO STYLE DIRECT STREAMING - ZERO DELAY
// Direct Telegram proxy with instant streaming

const MIME_TYPES = {
  'mp4': 'video/mp4', 'mkv': 'video/mp4', 'avi': 'video/mp4', 'mov': 'video/mp4',
  'm4v': 'video/mp4', 'wmv': 'video/mp4', 'flv': 'video/mp4', '3gp': 'video/mp4',
  'webm': 'video/webm', 'mp3': 'audio/mpeg', 'wav': 'audio/wav', 'aac': 'audio/mp4',
  'm4a': 'audio/mp4', 'ogg': 'audio/ogg', 'jpg': 'image/jpeg', 'jpeg': 'image/jpeg',
  'png': 'image/png', 'gif': 'image/gif', 'pdf': 'application/pdf'
};

export async function onRequest(context) {
  const { request, env, params } = context;
  const hash = params.hash;

  console.log('🎬 SAFONE STYLE STREAM:', hash);

  try {
    // Decode hash to get file info (same format as safone.co)
    const fileInfo = await decodeHash(env, hash);
    if (!fileInfo) {
      return new Response('File not found', { status: 404 });
    }

    const { telegramFileId, filename, size, mimeType } = fileInfo;

    console.log(`🎬 Streaming: ${filename} (${Math.round(size/1024/1024)}MB)`);

    // DIRECT TELEGRAM STREAMING (like safone.co)
    return await directTelegramStream(request, env, telegramFileId, filename, mimeType);

  } catch (error) {
    console.error('❌ Stream error:', error);
    return new Response(`Stream error: ${error.message}`, { status: 500 });
  }
}

// Direct Telegram streaming (Zero CPU usage)
async function directTelegramStream(request, env, telegramFileId, filename, mimeType) {
  const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);
  
  if (botTokens.length === 0) {
    return new Response('Service unavailable', { status: 503 });
  }

  // Try each bot token for best performance
  for (const botToken of botTokens) {
    try {
      console.log(`🚀 Getting direct URL with bot: ${botToken.slice(-4)}`);

      // Get fresh Telegram URL
      const getFileResponse = await fetch(
        `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(telegramFileId)}`,
        { signal: AbortSignal.timeout(8000) }
      );

      if (!getFileResponse.ok) continue;

      const getFileData = await getFileResponse.json();
      if (!getFileData.ok || !getFileData.result?.file_path) continue;

      const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;
      
      console.log(`✅ Direct URL obtained: ${directUrl.slice(0, 80)}...`);

      // PROXY MODE - Direct streaming like safone.co
      const telegramResponse = await fetch(directUrl, {
        headers: request.headers.get('Range') ? { 'Range': request.headers.get('Range') } : {},
        signal: AbortSignal.timeout(45000)
      });

      if (!telegramResponse.ok) continue;

      // Perfect headers for streaming
      const headers = new Headers();
      
      // Copy important headers from Telegram
      if (telegramResponse.headers.get('content-length')) {
        headers.set('Content-Length', telegramResponse.headers.get('content-length'));
      }
      if (telegramResponse.headers.get('content-range')) {
        headers.set('Content-Range', telegramResponse.headers.get('content-range'));
      }
      
      // Set custom headers
      headers.set('Content-Type', mimeType);
      headers.set('Accept-Ranges', 'bytes');
      headers.set('Access-Control-Allow-Origin', '*');
      headers.set('Content-Disposition', 'inline');
      headers.set('Cache-Control', 'public, max-age=3600');
      
      // Streaming headers (like YouTube/Netflix)
      headers.set('X-Content-Duration', 'stream');
      headers.set('Connection', 'keep-alive');

      console.log(`🎬 STREAMING LIVE: ${filename} (Status: ${telegramResponse.status})`);

      return new Response(telegramResponse.body, {
        status: telegramResponse.status,
        headers: headers
      });

    } catch (botError) {
      console.error(`❌ Bot ${botToken.slice(-4)} failed:`, botError.message);
      continue;
    }
  }

  return new Response('All streaming servers failed', { status: 503 });
}

// Decode hash (safone.co style)
async function decodeHash(env, hash) {
  try {
    // Try to get from KV cache first
    const cached = await env.FILES_KV.get(`stream_${hash}`);
    if (cached) {
      return JSON.parse(cached);
    }

    // Fallback: Try to decode from original file system
    const decoded = Buffer.from(hash, 'base64').toString('utf-8');
    const [telegramFileId, filename] = decoded.split('|');
    
    if (!telegramFileId) return null;

    // Get file info from Telegram
    const botToken = env.BOT_TOKEN || env.BOT_TOKEN2;
    const getFileResponse = await fetch(
      `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(telegramFileId)}`,
      { signal: AbortSignal.timeout(10000) }
    );

    const getFileData = await getFileResponse.json();
    if (!getFileData.ok) return null;

    const size = getFileData.result.file_size || 0;
    const extension = filename ? filename.split('.').pop() : 'mp4';
    const mimeType = MIME_TYPES[extension.toLowerCase()] || 'application/octet-stream';

    const fileInfo = {
      telegramFileId,
      filename: filename || 'stream',
      size,
      mimeType
    };

    // Cache for future use
    await env.FILES_KV.put(`stream_${hash}`, JSON.stringify(fileInfo), { expirationTtl: 3600 });

    return fileInfo;

  } catch (error) {
    console.error('❌ Hash decode error:', error);
    return null;
  }
}