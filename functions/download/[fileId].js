// SAFONE.CO STYLE DIRECT DOWNLOAD - INSTANT SPEED

const MIME_TYPES = {
  'mp4': 'video/mp4', 'mkv': 'video/mp4', 'avi': 'video/mp4', 'mov': 'video/mp4',
  'm4v': 'video/mp4', 'wmv': 'video/mp4', 'flv': 'video/mp4', '3gp': 'video/mp4',
  'webm': 'video/webm', 'mp3': 'audio/mpeg', 'wav': 'audio/wav', 'aac': 'audio/mp4'
};

export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.fileId;

  console.log('📥 SAFONE STYLE DOWNLOAD:', fileId);

  try {
    // Get hash from URL params
    const url = new URL(request.url);
    const hash = url.searchParams.get('hash');
    
    if (!hash) {
      return new Response('Invalid download link', { status: 400 });
    }

    // Decode to get file info
    const fileInfo = await decodeFileId(env, fileId, hash);
    if (!fileInfo) {
      return new Response('File not found', { status: 404 });
    }

    const { telegramFileId, filename, size, mimeType } = fileInfo;

    console.log(`📥 Download: ${filename} (${Math.round(size/1024/1024)}MB)`);

    // DIRECT TELEGRAM DOWNLOAD (like safone.co)
    return await directTelegramDownload(request, env, telegramFileId, filename, mimeType);

  } catch (error) {
    console.error('❌ Download error:', error);
    return new Response(`Download error: ${error.message}`, { status: 500 });
  }
}

// Direct Telegram download
async function directTelegramDownload(request, env, telegramFileId, filename, mimeType) {
  const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);
  
  for (const botToken of botTokens) {
    try {
      console.log(`📥 Getting download URL with bot: ${botToken.slice(-4)}`);

      // Get fresh Telegram URL
      const getFileResponse = await fetch(
        `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(telegramFileId)}`,
        { signal: AbortSignal.timeout(8000) }
      );

      if (!getFileResponse.ok) continue;

      const getFileData = await getFileResponse.json();
      if (!getFileData.ok || !getFileData.result?.file_path) continue;

      const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

      // PROXY DOWNLOAD
      const telegramResponse = await fetch(directUrl, {
        headers: request.headers.get('Range') ? { 'Range': request.headers.get('Range') } : {},
        signal: AbortSignal.timeout(60000)
      });

      if (!telegramResponse.ok) continue;

      // Download headers
      const headers = new Headers();
      
      // Copy size headers
      if (telegramResponse.headers.get('content-length')) {
        headers.set('Content-Length', telegramResponse.headers.get('content-length'));
      }
      if (telegramResponse.headers.get('content-range')) {
        headers.set('Content-Range', telegramResponse.headers.get('content-range'));
      }
      
      // Set download headers
      headers.set('Content-Type', mimeType);
      headers.set('Content-Disposition', `attachment; filename="${filename}"`);
      headers.set('Accept-Ranges', 'bytes');
      headers.set('Access-Control-Allow-Origin', '*');
      headers.set('Cache-Control', 'public, max-age=3600');

      console.log(`📥 DOWNLOAD LIVE: ${filename} (Status: ${telegramResponse.status})`);

      return new Response(telegramResponse.body, {
        status: telegramResponse.status,
        headers: headers
      });

    } catch (botError) {
      console.error(`❌ Download bot ${botToken.slice(-4)} failed:`, botError.message);
      continue;
    }
  }

  return new Response('All download servers failed', { status: 503 });
}

// Decode file ID and hash
async function decodeFileId(env, fileId, hash) {
  try {
    // Check cache first
    const cacheKey = `dl_${fileId}_${hash}`;
    const cached = await env.FILES_KV.get(cacheKey);
    if (cached) {
      return JSON.parse(cached);
    }

    // Get from original file system
    const originalId = fileId.includes('.') ? fileId.substring(0, fileId.lastIndexOf('.')) : fileId;
    const extension = fileId.includes('.') ? fileId.substring(fileId.lastIndexOf('.')) : '';
    
    // Try MSM system first
    if (originalId.startsWith('MSM')) {
      const metadata = await env.FILES_KV.get(originalId);
      if (metadata) {
        const data = JSON.parse(metadata);
        const fileInfo = {
          telegramFileId: data.telegramFileId,
          filename: data.filename,
          size: data.size,
          mimeType: MIME_TYPES[extension.toLowerCase().replace('.', '')] || 'application/octet-stream'
        };
        
        // Cache it
        await env.FILES_KV.put(cacheKey, JSON.stringify(fileInfo), { expirationTtl: 3600 });
        return fileInfo;
      }
    }

    // Fallback: Decode hash
    const decoded = Buffer.from(hash, 'base64').toString('utf-8');
    const [telegramFileId, filename] = decoded.split('|');
    
    if (!telegramFileId) return null;

    const fileInfo = {
      telegramFileId,
      filename: filename || fileId,
      size: 0,
      mimeType: MIME_TYPES[extension.toLowerCase().replace('.', '')] || 'application/octet-stream'
    };

    return fileInfo;

  } catch (error) {
    console.error('❌ File decode error:', error);
    return null;
  }
}