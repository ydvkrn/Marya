const MIME_TYPES = {
  'mp4': 'video/mp4',
  'mkv': 'video/x-matroska',
  'avi': 'video/x-msvideo',
  'mov': 'video/quicktime',
  'm4v': 'video/mp4',
  'wmv': 'video/x-ms-wmv',
  'flv': 'video/x-flv',
  '3gp': 'video/3gpp',
  'webm': 'video/webm',
  'ogv': 'video/ogg',
  'mp3': 'audio/mpeg',
  'wav': 'audio/wav',
  'aac': 'audio/mp4',
  'm4a': 'audio/mp4',
  'ogg': 'audio/ogg',
  'flac': 'audio/flac',
  'jpg': 'image/jpeg',
  'jpeg': 'image/jpeg',
  'png': 'image/png',
  'gif': 'image/gif',
  'webp': 'image/webp',
  'svg': 'image/svg+xml',
  'pdf': 'application/pdf',
  'm3u8': 'application/x-mpegURL',
  'ts': 'video/mp2t'
};

export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.id;

  console.log('🎬 TOP TIER STREAMING:', fileId);

  // Handle CORS preflight OPTIONS request
  if (request.method === 'OPTIONS') {
    const headers = new Headers();
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Access-Control-Allow-Methods', 'GET, OPTIONS');
    headers.set('Access-Control-Allow-Headers', 'Range');
    headers.set('Access-Control-Max-Age', '86400');
    return new Response(null, { status: 204, headers });
  }

  try {
    let actualId = fileId;
    let extension = '';
    let isHlsPlaylist = false;
    let isHlsSegment = false;
    let segmentIndex = -1;

    // Parse the extension and detect HLS playlist or segment
    if (fileId.includes('.')) {
      const parts = fileId.split('.');
      extension = parts.pop().toLowerCase();
      actualId = parts.join('.');

      if (extension === 'm3u8') {
        isHlsPlaylist = true;
      } else if (extension === 'ts' && actualId.includes('-')) {
        const segParts = actualId.split('-');
        if (segParts.length > 1 && !isNaN(parseInt(segParts[segParts.length - 1], 10))) {
          segmentIndex = parseInt(segParts.pop(), 10);
          actualId = segParts.join('-');
          isHlsSegment = true;
        }
      } else {
        // fallback actual ID and extension
        actualId = fileId.substring(0, fileId.lastIndexOf('.'));
        extension = fileId.substring(fileId.lastIndexOf('.') + 1).toLowerCase();
      }
    }

    // Fetch metadata from Cloudflare KV
    const metadataString = await env.FILES_KV.get(actualId);
    if (!metadataString) {
      console.error('File not found in KV:', actualId);
      return new Response('File not found', { status: 404 });
    }

    const metadata = JSON.parse(metadataString);
    if (!metadata.filename || !metadata.size) {
      console.error('Invalid metadata:', metadata);
      return new Response('Invalid file metadata', { status: 400 });
    }

    // Support backward keys
    metadata.telegramFileId = metadata.telegramFileId || metadata.fileIdCode;

    if (!metadata.telegramFileId && !metadata.chunks) {
      console.error('No telegramFileId or chunks in metadata:', metadata);
      return new Response('Missing file ID or chunks', { status: 400 });
    }

    const mimeType = metadata.contentType || MIME_TYPES[extension] || 'application/octet-stream';

    console.log(`📁 ${metadata.filename} | Size: ${Math.round(metadata.size/1024/1024)}MB | MIME: ${mimeType} | Chunks: ${metadata.chunks?.length || 0} | HLS Playlist: ${isHlsPlaylist} | HLS Segment: ${isHlsSegment} Index: ${segmentIndex}`);

    if (isHlsPlaylist) {
      return await handleHlsPlaylist(request, env, metadata, actualId);
    }

    if (isHlsSegment && segmentIndex >= 0) {
      return await handleHlsSegment(request, env, metadata, segmentIndex);
    }

    if (metadata.telegramFileId && (!metadata.chunks || metadata.chunks.length === 0)) {
      return await handleSingleFile(request, env, metadata, mimeType);
    }

    if (metadata.chunks && metadata.chunks.length > 0) {
      return await handleChunkedFile(request, env, metadata, mimeType, extension);
    }

    return new Response('Invalid file format', { status: 400 });

  } catch (error) {
    console.error('❌ Streaming error:', error);
    return new Response(`Streaming error: ${error.message}`, { status: 500 });
  }
}

async function handleHlsPlaylist(request, env, metadata, actualId) {
  if (!metadata.chunks || metadata.chunks.length === 0) {
    return new Response('HLS not supported for single files', { status: 400 });
  }

  const chunks = metadata.chunks;
  const segmentDuration = 5;
  const baseUrl = new URL(request.url).origin;

  let playlist = '#EXTM3U
';
  playlist += '#EXT-X-VERSION:3
';
  playlist += `#EXT-X-TARGETDURATION:${segmentDuration}
`;
  playlist += '#EXT-X-MEDIA-SEQUENCE:0
';
  playlist += '#EXT-X-PLAYLIST-TYPE:VOD
';

  chunks.forEach((_, i) => {
    playlist += `#EXTINF:${segmentDuration.toFixed(1)},
`;
    playlist += `${baseUrl}/btfstorage/file/${actualId}-${i}.ts
`;
  });

  playlist += '#EXT-X-ENDLIST
';

  const headers = new Headers();
  headers.set('Content-Type', 'application/x-mpegURL');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Cache-Control', 'no-cache');

  return new Response(playlist, { status: 200, headers });
}

async function handleHlsSegment(request, env, metadata, segmentIndex) {
  if (!metadata.chunks || segmentIndex >= metadata.chunks.length || segmentIndex < 0) {
    return new Response('Segment not found', { status: 404 });
  }

  try {
    const chunkInfo = metadata.chunks[segmentIndex];
    const chunkData = await loadSingleChunk(env, chunkInfo);

    const headers = new Headers();
    headers.set('Content-Type', 'video/mp2t');
    headers.set('Content-Length', chunkData.byteLength.toString());
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Cache-Control', 'public, max-age=31536000');
    headers.set('Content-Disposition', 'inline');

    return new Response(chunkData, { status: 200, headers });
  } catch (error) {
    return new Response(`Segment error: ${error.message}`, { status: 500 });
  }
}

async function handleSingleFile(request, env, metadata, mimeType) {
  const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);

  for (const botToken of botTokens) {
    try {
      const getFileResponse = await fetch(
        `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(metadata.telegramFileId)}`,
        { signal: AbortSignal.timeout(15000) }
      );

      const getFileData = await getFileResponse.json();
      if (!getFileData.ok || !getFileData.result?.file_path) {
        continue;
      }

      const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;
      const headers = request.headers.get('Range') ? { 'Range': request.headers.get('Range') } : {};

      const telegramResponse = await fetch(directUrl, { headers, signal: AbortSignal.timeout(45000) });
      if (!telegramResponse.ok) {
        continue;
      }

      const responseHeaders = new Headers();
      ['content-length', 'content-range', 'accept-ranges'].forEach(header => {
        const value = telegramResponse.headers.get(header);
        if (value) {
          responseHeaders.set(header, value);
        }
      });

      responseHeaders.set('Content-Type', mimeType);
      responseHeaders.set('Accept-Ranges', 'bytes');
      responseHeaders.set('Access-Control-Allow-Origin', '*');
      responseHeaders.set('Cache-Control', 'public, max-age=31536000');

      const url = new URL(request.url);
      if (url.searchParams.has('dl')) {
        responseHeaders.set('Content-Disposition', `attachment; filename="${metadata.filename}"`);
      } else {
        responseHeaders.set('Content-Disposition', 'inline');
      }

      return new Response(telegramResponse.body, { status: telegramResponse.status, headers: responseHeaders });
    } catch {
      continue;
    }
  }

  return new Response('All streaming servers failed', { status: 503 });
}

async function handleChunkedFile(request, env, metadata, mimeType, extension) {
  const chunks = metadata.chunks;
  const size = metadata.size;
  const chunkSize = metadata.chunkSize || 20 * 1024 * 1024;

  const range = request.headers.get('Range');
  const url = new URL(request.url);
  const isDownload = url.searchParams.has('dl');

  if (range) {
    return await handleSmartRange(request, env, metadata, range, mimeType, chunkSize, isDownload);
  }

  if (isDownload) {
    return await handleFullStreamDownload(request, env, metadata, mimeType);
  }

  return await handleInstantPlay(request, env, metadata, mimeType, size);
}

// Following functions are same as in your original code with minor improvements:
// handleInstantPlay, handleSmartRange, handleFullStreamDownload, loadSingleChunk, fetchWithRetry
// Please let me know if you want me to provide those as well with any specific improvements.