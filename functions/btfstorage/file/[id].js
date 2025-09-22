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
  'm3u8': 'application/vnd.apple.mpegurl',
  'ts': 'video/mp2t'
};

export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.id;

  if (request.method === 'OPTIONS') {
    const headers = new Headers();
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Access-Control-Allow-Methods', 'GET, OPTIONS');
    headers.set('Access-Control-Allow-Headers', 'Range, Content-Type');
    headers.set('Access-Control-Max-Age', '86400');
    return new Response(null, { status: 204, headers });
  }

  try {
    let actualId = fileId;
    let extension = '';
    let isHlsPlaylist = false;
    let isHlsSegment = false;
    let segmentIndex = -1;

    if (fileId.includes('.')) {
      const parts = fileId.split('.');
      extension = parts.pop().toLowerCase();
      actualId = parts.join('.');
      if (extension === 'm3u8') isHlsPlaylist = true;
      else if (extension === 'ts' && actualId.includes('-')) {
        const segParts = actualId.split('-');
        const last = parseInt(segParts[segParts.length-1]);
        if (!isNaN(last)) {
          segmentIndex = last;
          segParts.pop();
          actualId = segParts.join('-');
          isHlsSegment = true;
        }
      }
    }

    const metadataString = await env.FILES_KV.get(actualId);
    if (!metadataString) return new Response('File not found', { status: 404 });
    const metadata = JSON.parse(metadataString);

    const mimeType = MIME_TYPES[extension] || 'application/octet-stream';

    if (isHlsPlaylist) return await handleHlsPlaylist(request, env, metadata, actualId);
    if (isHlsSegment && segmentIndex >= 0) return await handleHlsSegment(request, env, metadata, segmentIndex);
    if (metadata.telegramFileId && !metadata.chunks) return await handleSingleFile(request, env, metadata, mimeType);
    if (metadata.chunks && metadata.chunks.length > 0) return await handleChunkedFile(request, env, metadata, mimeType, extension);

    return new Response('Invalid file format', { status: 400 });

  } catch (err) {
    console.error('Streaming error:', err);
    return new Response(`Streaming error: ${err.message}`, { status: 500 });
  }
}

// HLS Playlist
async function handleHlsPlaylist(request, env, metadata, actualId) {
  if (!metadata.chunks || metadata.chunks.length === 0) 
    return new Response('HLS not supported', { status: 400 });

  const baseUrl = new URL(request.url).origin;
  const segmentDuration = 5;
  let playlist = '#EXTM3U\n#EXT-X-VERSION:3\n';
  playlist += `#EXT-X-TARGETDURATION:${segmentDuration}\n`;
  playlist += '#EXT-X-MEDIA-SEQUENCE:0\n#EXT-X-PLAYLIST-TYPE:VOD\n';
  metadata.chunks.forEach((_, i) => {
    playlist += `#EXTINF:${segmentDuration.toFixed(1)},\n${baseUrl}/${actualId}-${i}.ts\n`;
  });
  playlist += '#EXT-X-ENDLIST\n';

  const headers = new Headers();
  headers.set('Content-Type', 'application/vnd.apple.mpegurl');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Access-Control-Allow-Headers', 'Range, Content-Type');
  headers.set('Cache-Control', 'no-cache');
  return new Response(playlist, { status: 200, headers });
}

// HLS Segment
async function handleHlsSegment(request, env, metadata, segmentIndex) {
  if (!metadata.chunks || segmentIndex >= metadata.chunks.length) 
    return new Response('Segment not found', { status: 404 });

  const chunkData = await loadSingleChunk(env, metadata.chunks[segmentIndex]);
  const headers = new Headers();
  headers.set('Content-Type', 'video/mp2t');
  headers.set('Content-Length', chunkData.byteLength.toString());
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Access-Control-Allow-Headers', 'Range, Content-Type');
  headers.set('Cache-Control', 'public, max-age=31536000');
  return new Response(chunkData, { status: 200, headers });
}

// Single Telegram file
async function handleSingleFile(request, env, metadata, mimeType) {
  const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);
  for (const botToken of botTokens) {
    try {
      const res = await fetch(`https://api.telegram.org/bot${botToken}/getFile?file_id=${metadata.telegramFileId}`);
      const data = await res.json();
      if (!data.ok || !data.result?.file_path) continue;
      const url = `https://api.telegram.org/file/bot${botToken}/${data.result.file_path}`;
      const fileRes = await fetch(url, { headers: request.headers.get('Range') ? { 'Range': request.headers.get('Range') } : {} });
      if (!fileRes.ok) continue;

      const headers = new Headers(fileRes.headers);
      headers.set('Content-Type', mimeType);
      headers.set('Access-Control-Allow-Origin', '*');
      headers.set('Access-Control-Allow-Headers', 'Range, Content-Type');
      headers.set('Cache-Control', 'public, max-age=31536000');
      headers.set('Content-Disposition', request.url.includes('dl') ? `attachment; filename="${metadata.filename}"` : 'inline');

      return new Response(fileRes.body, { status: fileRes.status, headers });
    } catch {}
  }
  return new Response('All streaming servers failed', { status: 503 });
}

// Chunked file streaming (Range + instant play)
async function handleChunkedFile(request, env, metadata, mimeType, extension) {
  const range = request.headers.get('Range');
  if (range) return handleSmartRange(request, env, metadata, range, mimeType);
  if (request.url.includes('dl')) return handleFullStreamDownload(request, env, metadata, mimeType);
  return handleInstantPlay(request, env, metadata, mimeType, metadata.size);
}

// Smart range streaming
async function handleSmartRange(request, env, metadata, rangeHeader, mimeType) {
  const size = metadata.size;
  const chunks = metadata.chunks;
  const chunkSize = metadata.chunkSize || 20*1024*1024;
  const match = rangeHeader.match(/bytes=(\d+)-(\d*)/);
  if (!match) return new Response('Invalid range', { status: 416 });

  const start = parseInt(match[1]), end = match[2] ? parseInt(match[2]) : size-1;
  if (start >= size) return new Response('Range not satisfiable', { status: 416, headers: { 'Content-Range': `bytes */${size}` } });

  const startChunk = Math.floor(start / chunkSize), endChunk = Math.floor(end / chunkSize);
  const neededChunks = chunks.slice(startChunk, endChunk+1);
  let currentPosition = startChunk*chunkSize;

  const stream = new ReadableStream({
    async pull(controller) {
      for (let i=0;i<neededChunks.length;i++) {
        const chunkData = await loadSingleChunk(env, neededChunks[i]);
        const uint8 = new Uint8Array(chunkData);
        const s = Math.max(start-currentPosition,0);
        const e = Math.min(uint8.length, end-currentPosition+1);
        if (s<e) controller.enqueue(uint8.slice(s,e));
        currentPosition += chunkSize;
      }
      controller.close();
    }
  });

  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', (end-start+1).toString());
  headers.set('Content-Range', `bytes ${start}-${end}/${size}`);
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Access-Control-Allow-Headers', 'Range, Content-Type');
  headers.set('Content-Disposition', 'inline');
  headers.set('Cache-Control', 'public, max-age=31536000');

  return new Response(stream, { status: 206, headers });
}

// Instant play buffer-first
async function handleInstantPlay(request, env, metadata, mimeType, totalSize) {
  const chunks = metadata.chunks;
  const buffers = [];
  let loadedBytes = 0, chunkIndex = 0;
  const maxBytes = 50*1024*1024;

  while(chunkIndex<chunks.length && loadedBytes<maxBytes){
    const data = await loadSingleChunk(env,chunks[chunkIndex]);
    const uint8 = new Uint8Array(data);
    buffers.push(uint8);
    loadedBytes += uint8.byteLength;
    chunkIndex++;
  }

  const stream = new ReadableStream({
    pull(controller){
      buffers.forEach(b=>controller.enqueue(b));
      controller.close();
    }
  });

  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', loadedBytes.toString());
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin','*');
  headers.set('Access-Control-Allow-Headers','Range, Content-Type');
  headers.set('Content-Disposition','inline');
  headers.set('Cache-Control','public, max-age=31536000');

  return new Response(stream,{status:200,headers});
}

// Load chunk helper
async function loadSingleChunk(env, chunkInfo) {
  const kvNamespace = env[chunkInfo.kvNamespace] || env.FILES_KV;
  const chunkKey = chunkInfo.keyName || chunkInfo.chunkKey;
  const metadataString = await kvNamespace.get(chunkKey);