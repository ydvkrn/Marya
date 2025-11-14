// functions/btfstorage/file/[id].js

const MIME_TYPES = {
  'mp4': 'video/mp4',
  'mkv': 'video/x-matroska',
  'avi': 'video/x-msvideo',
  'mov': 'video/quicktime',
  'webm': 'video/webm',
  'mp3': 'audio/mpeg',
  'jpg': 'image/jpeg',
  'jpeg': 'image/jpeg',
  'png': 'image/png',
  'pdf': 'application/pdf'
};

export async function onRequest(context) {
  const { request, env, params } = context;
  
  if (request.method === 'OPTIONS') {
    return new Response(null, {
      status: 204,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, HEAD, OPTIONS',
        'Access-Control-Allow-Headers': 'Range'
      }
    });
  }

  try {
    let fileId = params.id;
    let actualId = fileId;
    let extension = '';

    if (fileId.includes('.')) {
      const parts = fileId.split('.');
      extension = parts.pop().toLowerCase();
      actualId = parts.join('.');
    }

    const metadataString = await env.FILES_KV.get(actualId);
    
    if (!metadataString) {
      return new Response('File not found', { status: 404 });
    }

    const metadata = JSON.parse(metadataString);
    const mimeType = metadata.contentType || MIME_TYPES[extension] || 'application/octet-stream';

    if (metadata.telegramFileId && (!metadata.chunks || metadata.chunks.length === 0)) {
      return streamSingleFile(request, env, metadata, mimeType);
    }

    if (metadata.chunks && metadata.chunks.length > 0) {
      return streamChunkedFile(request, env, metadata, mimeType);
    }

    return new Response('Invalid file', { status: 400 });

  } catch (error) {
    return new Response('Error: ' + error.message, { status: 500 });
  }
}

async function streamSingleFile(request, env, metadata, mimeType) {
  const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);

  for (const botToken of botTokens) {
    try {
      const getFileRes = await fetch(
        `https://api.telegram.org/bot${botToken}/getFile?file_id=${metadata.telegramFileId}`
      );

      const fileData = await getFileRes.json();
      
      if (!fileData.ok || !fileData.result || !fileData.result.file_path) {
        continue;
      }

      const directUrl = `https://api.telegram.org/file/bot${botToken}/${fileData.result.file_path}`;
      
      const headers = {};
      const rangeHeader = request.headers.get('Range');
      if (rangeHeader) {
        headers['Range'] = rangeHeader;
      }

      const telegramRes = await fetch(directUrl, { headers });

      if (!telegramRes.ok) {
        continue;
      }

      const responseHeaders = new Headers();
      responseHeaders.set('Content-Type', mimeType);
      responseHeaders.set('Accept-Ranges', 'bytes');
      responseHeaders.set('Access-Control-Allow-Origin', '*');
      responseHeaders.set('Cache-Control', 'public, max-age=86400');

      const contentLength = telegramRes.headers.get('content-length');
      const contentRange = telegramRes.headers.get('content-range');
      
      if (contentLength) {
        responseHeaders.set('Content-Length', contentLength);
      }
      if (contentRange) {
        responseHeaders.set('Content-Range', contentRange);
      }

      return new Response(telegramRes.body, {
        status: telegramRes.status,
        headers: responseHeaders
      });

    } catch (e) {
      continue;
    }
  }

  return new Response('All servers failed', { status: 503 });
}

async function streamChunkedFile(request, env, metadata, mimeType) {
  const chunks = metadata.chunks;
  const totalSize = metadata.size;
  const rangeHeader = request.headers.get('Range');

  if (rangeHeader) {
    return streamRange(request, env, metadata, rangeHeader, mimeType);
  }

  return streamFull(env, chunks, mimeType, totalSize);
}

async function streamFull(env, chunks, mimeType, totalSize) {
  let index = 0;

  const stream = new ReadableStream({
    async pull(controller) {
      if (index >= chunks.length) {
        controller.close();
        return;
      }

      try {
        const chunkData = await getChunk(env, chunks[index]);
        controller.enqueue(new Uint8Array(chunkData));
        index++;
      } catch (e) {
        controller.error(e);
      }
    }
  });

  return new Response(stream, {
    status: 200,
    headers: {
      'Content-Type': mimeType,
      'Content-Length': totalSize.toString(),
      'Accept-Ranges': 'bytes',
      'Access-Control-Allow-Origin': '*',
      'Cache-Control': 'public, max-age=86400'
    }
  });
}

async function streamRange(request, env, metadata, rangeHeader, mimeType) {
  const totalSize = metadata.size;
  const chunks = metadata.chunks;
  const chunkSize = metadata.chunkSize || 20971520;

  const match = rangeHeader.match(/bytes=(d+)-(d*)/);
  
  if (!match) {
    return new Response('Invalid range', { status: 416 });
  }

  const start = parseInt(match[1], 10);
  let end = match[2] ? parseInt(match[2], 10) : totalSize - 1;

  if (end >= totalSize) {
    end = totalSize - 1;
  }

  if (start >= totalSize || start > end) {
    return new Response('Range not satisfiable', { status: 416 });
  }

  const requestedSize = end - start + 1;
  const startChunk = Math.floor(start / chunkSize);
  const endChunk = Math.floor(end / chunkSize);
  const neededChunks = chunks.slice(startChunk, endChunk + 1);

  let currentPos = startChunk * chunkSize;
  let idx = 0;

  const stream = new ReadableStream({
    async pull(controller) {
      if (idx >= neededChunks.length) {
        controller.close();
        return;
      }

      try {
        const chunkData = await getChunk(env, neededChunks[idx]);
        const arr = new Uint8Array(chunkData);

        const chunkStart = Math.max(start - currentPos, 0);
        const chunkEnd = Math.min(arr.length, end - currentPos + 1);

        if (chunkStart < chunkEnd) {
          controller.enqueue(arr.slice(chunkStart, chunkEnd));
        }

        currentPos += chunkSize;
        idx++;
      } catch (e) {
        controller.error(e);
      }
    }
  });

  return new Response(stream, {
    status: 206,
    headers: {
      'Content-Type': mimeType,
      'Content-Length': requestedSize.toString(),
      'Content-Range': `bytes ${start}-${end}/${totalSize}`,
      'Accept-Ranges': 'bytes',
      'Access-Control-Allow-Origin': '*',
      'Cache-Control': 'public, max-age=86400'
    }
  });
}

async function getChunk(env, chunkInfo) {
  const kvNamespace = env[chunkInfo.kvNamespace] || env.FILES_KV;
  const chunkKey = chunkInfo.keyName || chunkInfo.chunkKey;

  const metaStr = await kvNamespace.get(chunkKey);
  
  if (!metaStr) {
    throw new Error('Chunk not found');
  }

  const chunkMeta = JSON.parse(metaStr);
  const fileId = chunkMeta.telegramFileId || chunkMeta.fileIdCode;

  if (chunkMeta.directUrl) {
    try {
      const res = await fetch(chunkMeta.directUrl);
      if (res.ok) {
        return res.arrayBuffer();
      }
    } catch (e) {
      // Continue to refresh
    }
  }

  const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);

  for (const botToken of botTokens) {
    try {
      const getFileRes = await fetch(
        `https://api.telegram.org/bot${botToken}/getFile?file_id=${fileId}`
      );

      const fileData = await getFileRes.json();
      
      if (!fileData.ok || !fileData.result || !fileData.result.file_path) {
        continue;
      }

      const freshUrl = `https://api.telegram.org/file/bot${botToken}/${fileData.result.file_path}`;
      const res = await fetch(freshUrl);

      if (res.ok) {
        kvNamespace.put(chunkKey, JSON.stringify({
          ...chunkMeta,
          directUrl: freshUrl
        })).catch(() => {});

        return res.arrayBuffer();
      }
    } catch (e) {
      continue;
    }
  }

  throw new Error('All bots failed');
}