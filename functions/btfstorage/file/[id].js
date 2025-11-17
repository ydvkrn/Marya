// functions/btfstorage/files/[id].js
// 🚀 Ultra-Fast Media Streaming - Optimized for Speed

const MIME_TYPES = {
  mp4: 'video/mp4', mkv: 'video/x-matroska', avi: 'video/x-msvideo',
  mov: 'video/quicktime', webm: 'video/webm', mp3: 'audio/mpeg',
  wav: 'audio/wav', m4a: 'audio/mp4', jpg: 'image/jpeg', png: 'image/png',
  pdf: 'application/pdf', zip: 'application/zip'
};

const MAX_PARALLEL = 3;
const CACHE_TIME = 31536000;
const TIMEOUT = 25000;
const MAX_RETRY = 3;

export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.id;
  
  if (request.method === 'OPTIONS') {
    return corsResponse();
  }
  
  try {
    let actualId = fileId;
    let extension = '';
    
    if (fileId.includes('.')) {
      const lastDot = fileId.lastIndexOf('.');
      actualId = fileId.substring(0, lastDot);
      extension = fileId.substring(lastDot + 1).toLowerCase();
    }
    
    const metaStr = await env.FILES_KV.get(actualId);
    if (!metaStr) {
      return errorResponse('File not found', 404);
    }
    
    const meta = JSON.parse(metaStr);
    if (!meta.filename || !meta.size) {
      return errorResponse('Invalid metadata', 400);
    }
    
    meta.telegramFileId = meta.telegramFileId || meta.fileIdCode;
    
    if (!meta.telegramFileId && (!meta.chunks || meta.chunks.length === 0)) {
      return errorResponse('Missing file source', 400);
    }
    
    const mime = meta.contentType || MIME_TYPES[extension] || 'application/octet-stream';
    
    if (meta.telegramFileId && (!meta.chunks || meta.chunks.length === 0)) {
      return handleSingle(request, env, meta, mime);
    }
    
    if (meta.chunks && meta.chunks.length > 0) {
      return handleChunked(request, env, meta, mime);
    }
    
    return errorResponse('Invalid file', 400);
    
  } catch (err) {
    return errorResponse('Error: ' + err.message, 500);
  }
}

function corsResponse() {
  const h = new Headers();
  h.set('Access-Control-Allow-Origin', '*');
  h.set('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
  h.set('Access-Control-Allow-Headers', 'Range, Content-Type');
  return new Response(null, { status: 204, headers: h });
}

async function handleSingle(req, env, meta, mime) {
  const bots = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);
  
  if (bots.length === 0) {
    return errorResponse('No bot tokens', 503);
  }
  
  for (let i = 0; i < bots.length; i++) {
    const bot = bots[i];
    
    try {
      const fileRes = await retryFetch(
        'https://api.telegram.org/bot' + bot + '/getFile?file_id=' + encodeURIComponent(meta.telegramFileId),
        { signal: AbortSignal.timeout(15000) }
      );
      
      const fileData = await fileRes.json();
      
      if (!fileData.ok || !fileData.result?.file_path) {
        continue;
      }
      
      const url = 'https://api.telegram.org/file/bot' + bot + '/' + fileData.result.file_path;
      
      const headers = {};
      const range = req.headers.get('Range');
      if (range) {
        headers.Range = range;
      }
      
      const tgRes = await retryFetch(url, {
        headers: headers,
        signal: AbortSignal.timeout(TIMEOUT)
      });
      
      if (!tgRes.ok) {
        continue;
      }
      
      const h = new Headers();
      ['content-length', 'content-range', 'accept-ranges'].forEach(key => {
        const val = tgRes.headers.get(key);
        if (val) h.set(key, val);
      });
      
      h.set('Content-Type', mime);
      h.set('Accept-Ranges', 'bytes');
      h.set('Access-Control-Allow-Origin', '*');
      h.set('Cache-Control', 'public, max-age=' + CACHE_TIME + ', immutable');
      
      const u = new URL(req.url);
      if (u.searchParams.has('dl') || u.searchParams.has('download')) {
        h.set('Content-Disposition', 'attachment; filename="' + meta.filename + '"');
      } else {
        h.set('Content-Disposition', 'inline');
      }
      
      return new Response(tgRes.body, {
        status: tgRes.status,
        headers: h
      });
      
    } catch (e) {
      continue;
    }
  }
  
  return errorResponse('All bots failed', 503);
}

async function handleChunked(req, env, meta, mime) {
  const chunks = meta.chunks;
  const total = meta.size;
  const chunkSize = meta.chunkSize || 20971520;
  
  const range = req.headers.get('Range');
  const u = new URL(req.url);
  const isDl = u.searchParams.has('dl') || u.searchParams.has('download');
  
  if (range) {
    return handleRange(env, meta, range, mime, chunkSize, isDl);
  }
  
  if (isDl) {
    return handleDownload(env, meta, mime);
  }
  
  return handleStream(env, meta, mime, total);
}

async function handleStream(env, meta, mime, total) {
  const chunks = meta.chunks;
  
  try {
    const max = Math.min(2, chunks.length);
    const promises = [];
    
    for (let i = 0; i < max; i++) {
      promises.push(loadChunk(env, chunks[i]));
    }
    
    const loaded = await Promise.all(promises);
    
    let bytes = 0;
    const stream = new ReadableStream({
      start(ctrl) {
        for (let i = 0; i < loaded.length; i++) {
          const arr = new Uint8Array(loaded[i]);
          ctrl.enqueue(arr);
          bytes += arr.byteLength;
        }
        ctrl.close();
      }
    });
    
    const h = new Headers();
    h.set('Content-Type', mime);
    h.set('Content-Length', Math.min(bytes, total).toString());
    h.set('Content-Range', 'bytes 0-' + (Math.min(bytes, total) - 1) + '/' + total);
    h.set('Accept-Ranges', 'bytes');
    h.set('Access-Control-Allow-Origin', '*');
    h.set('Cache-Control', 'public, max-age=' + CACHE_TIME + ', immutable');
    
    return new Response(stream, { status: 206, headers: h });
    
  } catch (e) {
    return errorResponse('Stream failed: ' + e.message, 500);
  }
}

async function handleRange(env, meta, rangeHdr, mime, chunkSize, isDl) {
  const total = meta.size;
  const chunks = meta.chunks;
  
  const match = rangeHdr.match(/bytes=(d+)-(d*)/);
  if (!match) {
    return errorResponse('Invalid range', 416, {
      'Content-Range': 'bytes */' + total
    });
  }
  
  const start = parseInt(match[1], 10);
  let end = match[2] ? parseInt(match[2], 10) : total - 1;
  
  if (end >= total) end = total - 1;
  if (start >= total || start > end) {
    return errorResponse('Range error', 416, {
      'Content-Range': 'bytes */' + total
    });
  }
  
  const size = end - start + 1;
  
  const startChunk = Math.floor(start / chunkSize);
  const endChunk = Math.floor(end / chunkSize);
  const needed = chunks.slice(startChunk, endChunk + 1);
  
  try {
    const promises = needed.map(c => loadChunk(env, c));
    const loaded = await Promise.all(promises);
    
    let pos = startChunk * chunkSize;
    const parts = [];
    
    for (let i = 0; i < loaded.length; i++) {
      const arr = new Uint8Array(loaded[i]);
      const cStart = Math.max(start - pos, 0);
      const cEnd = Math.min(arr.length, end - pos + 1);
      
      if (cStart < cEnd) {
        parts.push(arr.slice(cStart, cEnd));
      }
      
      pos += chunkSize;
      if (pos > end) break;
    }
    
    const len = parts.reduce((sum, p) => sum + p.length, 0);
    const combined = new Uint8Array(len);
    let offset = 0;
    
    for (const p of parts) {
      combined.set(p, offset);
      offset += p.length;
    }
    
    const h = new Headers();
    h.set('Content-Type', mime);
    h.set('Content-Length', size.toString());
    h.set('Content-Range', 'bytes ' + start + '-' + end + '/' + total);
    h.set('Accept-Ranges', 'bytes');
    h.set('Access-Control-Allow-Origin', '*');
    h.set('Content-Disposition', isDl ? ('attachment; filename="' + meta.filename + '"') : 'inline');
    h.set('Cache-Control', 'public, max-age=' + CACHE_TIME + ', immutable');
    
    return new Response(combined, { status: 206, headers: h });
    
  } catch (e) {
    return errorResponse('Range failed: ' + e.message, 500);
  }
}

async function handleDownload(env, meta, mime) {
  const chunks = meta.chunks;
  const total = meta.size;
  const name = meta.filename;
  
  let idx = 0;
  
  const stream = new ReadableStream({
    async pull(ctrl) {
      while (idx < chunks.length) {
        try {
          const batch = Math.min(MAX_PARALLEL, chunks.length - idx);
          const promises = [];
          
          for (let i = 0; i < batch; i++) {
            promises.push(loadChunk(env, chunks[idx + i]));
          }
          
          const loaded = await Promise.all(promises);
          
          for (let i = 0; i < loaded.length; i++) {
            const arr = new Uint8Array(loaded[i]);
            ctrl.enqueue(arr);
          }
          
          idx += batch;
          
        } catch (e) {
          ctrl.error(e);
          return;
        }
      }
      
      ctrl.close();
    }
  });
  
  const h = new Headers();
  h.set('Content-Type', mime);
  h.set('Content-Length', total.toString());
  h.set('Content-Disposition', 'attachment; filename="' + name + '"');
  h.set('Access-Control-Allow-Origin', '*');
  h.set('Cache-Control', 'public, max-age=' + CACHE_TIME + ', immutable');
  
  return new Response(stream, { status: 200, headers: h });
}

async function loadChunk(env, info) {
  const kv = env[info.kvNamespace] || env.FILES_KV;
  const key = info.keyName || info.chunkKey;
  
  const metaStr = await kv.get(key);
  if (!metaStr) {
    throw new Error('Chunk not found: ' + key);
  }
  
  const meta = JSON.parse(metaStr);
  meta.telegramFileId = meta.telegramFileId || meta.fileIdCode;
  
  if (meta.directUrl) {
    try {
      const res = await retryFetch(meta.directUrl, {
        signal: AbortSignal.timeout(TIMEOUT)
      });
      
      if (res.ok) {
        return res.arrayBuffer();
      }
    } catch (e) {
      console.log('Cache expired, refreshing...');
    }
  }
  
  const bots = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);
  
  for (let i = 0; i < bots.length; i++) {
    const bot = bots[i];
    
    try {
      const fileRes = await retryFetch(
        'https://api.telegram.org/bot' + bot + '/getFile?file_id=' + encodeURIComponent(meta.telegramFileId),
        { signal: AbortSignal.timeout(15000) }
      );
      
      const fileData = await fileRes.json();
      
      if (!fileData.ok || !fileData.result?.file_path) {
        continue;
      }
      
      const url = 'https://api.telegram.org/file/bot' + bot + '/' + fileData.result.file_path;
      
      const res = await retryFetch(url, {
        signal: AbortSignal.timeout(TIMEOUT)
      });
      
      if (res.ok) {
        kv.put(key, JSON.stringify({
          ...meta,
          directUrl: url,
          refreshed: Date.now()
        })).catch(() => {});
        
        return res.arrayBuffer();
      }
      
    } catch (e) {
      continue;
    }
  }
  
  throw new Error('All bots failed for: ' + key);
}

async function retryFetch(url, opts, retries) {
  if (!retries) retries = MAX_RETRY;
  
  for (let i = 0; i < retries; i++) {
    try {
      const res = await fetch(url, opts);
      
      if (res.ok) {
        return res;
      }
      
      if (res.status === 429) {
        const wait = parseInt(res.headers.get('Retry-After')) || Math.pow(2, i);
        await new Promise(r => setTimeout(r, wait * 1000));
        continue;
      }
      
      if (res.status >= 500 && i < retries - 1) {
        const delay = Math.min(Math.pow(2, i) * 1000, 8000);
        await new Promise(r => setTimeout(r, delay));
        continue;
      }
      
      if (res.status >= 400 && res.status < 500) {
        return res;
      }
      
    } catch (e) {
      if (i === retries - 1) {
        throw e;
      }
    }
    
    if (i < retries - 1) {
      const delay = Math.min(Math.pow(2, i) * 1000, 8000);
      await new Promise(r => setTimeout(r, delay));
    }
  }
  
  throw new Error('All retries failed');
}

function errorResponse(msg, status, extra) {
  const h = new Headers({
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    ...extra
  });
  
  const err = {
    error: msg,
    status: status || 500,
    time: new Date().toISOString()
  };
  
  return new Response(JSON.stringify(err, null, 2), {
    status: status || 500,
    headers: h
  });
}