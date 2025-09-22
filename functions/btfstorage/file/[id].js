const MIME = {
  'mp4':'video/mp4', 'mkv':'video/mp4', 'webm':'video/webm', 'mp3':'audio/mpeg','wav':'audio/wav','jpg':'image/jpeg','jpeg':'image/jpeg','png':'image/png'
};

export async function onRequest({ request, env, params }) {
  const id = params.id.includes('.') ? params.id.split('.')[0] : params.id;
  const ext = params.id.includes('.') ? params.id.split('.').pop() : '';
  const metaStr = await env.FILES_KV.get(id);
  if (!metaStr) return new Response('404',{status:404});
  const meta = JSON.parse(metaStr);
  const mime = MIME[ext] || 'application/octet-stream';
  const range = request.headers.get('Range');
  
  // Single Telegram doc direct-proxy
  if (meta.telegramFileId && !meta.chunks)
    return proxyTG(request, env, meta.telegramFileId, mime, meta.filename);

  // Chunked file, Range support (max 4 chunks at a time)
  if (meta.chunks) {
    const size = meta.size;
    const chunkSize = meta.chunkSize || Math.ceil(size / meta.chunks.length);
    if (!range) return serveInitialChunks(env, meta, mime, size);
    // Parse and serve needed range (max 4 chunks)
    return serveRange(env, meta, mime, size, range, chunkSize);
  }
  return new Response('Invalid file',{status:400});
}

async function proxyTG(request, env, telegramFileId, mime, filename) {
  const bot=env.BOT_TOKEN;
  const gf=await fetch(`https://api.telegram.org/bot${bot}/getFile?file_id=${encodeURIComponent(telegramFileId)}`).then(r=>r.json());
  const direct=`https://api.telegram.org/file/bot${bot}/${gf.result.file_path}`;
  const res=await fetch(direct, { headers: request.headers.get('Range') ? {Range:request.headers.get('Range')} : {} });
  const hd = new Headers(res.headers);
  hd.set('Content-Type', mime);
  hd.set('Access-Control-Allow-Origin','*');
  hd.set('Accept-Ranges','bytes');
  hd.set('Content-Disposition','inline');
  return new Response(res.body, { status: res.status, headers: hd });
}

async function serveInitialChunks(env, meta, mime, size) {
  const initialChunks = meta.chunks.slice(0,3);
  let bufs = []; let total = 0;
  for (const c of initialChunks) {
    const kv = env[c.kvNamespace] || env.FILES_KV;
    const m = JSON.parse(await kv.get(c.keyName));
    const part = await fetch(m.directUrl).then(r=>r.arrayBuffer());
    bufs.push(new Uint8Array(part)); total += part.byteLength;
  }
  const buf = new Uint8Array(total); let off = 0; for (const b of bufs) { buf.set(b,off); off+=b.byteLength;}
  const h=new Headers({'Content-Type':mime,'Content-Length':buf.length,'Content-Range':`bytes 0-${buf.length-1}/${size}`,'Accept-Ranges':'bytes','Access-Control-Allow-Origin':'*','Content-Disposition':'inline'});
  return new Response(buf, {status:206, headers:h});
}

async function serveRange(env, meta, mime, size, range, chunkSize) {
  const m=range.match(/bytes=(d+)-(d*)/); if(!m) return new Response('Invalid range',{status:416});
  let start=+m[1], end=m[2]?Math.min(size-1,+m[2]):size-1;
  const sIdx=Math.floor(start/chunkSize), eIdx=Math.floor(end/chunkSize);
  const needed=meta.chunks.slice(sIdx,eIdx+1).slice(0,4); // max 4
  let bufs=[],total=0; for(const c of needed){const kv=env[c.kvNamespace]||env.FILES_KV;const m=JSON.parse(await kv.get(c.keyName)); const pb=await fetch(m.directUrl).then(r=>r.arrayBuffer()); bufs.push(new Uint8Array(pb));total+=pb.byteLength;}
  const combo = new Uint8Array(total); let off=0;for(const b of bufs){combo.set(b,off);off+=b.byteLength;}
  const rangeStartInBuffer=start-(sIdx*chunkSize),requestedSize=end-start+1;
  const exact=combo.slice(rangeStartInBuffer,rangeStartInBuffer+requestedSize);
  const hd=new Headers({'Content-Type':mime,'Content-Length':exact.length,'Content-Range':`bytes ${start}-${end}/${size}`,'Accept-Ranges':'bytes','Access-Control-Allow-Origin':'*','Content-Disposition':'inline'});
  return new Response(exact, {status:206,headers:hd});
}