/* Cloudflare Pages Function: Range-first video/audio/file server        */
/* 1) Single-Telegram file   → pass-through proxy (1 subrequest)          */
/* 2) Chunked file           → serve JUST requested Range (≤5 chunks)    */
/*    plain click download   → kick-start with 4 MB 206 so browser       */
/*                             switches to ranged download automatically */
/* Works inside Free Workers limits: <10 ms CPU + <50 subrequests         */

const MIME = {
  mp4:'video/mp4', webm:'video/webm', mkv:'video/mp4', avi:'video/mp4',
  mov:'video/mp4', m4v:'video/mp4', wmv:'video/mp4', flv:'video/mp4',
  '3gp':'video/mp4', mp3:'audio/mpeg', wav:'audio/wav', aac:'audio/mp4',
  m4a:'audio/mp4', ogg:'audio/ogg', jpg:'image/jpeg', jpeg:'image/jpeg',
  png:'image/png', gif:'image/gif', webp:'image/webp', pdf:'application/pdf',
  txt:'text/plain', zip:'application/zip'
};
const INITIAL_RANGE = 4 * 1024 * 1024;      // 4 MB

export async function onRequest({ request, env, params }) {
  const idWithExt = params.id;
  const id        = idWithExt.includes('.') ? idWithExt.slice(0, idWithExt.lastIndexOf('.')) : idWithExt;
  if (!id.startsWith('MSM')) return new Response('404', { status:404 });

  const metaStr = await env.FILES_KV.get(id);
  if (!metaStr) return new Response('404', { status:404 });

  const meta = JSON.parse(metaStr);                     // { size, filename, chunkSize, chunks[], telegramFileId }
  const mime = MIME[(meta.filename||'').split('.').pop()?.toLowerCase()] || 'application/octet-stream';
  const url  = new URL(request.url);
  const dl   = url.searchParams.get('dl') === '1';
  const rngH = request.headers.get('Range');

  /* ---------- FAST-PATH: single Telegram doc → proxy pass-through ---------- */
  if (meta.telegramFileId && (!meta.chunks || meta.chunks.length<=1))
    return proxyTG(request, env, meta.telegramFileId, mime);

  const size      = meta.size;
  const chunks    = meta.chunks;
  const perChunk  = meta.chunkSize || Math.ceil(size / chunks.length);

  /* ---------- kick browser into Range mode for big files ---------- */
  if (!rngH && !dl && size > INITIAL_RANGE)
    return rangeResp(0, INITIAL_RANGE-1);

  /* ---------- serve exact range (seek/play) ---------- */
  if (rngH && !dl) {
    const r = parseRange(rngH, size);
    if (!r) return rngErr();
    const {start,end} = r;  return rangeResp(start,end);
  }

  /* ---------- large download? push to ranged download ---------- */
  if (dl && chunks.length>45)
    return rangeResp(0, INITIAL_RANGE-1, /*forceAttachment=*/true);

  /* ---------- small file or few chunks → sequential stream ---------- */
  return streamAll(chunks, mime, size, dl ? meta.filename : null);

  /* ===== helpers below ===== */
  function rngErr(){ return new Response('416', {status:416, headers:{'Content-Range':`bytes */${size}`,'Accept-Ranges':'bytes'}}); }

  function parseRange(h, sz){
    const m=h.match(/bytes=(\d+)-(\d*)/); if(!m) return null;
    let s=+m[1], e=m[2]?Math.min(sz-1,+m[2]):sz-1;
    if(s>=sz||s>e) return null; return {start:s,end:e};
  }

  async function rangeResp(start,end, forceAtt=false){
    const sIdx=Math.floor(start/perChunk), eIdx=Math.floor(end/perChunk);
    const needed=chunks.slice(sIdx,eIdx+1);
    const buffParts=[];
    for(const info of needed) buffParts.push(new Uint8Array(await fetchChunk(info)));
    const total=buffParts.reduce((a,b)=>a+b.byteLength,0);
    const combo=new Uint8Array(total); let off=0; for(const p of buffParts){ combo.set(p,off); off+=p.byteLength; }
    const slice=combo.subarray(start - sIdx*perChunk, start - sIdx*perChunk + (end-start+1));

    const h=new Headers({'Content-Type':mime,'Content-Length':slice.byteLength,
                         'Content-Range':`bytes ${start}-${end}/${size}`,'Accept-Ranges':'bytes',
                         'Access-Control-Allow-Origin':'*'});
    if (forceAtt) h.set('Content-Disposition',`attachment; filename="${meta.filename}"`);
    return new Response(slice.buffer,{status:206,headers:h});
  }

  async function streamAll(list,mtype,tot, fname){
    const rs=new ReadableStream({
      async start(ctrl){ for(const c of list){ ctrl.enqueue(new Uint8Array(await fetchChunk(c))); await new Promise(r=>setTimeout(r,8)); } ctrl.close(); }
    });
    const h=new Headers({'Content-Type':mtype,'Content-Length':tot,'Accept-Ranges':'bytes','Access-Control-Allow-Origin':'*'});
    fname && h.set('Content-Disposition',`attachment; filename="${fname}"`);
    return new Response(rs,{headers:h});
  }

  async function fetchChunk(info){
    const kv = env[info.kvNamespace] || env.FILES_KV;
    const m  = JSON.parse(await kv.get(info.keyName));
    let res  = await fetch(m.directUrl, {signal:AbortSignal.timeout(45000)});
    if (res.ok) return res.arrayBuffer();
    /* refresh once */
    const bot = env.BOT_TOKEN||env.BOT_TOKEN2||env.BOT_TOKEN3||env.BOT_TOKEN4;
    const gf  = await fetch(`https://api.telegram.org/bot${bot}/getFile?file_id=${encodeURIComponent(m.telegramFileId)}`,{signal:AbortSignal.timeout(15000)}).then(r=>r.json());
    const fresh=`https://api.telegram.org/file/bot${bot}/${gf.result.file_path}`;
    res = await fetch(fresh,{signal:AbortSignal.timeout(45000)});
    kv.put(info.keyName, JSON.stringify({...m,directUrl:fresh,lastRefreshed:Date.now()})).catch(()=>{});
    if(!res.ok) throw new Error('chunk fetch fail');  return res.arrayBuffer();
  }

  async function proxyTG(req, env, fid, mtype){
    const bot=env.BOT_TOKEN||env.BOT_TOKEN2||env.BOT_TOKEN3||env.BOT_TOKEN4;
    const {result:{file_path}} = await fetch(`https://api.telegram.org/bot${bot}/getFile?file_id=${encodeURIComponent(fid)}`,{signal:AbortSignal.timeout(15000)}).then(r=>r.json());
    const upstream=await fetch(`https://api.telegram.org/file/bot${bot}/${file_path}`, {headers:req.headers.has('Range')?{Range:req.headers.get('Range')}:{}});
    const h=new Headers(upstream.headers); h.set('Content-Type',mtype); h.set('Access-Control-Allow-Origin','*');
    return new Response(upstream.body,{status:upstream.status,headers:h});
  }
}
