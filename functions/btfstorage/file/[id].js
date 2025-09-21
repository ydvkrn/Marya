/*  pages/functions/btfstorage/file/[id].js  */

/* ───────────────── MIME MAP ───────────────── */
const MIME = Object.fromEntries(
  ['mp4','webm','mkv','avi','mov','m4v','wmv','flv','3gp']
    .map(e=>[e,'video/mp4'])
    .concat([['webm','video/webm'],['mp3','audio/mpeg'],['wav','audio/wav'],
             ['aac','audio/mp4'],['m4a','audio/mp4'],['ogg','audio/ogg'],
             ['jpg','image/jpeg'],['jpeg','image/jpeg'],['png','image/png'],
             ['gif','image/gif'],['webp','image/webp']])
);

/* ───────── helpers ───────── */
const mimeOf = f => MIME[(f||'').split('.').pop().toLowerCase()]||'application/octet-stream';
const INITIAL_RANGE = 4*1024*1024;          // 4 MB kick-start
const pickBot = env => env.BOT_TOKEN||env.BOT_TOKEN2||env.BOT_TOKEN3||env.BOT_TOKEN4;

/* ───────── entry ───────── */
export async function onRequest({ request, env, params }){
  const idWithExt = params.id;
  const id        = idWithExt.includes('.') ? idWithExt.slice(0,idWithExt.lastIndexOf('.')) : idWithExt;
  if(!id.startsWith('MSM')) return new Response('404',{status:404});

  const metaStr = await env.FILES_KV.get(id);
  if(!metaStr) return new Response('404',{status:404});
  const meta = JSON.parse(metaStr);               // size, filename, telegramFileId?, chunks[]
  const mime = mimeOf(meta.filename||idWithExt);
  const url  = new URL(request.url);
  const dl   = url.searchParams.get('dl')==='1';
  const rangeHdr = request.headers.get('Range');

  /* 1️⃣ Single-file pass-through */
  if(meta.telegramFileId && (!meta.chunks||meta.chunks.length<=1)){
    return proxyTelegram(request, env, meta.telegramFileId, mime);
  }

  /* 2️⃣ Chunked path */
  const size      = meta.size;
  const chunks    = meta.chunks;
  const perChunk  = meta.chunkSize || Math.ceil(size/chunks.length);

  // force browser into Range mode for big media
  if(!rangeHdr && !dl && size>INITIAL_RANGE){
    return serveRange(0, INITIAL_RANGE-1);
  }
  // normal seek/play ranges
  if(rangeHdr && !dl){
    const r = parseRange(rangeHdr,size);
    if(!r) return rngErr(size);
    return serveRange(r.start,r.end);
  }
  // big download? start with 4 MB 206 so client continues with ranges
  if(dl && chunks.length>45){
    return serveRange(0, INITIAL_RANGE-1, true);
  }
  // small file (<=45 chunks) → sequential stream
  return streamAll(chunks, mime, size, dl?meta.filename:null);

  /* ───────── implementations ───────── */
  function rngErr(sz){return new Response('416',{status:416,headers:{'Content-Range':`bytes */${sz}`,'Accept-Ranges':'bytes'}});}
  function parseRange(h,sz){
    const m=h.match(/bytes=(\d+)-(\d*)/); if(!m)return null;
    let s=+m[1],e=m[2]?Math.min(sz-1,+m[2]):sz-1;
    return (s>=sz||s>e)?null:{start:s,end:e};
  }

  async function serveRange(start,end,forceAtt=false){
    const sIdx=Math.floor(start/perChunk), eIdx=Math.floor(end/perChunk);
    const needed=chunks.slice(sIdx,eIdx+1);
    const bufParts=[];
    for(const info of needed) bufParts.push(new Uint8Array(await fetchChunk(info)));
    const total=bufParts.reduce((a,b)=>a+b.byteLength,0);
    const combo=new Uint8Array(total);
    let off=0; for(const p of bufParts){combo.set(p,off);off+=p.byteLength;}
    const slice=combo.subarray(start - sIdx*perChunk, start - sIdx*perChunk + (end-start+1));

    const h=new Headers({'Content-Type':mime,'Content-Length':slice.byteLength,
      'Content-Range':`bytes ${start}-${end}/${size}`,'Accept-Ranges':'bytes',
      'Access-Control-Allow-Origin':'*'});
    if(forceAtt) h.set('Content-Disposition',`attachment; filename="${meta.filename}"`);
    return new Response(slice.buffer,{status:206,headers:h});
  }

  async function streamAll(list,mtype,tot,fname){
    const rs=new ReadableStream({
      async start(c){for(const x of list){c.enqueue(new Uint8Array(await fetchChunk(x)));await new Promise(r=>setTimeout(r,8));}c.close();}
    });
    const h=new Headers({'Content-Type':mtype,'Content-Length':tot,'Accept-Ranges':'bytes','Access-Control-Allow-Origin':'*'});
    if(fname) h.set('Content-Disposition',`attachment; filename="${fname}"`);
    return new Response(rs,{headers:h});
  }

  async function fetchChunk(info){
    const kv=env[info.kvNamespace]||env.FILES_KV;
    const m=JSON.parse(await kv.get(info.keyName));
    let r=await fetch(m.directUrl,{signal:AbortSignal.timeout(45000)});
    if(r.ok) return r.arrayBuffer();

    // refresh once
    const bot=pickBot(env);
    const path=await fetch(`https://api.telegram.org/bot${bot}/getFile?file_id=${encodeURIComponent(m.telegramFileId)}`,{signal:AbortSignal.timeout(15000)}).then(x=>x.json()).then(j=>j.result.file_path);
    const fresh=`https://api.telegram.org/file/bot${bot}/${path}`;
    r=await fetch(fresh,{signal:AbortSignal.timeout(45000)});
    if(!r.ok) throw new Error('chunk fetch fail');
    kv.put(info.keyName,JSON.stringify({...m,directUrl:fresh,lastRefreshed:Date.now()})).catch(()=>{});
    return r.arrayBuffer();
  }

  async function proxyTelegram(req,env,fileId,mtype){
    const bot=pickBot(env);
    const p=await fetch(`https://api.telegram.org/bot${bot}/getFile?file_id=${encodeURIComponent(fileId)}`,{signal:AbortSignal.timeout(15000)}).then(r=>r.json()).then(j=>j.result.file_path);
    const tg=await fetch(`https://api.telegram.org/file/bot${bot}/${p}`,{headers:req.headers.has('Range')?{Range:req.headers.get('Range')}:{}});
    const h=new Headers(tg.headers); h.set('Content-Type',mtype); h.set('Access-Control-Allow-Origin','*');
    return new Response(tg.body,{status:tg.status,headers:h});
  }
}
