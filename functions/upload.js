import { BOT_TOKEN, CHANNEL_ID, MAX_SIZE, CHUNK } from './_config.js';

const cors = { 'Access-Control-Allow-Origin':'*',
               'Access-Control-Allow-Methods':'GET,POST,OPTIONS',
               'Access-Control-Allow-Headers':'Content-Type,Range,Content-Range',
               'Access-Control-Expose-Headers':'Content-Range,Content-Length,Accept-Ranges' };
const json = (o,s=200)=>new Response(JSON.stringify(o),{status:s,
               headers:{'Content-Type':'application/json',...cors}});

export async function onRequest({request,env}) {
  if (request.method==='OPTIONS') return new Response(null,{headers:cors});
  const kv   = env.FILES_KV;
  const url  = new URL(request.url);

  /* ---------- via ?url=external ---------- */
  if (url.pathname==='/hosturl' && request.method==='GET'){
    const remote=url.searchParams.get('url');
    if(!remote) return json(msg('invalid url'),400);
    return await handleBuffer(await fetchBlob(remote),remote.split('/').pop(),kv,url.origin);
  }

  /* ---------- via form POST /upload ---------- */
  if (url.pathname==='/upload' && request.method==='POST'){
    const fd=await request.formData(); const file=fd.get('file');
    if(!file)         return json(msg('no file'),400);
    if(file.size>MAX_SIZE) return json(msg('>2GB'),413);
    return await handleBuffer(await file.arrayBuffer(),file.name,kv,url.origin,file.type);
  }

  return new Response('404',{status:404,headers:cors});
}

/* ---------- helpers ---------- */
async function handleBuffer(buf,name,kv,origin,ctype='application/octet-stream'){
  if(buf.byteLength>MAX_SIZE) return json(msg('>2GB'),413);

  /* choose Telegram method */
  const low = ctype.toLowerCase();
  let method='sendDocument';
  if(low.startsWith('image/') && buf.byteLength<10*1024*1024) method='sendPhoto';
  else if(low.startsWith('video/') && buf.byteLength<50*1024*1024) method='sendVideo';
  else if(low.startsWith('audio/')) method='sendAudio';

  /* build FormData, chunk not needed (2 GB max fits single sendDocument) */
  const fd=new FormData();
  fd.append('chat_id',CHANNEL_ID);
  fd.append(method.replace('send','').toLowerCase(),new File([buf],name,{type:ctype}));
  if(method==='sendVideo') fd.append('supports_streaming','true');

  const tg=`https://api.telegram.org/bot${BOT_TOKEN}/${method}`;
  const r = await fetch(tg,{method:'POST',body:fd}).then(x=>x.json());
  if(!r.ok) return json(msg(r.description||'telegram error'),502);

  /* extract file_id */
  const id = extractFileId(r.result);
  const path = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/getFile?file_id=${id}`)
                    .then(x=>x.json()).then(x=>x.result.file_path);
  const tgURL = `https://api.telegram.org/file/bot${BOT_TOKEN}/${path}`;

  /* slug + KV save */
  const slug = makeSlug(name);
  await kv.put(slug,tgURL,{metadata:{filename:name,size:buf.byteLength,ctype}});
  const base = `${origin}/m/${slug}`;
  return json({success:true,filename:name,size:buf.byteLength,media_type:ctype,
               view_url:base,download_url:base+'?dl=1',stream_url:base});
}

function extractFileId(obj){
  if(obj.document) return obj.document.file_id;
  if(obj.photo)    return obj.photo.at(-1).file_id;
  if(obj.video)    return obj.video.file_id;
  if(obj.audio)    return obj.audio.file_id;
  return null;
}
const makeSlug = n=>crypto.randomUUID().slice(0,8)+'-'+
                  n.replace(/[^a-z0-9]+/gi,'-').slice(0,40).toLowerCase();
const msg = t=>({success:false,error:t});

/* fetch external */
async function fetchBlob(u){
  const r=await fetch(u);
  if(!r.ok) throw new Error('fetch fail');
  if(+r.headers.get('content-length')>MAX_SIZE) throw new Error('>2GB');
  return await r.arrayBuffer();
}
