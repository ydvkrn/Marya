import { MAX_MB, BOT_TOKEN, CHAT_ID } from '../../_config.js';

const BYTES = MAX_MB * 1024 * 1024;
const J = (o,s=200)=>new Response(JSON.stringify(o),{status:s,headers:{'Content-Type':'application/json'}});

export async function onRequest({request,env}) {
  const kv = env.FILES_KV, url = new URL(request.url);

  // ---------- upload via URL ----------
  if (url.searchParams.has('src')) {
    const src=url.searchParams.get('src');
    const r = await fetch(src); if(!r.ok) return J(err('fetch failed'),502);
    const buf = await r.arrayBuffer(); if(buf.byteLength>BYTES) return J(err('>25 MB'),413);
    const ctype = r.headers.get('content-type')||'application/octet-stream';
    return await handleBuffer(buf,getName(src),ctype,kv,url.origin);
  }

  // ---------- upload via form ----------
  if (request.method==='POST') {
    const fd=await request.formData(); const file=fd.get('file');
    if(!file) return J(err('no file'),400);
    if(file.size>BYTES) return J(err('>25 MB'),413);
    return await handleBuffer(await file.arrayBuffer(),file.name,file.type,kv,new URL(request.url).origin);
  }

  return J(err('method'),405);
}

/*  CORE  ------------------------------------------------------------------ */
async function handleBuffer(buf,origName,ctype,kv,origin){
  // 1. send to Telegram
  const tgRes = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/sendDocument`,{
    method:'POST',
    body:buildForm(buf,origName,ctype,CHAT_ID)
  }).then(r=>r.json());
  if(!tgRes.ok) return J(err('TG error'),502);

  // 2. get CDN link
  const fileId = tgRes.result.document.file_id;
  const info   = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/getFile?file_id=${fileId}`)
                  .then(r=>r.json());
  const tgURL  = `https://api.telegram.org/file/bot${BOT_TOKEN}/${info.result.file_path}`;

  // 3. create slug
  const slug = crypto.randomUUID().slice(0,8)+'-'+clean(origName)+'.'+ext(origName,ctype);

  // 4. store redirect in KV
  await kv.put(slug,tgURL,{metadata:{filename:origName,size:buf.byteLength,ctype}});

  return J({
    success:true,filename:origName,size:buf.byteLength,media_type:ctype,
    preview_url:`${origin}/m/${slug}`,stream_url:`${origin}/m/${slug}`,
    download_url:`${origin}/m/${slug}?dl=1`
  });
}

const buildForm = (buf,name,type,chat) =>{
  const f = new FormData();
  f.append('chat_id',chat);
  f.append('caption',name);
  f.append('document',new File([buf],name,{type}));
  return f;
};
const clean = n=>n.replace(/\.[^.]+$/,'').replace(/[^a-z0-9]+/gi,'-').toLowerCase();
const ext   = (n,t)=> n.split('.').pop() || (t||'bin').split('/').pop();
const err   = m=>({success:false,error:m});
const getName = u=>decodeURIComponent(u.split('/').pop().split('?')[0]||'file');
