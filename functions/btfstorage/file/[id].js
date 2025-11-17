// functions/btfstorage/files/[id].js
// Production Ready Video Streaming

const MIME_TYPES = {
mp4:'video/mp4',mkv:'video/x-matroska',avi:'video/x-msvideo',mov:'video/quicktime',
webm:'video/webm',mp3:'audio/mpeg',wav:'audio/wav',m4a:'audio/mp4',jpg:'image/jpeg',
png:'image/png',pdf:'application/pdf',zip:'application/zip'
};

export async function onRequest(ctx){
const {request,env,params}=ctx;
const fileId=params.id;

if(request.method==='OPTIONS'){
const h=new Headers();
h.set('Access-Control-Allow-Origin','*');
h.set('Access-Control-Allow-Methods','GET,HEAD,OPTIONS');
h.set('Access-Control-Allow-Headers','Range,Content-Type');
return new Response(null,{status:204,headers:h});
}

try{
let id=fileId,ext='';
if(fileId.includes('.')){
const i=fileId.lastIndexOf('.');
id=fileId.substring(0,i);
ext=fileId.substring(i+1).toLowerCase();
}

const ms=await env.FILES_KV.get(id);
if(!ms)return err('File not found',404);

const m=JSON.parse(ms);
if(!m.filename||!m.size)return err('Invalid metadata',400);

m.telegramFileId=m.telegramFileId||m.fileIdCode;

if(!m.telegramFileId&&(!m.chunks||!m.chunks.length))return err('Missing source',400);

const mime=m.contentType||MIME_TYPES[ext]||'application/octet-stream';

if(m.telegramFileId&&(!m.chunks||!m.chunks.length)){
return single(request,env,m,mime);
}

if(m.chunks&&m.chunks.length>0){
return chunked(request,env,m,mime);
}

return err('Invalid file',400);

}catch(e){
return err('Error: '+e.message,500);
}
}

async function single(req,env,m,mime){
const bots=[env.BOT_TOKEN,env.BOT_TOKEN2,env.BOT_TOKEN3,env.BOT_TOKEN4].filter(t=>t);
if(!bots.length)return err('No bots',503);

for(let i=0;i<bots.length;i++){
const bot=bots[i];

try{
const fr=await fetch('https://api.telegram.org/bot'+bot+'/getFile?file_id='+encodeURIComponent(m.telegramFileId),{signal:AbortSignal.timeout(15000)});
const fd=await fr.json();

if(!fd.ok||!fd.result?.file_path)continue;

const url='https://api.telegram.org/file/bot'+bot+'/'+fd.result.file_path;
const hdrs={};
const rng=req.headers.get('Range');
if(rng)hdrs.Range=rng;

const tr=await fetch(url,{headers:hdrs,signal:AbortSignal.timeout(45000)});
if(!tr.ok)continue;

const h=new Headers();
const cl=tr.headers.get('content-length');
const cr=tr.headers.get('content-range');

if(cl)h.set('Content-Length',cl);
if(cr)h.set('Content-Range',cr);

h.set('Content-Type',mime);
h.set('Accept-Ranges','bytes');
h.set('Access-Control-Allow-Origin','*');
h.set('Cache-Control','public,max-age=31536000');

const u=new URL(req.url);
if(u.searchParams.has('dl')||u.searchParams.has('download')){
h.set('Content-Disposition','attachment; filename="'+m.filename+'"');
}else{
h.set('Content-Disposition','inline');
}

return new Response(tr.body,{status:tr.status,headers:h});

}catch(e){
continue;
}
}

return err('All bots failed',503);
}

async function chunked(req,env,m,mime){
const cs=m.chunks;
const tot=m.size;
const csz=m.chunkSize||20971520;
const rng=req.headers.get('Range');
const u=new URL(req.url);
const dl=u.searchParams.has('dl')||u.searchParams.has('download');

if(rng){
return rngReq(env,m,rng,mime,csz,dl,tot);
}

if(dl){
return dlReq(env,m,mime,tot);
}

return initReq(env,m,mime,tot);
}

async function initReq(env,m,mime,tot){
const cs=m.chunks;

const h=new Headers();
h.set('Content-Type',mime);
h.set('Content-Length',tot.toString());
h.set('Accept-Ranges','bytes');
h.set('Access-Control-Allow-Origin','*');
h.set('Cache-Control','public,max-age=31536000');
h.set('Content-Disposition','inline');

let idx=0;

const stm=new ReadableStream({
async pull(c){
if(idx>=cs.length){
c.close();
return;
}

try{
const d=await ld(env,cs[idx]);
c.enqueue(new Uint8Array(d));
idx++;
}catch(e){
c.error(e);
}
}
});

return new Response(stm,{status:200,headers:h});
}

async function rngReq(env,m,rh,mime,csz,dl,tot){
const cs=m.chunks;
const mt=rh.match(/bytes=(d+)-(d*)/);
if(!mt)return err('Invalid range',416,{'Content-Range':'bytes */'+tot});

const st=parseInt(mt[1],10);
let ed=mt[2]?parseInt(mt[2],10):tot-1;

if(ed>=tot)ed=tot-1;
if(st>=tot||st>ed)return err('Range error',416,{'Content-Range':'bytes */'+tot});

const sz=ed-st+1;
const sc=Math.floor(st/csz);
const ec=Math.floor(ed/csz);
const nd=cs.slice(sc,ec+1);

try{
let p=sc*csz;
const pts=[];

for(let i=0;i<nd.length;i++){
const d=await ld(env,nd[i]);
const a=new Uint8Array(d);
const cs=Math.max(st-p,0);
const ce=Math.min(a.length,ed-p+1);

if(cs<ce)pts.push(a.slice(cs,ce));

p+=csz;
if(p>ed)break;
}

const ln=pts.reduce((s,p)=>s+p.length,0);
const cb=new Uint8Array(ln);
let of=0;

for(let i=0;i<pts.length;i++){
cb.set(pts[i],of);
of+=pts[i].length;
}

const h=new Headers();
h.set('Content-Type',mime);
h.set('Content-Length',sz.toString());
h.set('Content-Range','bytes '+st+'-'+ed+'/'+tot);
h.set('Accept-Ranges','bytes');
h.set('Access-Control-Allow-Origin','*');
h.set('Content-Disposition',dl?'attachment; filename="'+m.filename+'"':'inline');
h.set('Cache-Control','public,max-age=31536000');

return new Response(cb,{status:206,headers:h});

}catch(e){
return err('Range failed: '+e.message,500);
}
}

async function dlReq(env,m,mime,tot){
const cs=m.chunks;
let idx=0;

const stm=new ReadableStream({
async pull(c){
if(idx>=cs.length){
c.close();
return;
}

try{
const d=await ld(env,cs[idx]);
c.enqueue(new Uint8Array(d));
idx++;
}catch(e){
c.error(e);
}
}
});

const h=new Headers();
h.set('Content-Type',mime);
h.set('Content-Length',tot.toString());
h.set('Content-Disposition','attachment; filename="'+m.filename+'"');
h.set('Access-Control-Allow-Origin','*');
h.set('Cache-Control','public,max-age=31536000');

return new Response(stm,{status:200,headers:h});
}

async function ld(env,inf){
const kv=env[inf.kvNamespace]||env.FILES_KV;
const k=inf.keyName||inf.chunkKey;

const ms=await kv.get(k);
if(!ms)throw new Error('Chunk not found: '+k);

const m=JSON.parse(ms);
m.telegramFileId=m.telegramFileId||m.fileIdCode;

if(m.directUrl){
try{
const r=await fetch(m.directUrl,{signal:AbortSignal.timeout(30000)});
if(r.ok)return r.arrayBuffer();
}catch(e){}
}

const bots=[env.BOT_TOKEN,env.BOT_TOKEN2,env.BOT_TOKEN3,env.BOT_TOKEN4].filter(t=>t);

for(let i=0;i<bots.length;i++){
const bot=bots[i];

try{
const fr=await fetch('https://api.telegram.org/bot'+bot+'/getFile?file_id='+encodeURIComponent(m.telegramFileId),{signal:AbortSignal.timeout(15000)});
const fd=await fr.json();

if(!fd.ok||!fd.result?.file_path)continue;

const url='https://api.telegram.org/file/bot'+bot+'/'+fd.result.file_path;
const r=await fetch(url,{signal:AbortSignal.timeout(30000)});

if(r.ok){
kv.put(k,JSON.stringify({...m,directUrl:url,refreshed:Date.now()})).catch(()=>{});
return r.arrayBuffer();
}

}catch(e){
continue;
}
}

throw new Error('All bots failed');
}

function err(msg,st,ex){
const h=new Headers({'Content-Type':'application/json','Access-Control-Allow-Origin':'*',...ex});
const e={error:msg,status:st||500,time:new Date().toISOString()};
return new Response(JSON.stringify(e,null,2),{status:st||500,headers:h});
}