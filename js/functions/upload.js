const MAX = 25 * 1024 * 1024;                          // 25 MB
const FIVE_YEARS = 157680000;                          // seconds
const json = (o,s=200)=>new Response(JSON.stringify(o),{status:s,headers:{'Content-Type':'application/json'}});

export async function onRequest({request,env}) {
  const kv = env.FILES_KV, url = new URL(request.url);

  // -------- via external URL --------
  if (url.searchParams.has('src')) {
    try {
      const src=url.searchParams.get('src'); const r=await fetch(src);
      if(!r.ok) return json(err('fetch failed'),502);
      const buf=await r.arrayBuffer(); if(buf.byteLength>MAX) return json(err('Max 25 MB'),413);
      const {slug,ext}=slugExt(getName(src),r.headers.get('content-type'));
      const key=`${slug}.${ext}`;
      await kv.put(key,buf,{metadata:{filename:getName(src),ctype:r.headers.get('content-type'),size:buf.byteLength}});
      return json(resp(url.origin,key,getName(src),buf.byteLength,r.headers.get('content-type')));
    } catch(e){ return json(err(e.message),500);}
  }

  // -------- via form POST --------
  if(request.method==='POST'){
    try{
      const fd=await request.formData(); const file=fd.get('file');
      if(!file) return json(err('no file'),400);
      if(file.size>MAX) return json(err('Max 25 MB'),413);
      const {slug,ext}=slugExt(file.name,file.type);
      const key=`${slug}.${ext}`;
      await kv.put(key,await file.arrayBuffer(),{metadata:{filename:file.name,ctype:file.type,size:file.size}});
      return json(resp(url.origin,key,file.name,file.size,file.type));
    }catch(e){ return json(err(e.message),500);}
  }
  return json(err('method'),405);
}

/* helpers */
const slugExt = (name,ctype)=>{
  const base=name.replace(/\.[a-z0-9]+$/i,'').replace(/[^a-z0-9]+/gi,'-').toLowerCase().slice(0,40)||'file';
  const ext = (ctype||'').split('/').pop().split(';')[0]||'bin';
  return {slug:crypto.randomUUID().slice(0,8)+'-'+base,ext};
};
const getName = u=>decodeURIComponent(u.split('/').pop().split('?')[0]||'file');
const err = m=>({success:false,error:m});
const resp=(o,k,n,s,t)=>({success:true,filename:n,size:s,media_type:t,
view_url:`${o}/m/${k}`,download_url:`${o}/m/${k}?dl=1`});
