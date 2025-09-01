const MAX = 25 * 1024 * 1024;                      // ≤25 MB
const j  = (o,s=200)=>new Response(JSON.stringify(o),{status:s,headers:{'Content-Type':'application/json'}});

export async function onRequest({request,env}) {
  const kv = env.FILES_KV, url = new URL(request.url);

  // ------------ via external URL ------------
  if (url.searchParams.has('src')) {
    try {
      const src = url.searchParams.get('src');
      const r   = await fetch(src); if (!r.ok) return j({success:false,error:'fetch failed'},502);
      const buf = await r.arrayBuffer();
      if (buf.byteLength>MAX) return j({success:false,error:'Max 25 MB'},413);
      const id  = crypto.randomUUID();
      await kv.put(id,buf,{metadata:{filename:name(src),ctype:r.headers.get('content-type'),size:buf.byteLength}});
      return j(resp(url.origin,id,name(src),buf.byteLength,r.headers.get('content-type')));
    } catch (e){ return j({success:false,error:e.message},500); }
  }

  // ------------ via form POST ------------
  if (request.method==='POST') {
    try{
      const fd = await request.formData(); const file = fd.get('file');
      if (!file)           return j({success:false,error:'no file'},400);
      if (file.size>MAX)   return j({success:false,error:'Max 25 MB'},413);
      const id = crypto.randomUUID();
      await kv.put(id,await file.arrayBuffer(),{metadata:{filename:file.name,ctype:file.type,size:file.size}});
      return j(resp(url.origin,id,file.name,file.size,file.type));
    }catch(e){ return j({success:false,error:e.message},500); }
  }
  return j({success:false,error:'method not allowed'},405);
}

const resp = (o,id,n,s,t)=>({success:true,file_id:id,filename:n,size:s,media_type:t,
  view_url:`${o}/file/${id}`,download_url:`${o}/file/${id}?dl=1`});
const name = u => u.split('/').pop().split('?')[0] || 'file';
