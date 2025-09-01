const max = 25 * 1024 * 1024;
const json=(o,s=200)=>new Response(JSON.stringify(o),{status:s,headers:{'Content-Type':'application/json'}});

export async function onRequest(context){
  const kv=context.env.FILES_KV; const req=context.request; const url=new URL(req.url);

  /* URL upload */
  if(url.searchParams.has('src')){
    try{
      const src=url.searchParams.get('src'); const r=await fetch(src);
      if(!r.ok)         return json({success:false,error:'fetch failed'},502);
      const buf=await r.arrayBuffer();
      if(buf.byteLength>max) return json({success:false,error:'Max 25 MB'},413);
      const id=crypto.randomUUID();
      await kv.put(id,buf,{metadata:{filename:name(src),ctype:r.headers.get('content-type'),size:buf.byteLength}});
      return json(resp(url.origin,id,name(src),buf.byteLength,r.headers.get('content-type')));
    }catch(e){return json({success:false,error:e.message},500);}
  }

  /* form upload */
  if(req.method==='POST'){
    try{
      const fd=await req.formData(); const file=fd.get('file');
      if(!file)              return json({success:false,error:'no file'});
      if(file.size>max)      return json({success:false,error:'Max 25 MB'},413);
      const id=crypto.randomUUID();
      await kv.put(id,await file.arrayBuffer(),{metadata:{filename:file.name,ctype:file.type,size:file.size}});
      return json(resp(url.origin,id,file.name,file.size,file.type));
    }catch(e){return json({success:false,error:e.message},500);}
  }
  return json({success:false,error:'method?'} ,405);
}

const resp=(o,id,n,s,t)=>({success:true,file_id:id,filename:n,size:s,media_type:t,
  view_url:`${o}/file/${id}`,download_url:`${o}/file/${id}?dl=1`});
const name=u=>u.split('/').pop().split('?')[0]||'file';
