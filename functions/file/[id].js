export async function onRequest(context){
  const kv=context.env.FILES_KV; const id=context.params.id;
  const hit=await kv.getWithMetadata(id,'arrayBuffer');
  if(!hit.value) return new Response('404',{status:404});
  const meta=hit.metadata||{}, buf=new Uint8Array(hit.value);
  const h={'Content-Type':meta.ctype||'application/octet-stream','Cache-Control':'public, max-age=31536000, immutable','Expires':new Date(Date.now()+31536000000).toUTCString(),'Accept-Ranges':'bytes'};
  if(context.request.url.includes('dl=1')) h['Content-Disposition']=`attachment; filename="${meta.filename||id}"`;
  const range=context.request.headers.get('Range');
  if(range){
    const [ ,s,e]=/bytes=(\d*)-(\d*)/.exec(range); const st=Number(s||0); const en=e?Number(e):buf.length-1;
    h['Content-Range']=`bytes ${st}-${en}/${buf.length}`; h['Content-Length']=en-st+1;
    return new Response(buf.slice(st,en+1),{status:206,headers:h});
  }
  h['Content-Length']=buf.length; return new Response(buf,{headers:h});
}
