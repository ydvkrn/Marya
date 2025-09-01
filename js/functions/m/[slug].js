export async function onRequest({params,env,request}) {
  const key = params.slug;                       // slug.ext
  const hit = await env.FILES_KV.getWithMetadata(key,'arrayBuffer');
  if(!hit.value) return new Response('404',{status:404});

  const meta=hit.metadata||{}, buf=new Uint8Array(hit.value);
  const baseHdr = {
    'Content-Type' : meta.ctype||'application/octet-stream',
    'Cache-Control': 'public, max-age=157680000, immutable',  // 5 yr
    'Expires'      : new Date(Date.now()+157680000000).toUTCString(),
    'X-Content-Type-Options':'nosniff',
    'Access-Control-Allow-Origin':'*',
    'Accept-Ranges':'bytes'
  };
  if (request.url.includes('dl=1'))
      baseHdr['Content-Disposition']=`attachment; filename="${meta.filename||key}"`;

  /* Range support */
  const range=request.headers.get('Range');
  if(range){
    const [ ,s,e]=/bytes=(\d*)-(\d*)/.exec(range); const st=Number(s||0); const en=e?Number(e):buf.length-1;
    baseHdr['Content-Range']=`bytes ${st}-${en}/${buf.length}`; baseHdr['Content-Length']=en-st+1;
    return new Response(buf.slice(st,en+1),{status:206,headers:baseHdr});
  }
  baseHdr['Content-Length']=buf.length;
  return new Response(buf,{headers:baseHdr});
}
