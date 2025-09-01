export async function onRequest({params,env,request}) {
  const hit = await env.FILES_KV.getWithMetadata(params.id,'arrayBuffer');
  if (!hit.value) return new Response('404',{status:404});

  const meta=hit.metadata||{}, buf=new Uint8Array(hit.value);
  const hdr={
    'Content-Type' : meta.ctype||'application/octet-stream',
    'Cache-Control': 'public, max-age=31536000, immutable',
    'Expires'      : new Date(Date.now()+31536000000).toUTCString(),
    'Accept-Ranges': 'bytes'
  };
  if (request.url.includes('dl=1'))
      hdr['Content-Disposition']=`attachment; filename="${meta.filename||params.id}"`;

  const range=request.headers.get('Range');
  if(range){
    const [ ,s,e]=/bytes=(\d*)-(\d*)/.exec(range); const start=Number(s||0); const end=e?Number(e):buf.length-1;
    hdr['Content-Range']=`bytes ${start}-${end}/${buf.length}`; hdr['Content-Length']=end-start+1;
    return new Response(buf.slice(start,end+1),{status:206,headers:hdr});
  }
  hdr['Content-Length']=buf.length; return new Response(buf,{headers:hdr});
}
