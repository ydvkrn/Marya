export async function onRequest(context) {
  const kv = context.env.FILES_KV;
  const { request } = context;
  const url = new URL(request.url);

  // ---------- GET ?src=externalURL ----------
  if (request.method === 'GET' && url.searchParams.has('src')) {
    const src = url.searchParams.get('src');
    const r = await fetch(src);
    if (!r.ok) return j({success:false,error:'fetch failed'},502);
    const buf = await r.arrayBuffer();
    if (buf.byteLength > 25*1024*1024) return j({success:false,error:'Max 25 MB'},413);
    const id = crypto.randomUUID();
    await kv.put(id, buf, {
      metadata: { filename: getName(src), ctype: r.headers.get('content-type'), size: buf.byteLength }
    });
    return j(res(url.origin,id,getName(src),buf.byteLength,r.headers.get('content-type')));
  }

  // ---------- POST form-data ----------
  if (request.method === 'POST') {
    const fd = await request.formData();
    const file = fd.get('file');
    if (!file) return j({success:false,error:'no file'},400);
    if (file.size > 25*1024*1024) return j({success:false,error:'Max 25 MB'},413);
    const id = crypto.randomUUID();
    await kv.put(id, await file.arrayBuffer(), {
      metadata: { filename: file.name, ctype: file.type, size: file.size }
    });
    return j(res(url.origin,id,file.name,file.size,file.type));
  }

  return new Response('Method Not Allowed',{status:405});
}

const j = (o,s=200)=>new Response(JSON.stringify(o),{status:s,headers:{'Content-Type':'application/json'}});
const res = (orig,id,name,size,type)=>({
  success:true,file_id:id,filename:name,size,media_type:type,
  view_url:`${orig}/file/${id}`,download_url:`${orig}/file/${id}?dl=1`
});
const getName = u => u.split('/').pop().split('?')[0] || 'file';
