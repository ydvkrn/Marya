export async function onRequest(context) {
  const kv = context.env.FILES_KV;
  const id = context.params.id;
  const obj = await kv.getWithMetadata(id,'arrayBuffer');
  if (!obj.value) return new Response('Not found', { status: 404 });

  const meta = obj.metadata || {};
  const hdr = new Headers({
    'Content-Type': meta.ctype || 'application/octet-stream',
    'Cache-Control': 'public, max-age=31536000, immutable',
    'Expires': new Date(Date.now()+31536000000).toUTCString()
  });
  if (context.request.url.includes('dl=1'))
      hdr.set('Content-Disposition', `attachment; filename="${meta.filename||id}"`);
  return new Response(obj.value, { headers: hdr });
}
