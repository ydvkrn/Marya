export async function onRequest({ env, params }) {
  // key format: id_filename.ext
  let slug = params.key;
  let [id, ...fnameParts] = slug.split("_");
  let filename = fnameParts.join("_");
  const meta = await env.FILES_KV.get(`${id}_meta`, { type: 'json' });
  if (!meta) return new Response('Not found', { status: 404 });

  let { total } = meta;
  const CHUNK_SIZE = 20 * 1024 * 1024;
  async function* chunkStream() {
    for (let i = 0; i < total; i++) {
      let abuf = await env.FILES_KV.get(`${id}_chunk_${i}`, { type: 'arrayBuffer' });
      if (!abuf) throw new Error("Missing chunk " + i);
      yield new Uint8Array(abuf);
    }
  }
  const stream = new ReadableStream({
    async pull(ctrl) {
      for await (let chunk of chunkStream()) ctrl.enqueue(chunk);
      ctrl.close();
    }
  });

  // Set mime/headers (basic ext to mime)
  const mime = filename.endsWith('.mp4') ? 'video/mp4'
      : filename.endsWith('.jpg') ? 'image/jpeg'
      : filename.endsWith('.png') ? 'image/png'
      : filename.endsWith('.zip') ? 'application/zip'
      : 'application/octet-stream';
  return new Response(stream, {
    headers: {
      "Content-Type": mime,
      "Content-Disposition": `inline; filename="${filename}"`,
      "Cache-Control": "public, max-age=31536000, immutable"
    }
  });
}
