// MIME type map and getMimeType(...) remain unchanged

export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.id;

  try {
    const actualId = fileId.includes('.') ? fileId.substring(0, fileId.lastIndexOf('.')) : fileId;
    const extension = fileId.includes('.') ? fileId.substring(fileId.lastIndexOf('.') + 1) : '';

    // Read value + metadata together; value is the direct Telegram URL (text), metadata is the object stored at put(...)
    const { value: directUrl, metadata } = await env.FILES_KV.getWithMetadata(actualId, { type: 'text' });
    if (!directUrl) {
      return new Response('File not found', { status: 404, headers: { 'Content-Type': 'text/plain' } });
    }

    const range = request.headers.get('Range');
    const fetchOptions = range ? { headers: { Range: range } } : {};
    const upstream = await fetch(directUrl, fetchOptions);
    if (!upstream.ok) {
      return new Response(`File not accessible: ${upstream.status}`, { status: upstream.status, headers: { 'Content-Type': 'text/plain' } });
    }

    const headers = new Headers();
    for (const [key, value] of upstream.headers.entries()) {
      const lower = key.toLowerCase();
      if (['content-length', 'content-range', 'accept-ranges', 'last-modified', 'etag'].includes(lower)) {
        headers.set(key, value);
      }
    }

    const mimeType = getMimeType(extension);
    headers.set('Content-Type', mimeType);
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Accept-Ranges', 'bytes');
    headers.set('Cache-Control', 'public, max-age=31536000, immutable');

    const url = new URL(request.url);
    const isDownload = url.searchParams.has('dl');
    const filename = (metadata && metadata.filename) ? metadata.filename : fileId;

    if (isDownload) {
      headers.set('Content-Disposition', `attachment; filename="${filename}"`);
    } else {
      if (mimeType.startsWith('image/') || mimeType.startsWith('video/') || mimeType.startsWith('audio/') || mimeType === 'application/pdf' || mimeType.startsWith('text/')) {
        headers.set('Content-Disposition', 'inline');
      } else {
        headers.set('Content-Disposition', `attachment; filename="${filename}"`);
      }
    }

    return new Response(upstream.body, { status: upstream.status, headers });

  } catch (err) {
    return new Response(`Server error: ${err.message}`, { status: 500, headers: { 'Content-Type': 'text/plain' } });
  }
}
