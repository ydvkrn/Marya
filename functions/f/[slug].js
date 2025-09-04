export async function onRequest(context) {
  const { request, env, params } = context;
  const slug = params.slug;
  const url = new URL(request.url);

  try {
    let directUrl;
    let metadata;
    
    if (env.FILES_KV) {
      directUrl = await env.FILES_KV.get(slug, 'text');
      metadata = await env.FILES_KV.get(slug, { type: 'json' });
    }
    
    if (!directUrl) {
      return new Response('File not found', { status: 404 });
    }

    const range = request.headers.get('Range');
    const fetchOptions = {};
    
    if (range) {
      fetchOptions.headers = { 'Range': range };
    }

    const response = await fetch(directUrl, fetchOptions);
    
    if (!response.ok) {
      return new Response('File not accessible', { status: response.status });
    }

    const headers = new Headers();

    for (const [key, value] of response.headers.entries()) {
      const lowerKey = key.toLowerCase();
      if (['content-type', 'content-length', 'content-range', 'accept-ranges', 'last-modified', 'etag'].includes(lowerKey)) {
        headers.set(key, value);
      }
    }

    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Accept-Ranges', 'bytes');
    headers.set('Cache-Control', 'public, max-age=31536000, immutable');

    const isDownload = url.searchParams.has('dl');
    const filename = metadata?.metadata?.filename || slug;
    const contentType = headers.get('Content-Type') || '';

    if (isDownload) {
      headers.set('Content-Disposition', `attachment; filename="${filename}"`);
    } else {
      if (contentType.startsWith('image/') || 
          contentType.startsWith('video/') || 
          contentType.startsWith('audio/') ||
          contentType === 'application/pdf' ||
          contentType.startsWith('text/')) {
        headers.set('Content-Disposition', 'inline');
      } else {
        headers.set('Content-Disposition', `attachment; filename="${filename}"`);
      }
    }

    return new Response(response.body, {
      status: response.status,
      headers: headers
    });

  } catch (error) {
    return new Response(`Server error: ${error.message}`, { status: 500 });
  }
}
