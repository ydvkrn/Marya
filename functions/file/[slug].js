export async function onRequest(context) {
  const { request, env, params } = context;
  const slug = params.slug;

  try {
    // Get file URL from KV
    const telegramURL = await env.FILES_KV.get(slug);
    const metadata = await env.FILES_KV.get(slug, { type: 'json' });

    if (!telegramURL) {
      return new Response('File not found', { status: 404 });
    }

    // Handle range requests
    const range = request.headers.get('Range');
    const fetchHeaders = {};
    if (range) {
      fetchHeaders['Range'] = range;
    }

    // Fetch from Telegram
    const response = await fetch(telegramURL, { headers: fetchHeaders });

    if (!response.ok) {
      return new Response('File not accessible', { status: response.status });
    }

    // Setup headers
    const headers = new Headers();
    
    // Copy important headers
    for (const [key, value] of response.headers.entries()) {
      if (['content-type', 'content-length', 'content-range', 'accept-ranges'].includes(key.toLowerCase())) {
        headers.set(key, value);
      }
    }

    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Cache-Control', 'public, max-age=86400');
    headers.set('Accept-Ranges', 'bytes');

    // Content disposition
    const url = new URL(request.url);
    const isDownload = url.searchParams.has('dl');
    const filename = metadata?.metadata?.filename || slug;
    const contentType = headers.get('Content-Type') || '';

    if (isDownload) {
      headers.set('Content-Disposition', `attachment; filename="${filename}"`);
    } else if (contentType.startsWith('image/') || contentType.startsWith('video/') || contentType.startsWith('audio/')) {
      headers.set('Content-Disposition', 'inline');
    } else {
      headers.set('Content-Disposition', `attachment; filename="${filename}"`);
    }

    return new Response(response.body, {
      status: response.status,
      headers: headers
    });

  } catch (error) {
    return new Response(`Error: ${error.message}`, { status: 500 });
  }
}
