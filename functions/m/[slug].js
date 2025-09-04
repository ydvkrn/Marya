export async function onRequest({ params, request, env }) {
  try {
    const slug = params.slug;
    
    // ✅ Get URL as plain text (fixes JSON error)
    const directUrl = await env.FILES_KV.get(slug, 'text');
    const kvData = await env.FILES_KV.get(slug, { type: 'json' });

    if (!directUrl) {
      return new Response('File not found', { status: 404 });
    }

    // Handle range requests for video streaming
    const range = request.headers.get('Range');
    const fetchOptions = { headers: {} };
    
    if (range) {
      fetchOptions.headers['Range'] = range;
    }

    // Fetch from Telegram
    const response = await fetch(directUrl, fetchOptions);
    
    if (!response.ok) {
      return new Response('File not accessible', { status: 404 });
    }

    // Copy headers from Telegram response
    const headers = new Headers();
    for (const [key, value] of response.headers.entries()) {
      headers.set(key, value);
    }

    // Set custom headers
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Cache-Control', 'public, max-age=315360000, immutable');
    headers.set('Accept-Ranges', 'bytes');

    // Content disposition
    const isDownload = request.url.includes('dl=1');
    const filename = kvData?.metadata?.filename || slug;
    const contentType = headers.get('Content-Type') || kvData?.metadata?.contentType;
    
    if (isDownload) {
      headers.set('Content-Disposition', `attachment; filename="${filename}"`);
    } else if (contentType && (contentType.startsWith('image/') || contentType.startsWith('video/') || contentType.startsWith('audio/'))) {
      headers.set('Content-Disposition', 'inline');
    } else {
      headers.set('Content-Disposition', `attachment; filename="${filename}"`);
    }

    return new Response(response.body, {
      status: response.status,
      headers: headers
    });

  } catch (error) {
    console.error('Serve error:', error);
    return new Response(`Server error: ${error.message}`, { status: 500 });
  }
}
