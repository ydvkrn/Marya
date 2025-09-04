export async function onRequest({ params, request, env }) {
  try {
    const slug = params.slug;
    
    // Get Telegram URL from KV
    const telegramUrl = await env.FILES_KV.get(slug, 'text');
    const metadata = await env.FILES_KV.get(slug, { type: 'json' });

    if (!telegramUrl) {
      return new Response('File not found', { status: 404 });
    }

    console.log('Serving:', slug, 'URL:', telegramUrl);

    // Handle range requests for video streaming
    const range = request.headers.get('Range');
    const fetchOptions = { method: 'GET', headers: {} };
    
    if (range) {
      fetchOptions.headers['Range'] = range;
    }

    // Fetch from Telegram
    const response = await fetch(telegramUrl, fetchOptions);
    
    if (!response.ok) {
      console.error('Telegram fetch failed:', response.status);
      return new Response('File not accessible', { status: 404 });
    }

    // Create response headers
    const headers = new Headers();
    
    // Copy important headers from Telegram response
    for (const [key, value] of response.headers.entries()) {
      if (['content-type', 'content-length', 'content-range', 'accept-ranges'].includes(key.toLowerCase())) {
        headers.set(key, value);
      }
    }

    // Set our custom headers
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
    headers.set('Access-Control-Allow-Headers', 'Range');
    headers.set('Cache-Control', 'public, max-age=315360000, immutable');
    headers.set('Accept-Ranges', 'bytes');

    // Content disposition
    const isDownload = request.url.includes('dl=1');
    const filename = metadata?.metadata?.filename || slug;
    const contentType = response.headers.get('Content-Type') || metadata?.metadata?.contentType || 'application/octet-stream';
    
    if (isDownload) {
      headers.set('Content-Disposition', `attachment; filename="${filename}"`);
    } else {
      // For images/videos/audio, show inline
      if (contentType.startsWith('image/') || contentType.startsWith('video/') || contentType.startsWith('audio/')) {
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
    console.error('Serve error:', error);
    return new Response(`Server error: ${error.message}`, { status: 500 });
  }
}
