export async function onRequest(context) {
  const { request, env, params } = context;
  const slug = params.slug;

  console.log('Serving file:', slug);

  try {
    // ✅ FIXED: Get URL as plain text (no JSON parsing)
    const directUrl = await env.FILES_KV.get(slug, 'text');
    const kvData = await env.FILES_KV.get(slug, { type: 'json' });

    console.log('Retrieved from KV - URL:', directUrl, 'Metadata:', kvData);

    if (!directUrl) {
      console.log('File not found in KV');
      return new Response('File not found', { status: 404 });
    }

    // ✅ FIXED: Proper Range header handling for video streaming
    const range = request.headers.get('Range');
    const fetchOptions = { method: 'GET', headers: {} };
    
    if (range) {
      console.log('Range request detected:', range);
      fetchOptions.headers['Range'] = range;
    }

    // Fetch from Telegram
    const response = await fetch(directUrl, fetchOptions);
    console.log('Telegram response status:', response.status);

    if (!response.ok) {
      console.error('Telegram fetch failed:', response.status);
      return new Response('File not accessible', { status: response.status });
    }

    // Create response headers
    const headers = new Headers();

    // ✅ FIXED: Copy all important headers for proper streaming
    for (const [key, value] of response.headers.entries()) {
      const lowerKey = key.toLowerCase();
      if (['content-type', 'content-length', 'content-range', 'accept-ranges', 'last-modified', 'etag'].includes(lowerKey)) {
        headers.set(key, value);
        console.log(`Copied header: ${key}: ${value}`);
      }
    }

    // Set CORS and caching headers
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
    headers.set('Accept-Ranges', 'bytes');
    headers.set('Cache-Control', 'public, max-age=31536000, immutable');

    // ✅ FIXED: Proper content disposition
    const url = new URL(request.url);
    const isDownload = url.searchParams.has('dl');
    const filename = kvData?.metadata?.filename || slug;
    const contentType = headers.get('Content-Type') || kvData?.metadata?.contentType || '';

    console.log('Content-Type:', contentType, 'Is download:', isDownload);

    if (isDownload) {
      headers.set('Content-Disposition', `attachment; filename="${filename}"`);
    } else {
      // For media files, show inline for streaming
      if (contentType.startsWith('image/') || 
          contentType.startsWith('video/') || 
          contentType.startsWith('audio/')) {
        headers.set('Content-Disposition', 'inline');
      } else {
        headers.set('Content-Disposition', `attachment; filename="${filename}"`);
      }
    }

    console.log('Serving file successfully with status:', response.status);

    return new Response(response.body, {
      status: response.status,
      headers: headers
    });

  } catch (error) {
    console.error('File serve error:', error);
    return new Response(`Server error: ${error.message}`, { status: 500 });
  }
}
