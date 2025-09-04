export async function onRequest(context) {
  const { request, env, params } = context;
  const slug = params.slug;
  const url = new URL(request.url);

  console.log('=== SERVING FILE ===');
  console.log('Slug:', slug);
  console.log('Is download?', url.searchParams.has('dl'));

  try {
    // Get file URL from KV
    let directUrl;
    let metadata;
    
    if (env.FILES_KV) {
      directUrl = await env.FILES_KV.get```ug, 'text');
      metadata = await env.FILES_KV.get(slug, { type: 'json' });
    }
    
    if (!directUrl) {
      return new Response('File not found', { status: 404 });
    }

    // Handle Range requests for video streaming
    const range = request.headers.get('Range');
    const fetchOptions = {};
    
    if (range) {
      fetchOptions.headers = { 'Range': range };
    }

    // Fetch from Telegram
    const response = await fetch(directUrl, fetchOptions);
    
    if (!response.ok) {
      return new Response('File not accessible', { status: response.status });
    }

    // Setup response headers
    const headers = new Headers();

    // Copy important headers
    for (const [key, value] of response.headers.entries()) {
      const lowerKey = key.toLowerCase();
      if (['content-type', 'content-length', 'content-range', 'accept-ranges', 'last-modified', 'etag'].includes(lowerKey)) {
        headers.set(key, value);
      }
    }

    // Set CORS and cache headers
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Accept-Ranges', 'bytes');
    headers.set('Cache-Control', 'public, max-age=31536000, immutable');

    // ✅ FIXED: Proper Content-Disposition logic
    const isDownload = url.searchParams.has('dl');
    const filename = metadata?.metadata?.filename || slug;
    const contentType = headers.get('Content-Type') || '';

    console.log('Content-Type:', contentType);
    console.log('Filename:', filename);

    if (isDownload) {
      // Force download
      headers.set('Content-Disposition', `attachment; filename="${filename}"`);
      console.log('Setting as download');
    } else {
      // Show in browser (view)
      if (contentType.startsWith('image/') || 
          contentType.startsWith('video/') || 
          contentType.startsWith('audio/') ||
          contentType === 'application/pdf' ||
          contentType.startsWith('text/')) {
        headers.set('Content-Disposition', 'inline');
        console.log('Setting as inline view');
      } else {
        // For unknown types, force download
        headers.set('Content-Disposition', `attachment; filename="${filename}"`);
        console.log('Unknown type, setting as download');
      }
    }

    return new Response(response.body, {
      status: response.status,
      headers: headers
    });

  } catch (error) {
    console.error('File serve error:', error);
    return new Response(`Server error: ${error.message}`, { status: 500 });
  }
}
