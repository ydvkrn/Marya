export async function onRequest(context) {
  const { request, env, params } = context;
  const slug = params.slug;

  console.log('=== SERVING FILE ===');
  console.log('Slug:', slug);
  console.log('URL:', request.url);

  try {
    // Get file URL from KV
    let directUrl;
    
    if (env.FILES_KV) {
      directUrl = await env.FILES_KV.get(slug, 'text');
      console.log('KV lookup result:', !!directUrl);
    }
    
    if (!directUrl) {
      console.log('File not found in KV');
      return new Response('File not found', { 
        status: 404,
        headers: { 'Content-Type': 'text/plain' }
      });
    }

    console.log('Found direct URL, fetching...');

    // Handle Range requests for video streaming
    const range = request.headers.get('Range');
    const fetchOptions = {};
    
    if (range) {
      console.log('Range request:', range);
      fetchOptions.headers = { 'Range': range };
    }

    // Fetch from Telegram
    const response = await fetch(directUrl, fetchOptions);
    console.log('Telegram response:', response.status, response.ok);
    
    if (!response.ok) {
      return new Response(`File not accessible: ${response.status}`, { 
        status: response.status,
        headers: { 'Content-Type': 'text/plain' }
      });
    }

    // Setup response headers
    const headers = new Headers();

    // Copy important headers
    for (const [key, value] of response.headers.entries()) {
      const lowerKey = key.toLowerCase();
      if (['content-type', 'content-length', 'content-range', 'accept-ranges', 'last-modified', 'etag'].includes(lowerKey)) {
        headers.set(key, value);
        console.log(`Header: ${key} = ${value}`);
      }
    }

    // Set CORS and cache headers
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Accept-Ranges', 'bytes');
    headers.set('Cache-Control', 'public, max-age=31536000, immutable');

    // Content disposition
    const url = new URL(request.url);
    const isDownload = url.searchParams.has('dl');
    const filename = slug;

    if (isDownload) {
      headers.set('Content-Disposition', `attachment; filename="${filename}"`);
    } else {
      const contentType = headers.get('Content-Type') || '';
      if (contentType.startsWith('image/') || 
          contentType.startsWith('video/') || 
          contentType.startsWith('audio/') ||
          contentType === 'application/pdf') {
        headers.set('Content-Disposition', 'inline');
      } else {
        headers.set('Content-Disposition', `attachment; filename="${filename}"`);
      }
    }

    console.log('Serving file successfully');
    
    return new Response(response.body, {
      status: response.status,
      headers: headers
    });

  } catch (error) {
    console.error('File serve error:', error);
    return new Response(`Server error: ${error.message}`, { 
      status: 500,
      headers: { 'Content-Type': 'text/plain' }
    });
  }
}
