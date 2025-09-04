export async function onRequest(context) {
  const { request, env, params } = context;
  const url = new URL(request.url);
  
  // Extract slug from URL path /f/slug
  const pathParts = url.pathname.split('/');
  const slug = pathParts[2]; // /f/slug -> slug is at index 2
  
  if (!slug) {
    return new Response('File not found', { status: 404 });
  }

  try {
    // Get Telegram URL from KV
    const telegramURL = await env.VAULT_KV.get(slug);
    const metadata = await env.VAULT_KV.get(slug, { type: 'json' });
    
    if (!telegramURL) {
      return new Response('File not found', { status: 404 });
    }

    // Handle Range requests for streaming
    const range = request.headers.get('Range');
    const fetchHeaders = {};
    if (range) {
      fetchHeaders['Range'] = range;
    }

    // Fetch file from Telegram
    const response = await fetch(telegramURL, { headers: fetchHeaders });
    
    if (!response.ok) {
      return new Response('File not accessible', { status: response.status });
    }

    // Setup response headers
    const headers = new Headers();
    
    // Copy important headers from Telegram
    for (const [key, value] of response.headers.entries()) {
      if (['content-type', 'content-length', 'content-range', 'accept-ranges'].includes(key.toLowerCase())) {
        headers.set(key, value);
      }
    }

    // Set CORS and caching
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Cache-Control', 'public, max-age=86400');
    headers.set('Accept-Ranges', 'bytes');

    // Content disposition
    const isDownload = url.searchParams.has('dl');
    const contentType = headers.get('Content-Type') || '';
    const filename = metadata?.metadata?.filename || slug;
    
    if (isDownload) {
      headers.set('Content-Disposition', `attachment; filename="${filename}"`);
    } else {
      // For media files, show inline
      if (contentType.startsWith('image/') || 
          contentType.startsWith('video/') || 
          contentType.startsWith('audio/')) {
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
    return new Response(`Error: ${error.message}`, { status: 500 });
  }
}
