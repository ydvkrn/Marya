export async function onRequest({ params, request, env }) {
  try {
    const slug = params.slug;
    
    // Get file URL from KV
    const directUrl = await env.FILES_KV.get(slug);
    const metadata = await env.FILES_KV.get(slug, { type: 'json' });
    
    if (!directUrl) {
      return new Response('File not found', { status: 404 });
    }

    // Handle range requests for streaming
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

    // Create headers
    const headers = new Headers();
    
    // Copy essential headers
    for (const [key, value] of response.headers.entries()) {
      if (['content-type', 'content-length', 'content-range', 'accept-ranges'].includes(key.toLowerCase())) {
        headers.set(key, value);
      }
    }

    // Custom headers
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Cache-Control', 'public, max-age=315360000, immutable');
    headers.set('Accept-Ranges', 'bytes');

    // Content disposition
    const contentType = headers.get('Content-Type') || metadata?.metadata?.contentType || 'application/octet-stream';
    const filename = metadata?.metadata?.filename || slug;
    const isDownload = request.url.includes('dl=1');
    
    if (isDownload) {
      headers.set('Content-Disposition', `attachment; filename="${filename}"`);
    } else {
      headers.set('Content-Disposition', 'inline');
    }

    return new Response(response.body, {
      status: response.status,
      headers: headers
    });

  } catch (error) {
    console.error('Serve error:', error);
    return new Response(`Error: ${error.message}`, { status: 500 });
  }
}
