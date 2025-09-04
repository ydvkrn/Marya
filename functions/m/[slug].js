export async function onRequest({ params, request, env }) {
  try {
    const slug = params.slug;
    
    // ✅ MAIN FIX: Get as plain text, NOT JSON
    const directUrl = await env.FILES_KV.get(slug);
    
    if (!directUrl) {
      return new Response('File not found', { status: 404 });
    }

    console.log('Direct URL:', directUrl);

    // Handle range requests
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

    // Create new headers
    const headers = new Headers();
    
    // Copy essential headers
    const contentType = response.headers.get('Content-Type') || 'application/octet-stream';
    const contentLength = response.headers.get('Content-Length');
    const contentRange = response.headers.get('Content-Range');
    
    headers.set('Content-Type', contentType);
    if (contentLength) headers.set('Content-Length', contentLength);
    if (contentRange) headers.set('Content-Range', contentRange);
    
    // Custom headers
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Cache-Control', 'public, max-age=315360000, immutable');
    headers.set('Accept-Ranges', 'bytes');

    // Content disposition
    const isDownload = request.url.includes('dl=1');
    if (isDownload) {
      headers.set('Content-Disposition', `attachment; filename="${slug}"`);
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
