import { CACHE_SECS } from '../_config.js';

export async function onRequest({ params, request, env }) {
  try {
    const slug = params.slug;
    
    // ✅ FIXED: Get URL as text (not JSON)
    const directUrl = await env.FILES_KV.get(slug, 'text');
    const metadata = await env.FILES_KV.get(slug, { type: 'json' });

    if (!directUrl) {
      return new Response('File not found', { status: 404 });
    }

    console.log('Serving file:', slug);
    console.log('Direct URL:', directUrl);

    // ✅ Handle range requests for video streaming
    const range = request.headers.get('Range');
    const fetchHeaders = {};
    
    if (range) {
      fetchHeaders['Range'] = range;
    }

    // Fetch file from Telegram
    const response = await fetch(directUrl, { 
      headers: fetchHeaders
    });
    
    if (!response.ok) {
      console.error('Telegram fetch error:', response.status, response.statusText);
      return new Response('File not accessible', { status: 404 });
    }

    const headers = new Headers();
    
    // ✅ FIXED: Set proper content type
    let contentType = response.headers.get('Content-Type');
    if (metadata?.metadata?.contentType) {
      contentType = metadata.metadata.contentType;
    }
    
    // Determine if should be inline or attachment
    const isDownload = request.url.includes('dl=1');
    const isViewable = contentType && (
      contentType.startsWith('image/') ||
      contentType.startsWith('video/') ||
      contentType.startsWith('audio/') ||
      contentType === 'application/pdf'
    );

    // Set headers
    headers.set('Content-Type', contentType || 'application/octet-stream');
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Cache-Control', `public, max-age=${CACHE_SECS}, immutable`);
    headers.set('Accept-Ranges', 'bytes');
    
    // Copy range headers if present
    if (response.headers.get('Content-Range')) {
      headers.set('Content-Range', response.headers.get('Content-Range'));
    }
    if (response.headers.get('Content-Length')) {
      headers.set('Content-Length', response.headers.get('Content-Length'));
    }

    // ✅ FIXED: Proper disposition based on file type and request
    const filename = metadata?.metadata?.filename || slug;
    
    if (isDownload || !isViewable) {
      headers.set('Content-Disposition', `attachment; filename="${filename}"`);
    } else {
      headers.set('Content-Disposition', 'inline');
    }

    return new Response(response.body, {
      status: response.status,
      headers
    });

  } catch (error) {
    console.error('Serve error:', error);
    return new Response(`Server error: ${error.message}`, { status: 500 });
  }
}
