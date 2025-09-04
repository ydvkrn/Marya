import { CACHE_SECS } from '../_config.js';

export async function onRequest({ params, request, env }) {
  try {
    const slug = params.slug;
    
    // ✅ Get direct URL from KV (stored as string)
    const directUrl = await env.FILES_KV.get(slug);
    const metadata = await env.FILES_KV.get(slug, 'json');

    if (!directUrl) {
      return new Response('File not found', { status: 404 });
    }

    console.log('Serving file:', slug, 'URL:', directUrl);

    // ✅ FIXED: Proxy request through our worker
    const range = request.headers.get('Range');
    const fetchHeaders = {};
    
    if (range) {
      fetchHeaders['Range'] = range;
    }

    const response = await fetch(directUrl, { 
      headers: fetchHeaders,
      cf: {
        cacheTtl: CACHE_SECS,
        cacheEverything: true
      }
    });
    
    if (!response.ok) {
      return new Response('File not accessible', { status: 404 });
    }

    const headers = new Headers();
    
    // Copy important headers
    if (response.headers.get('Content-Type')) {
      headers.set('Content-Type', response.headers.get('Content-Type'));
    }
    
    if (response.headers.get('Content-Length')) {
      headers.set('Content-Length', response.headers.get('Content-Length'));
    }
    
    if (response.headers.get('Content-Range')) {
      headers.set('Content-Range', response.headers.get('Content-Range'));
    }

    // Set our headers
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Cache-Control', `public, max-age=${CACHE_SECS}, immutable`);
    headers.set('Accept-Ranges', 'bytes');
    
    // ✅ FIXED: Proper Content-Disposition based on request
    const isDownload = request.url.includes('dl=1');
    const filename = metadata?.metadata?.filename || slug;
    
    if (isDownload) {
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
