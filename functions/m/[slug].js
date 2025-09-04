import { CACHE_SECS } from '../_config.js';

export async function onRequest({ params, request, env }) {
  try {
    const slug = params.slug;
    const directUrl = await env.FILES_KV.get(slug);
    const metadata = await env.FILES_KV.get(slug, 'json');

    if (!directUrl) {
      return new Response('File not found', { status: 404 });
    }

    const response = await fetch(directUrl);
    
    if (!response.ok) {
      return new Response('File not accessible', { status: 404 });
    }

    const headers = new Headers();
    
    // ✅ FIXED: Set proper Content-Type for images/videos to view inline
    let contentType = response.headers.get('Content-Type');
    if (metadata?.contentType) {
      contentType = metadata.contentType;
    }

    // Determine if file should be viewed inline or downloaded
    const isDownload = request.url.includes('dl=1');
    const isViewableInline = contentType && (
      contentType.startsWith('image/') ||
      contentType.startsWith('video/') ||
      contentType.startsWith('audio/') ||
      contentType === 'application/pdf' ||
      contentType.startsWith('text/')
    );

    headers.set('Content-Type', contentType || 'application/octet-stream');
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Cache-Control', `public, max-age=${CACHE_SECS}, immutable`);
    headers.set('Accept-Ranges', 'bytes');

    // ✅ FIXED: Only set attachment for download links, inline for view
    if (isDownload || !isViewableInline) {
      const filename = metadata?.filename || slug;
      headers.set('Content-Disposition', `attachment; filename="${filename}"`);
    } else {
      headers.set('Content-Disposition', 'inline');
    }

    // Pass through range headers for video streaming
    const range = request.headers.get('Range');
    const fetchHeaders = {};
    if (range) {
      fetchHeaders['Range'] = range;
    }

    const finalResponse = await fetch(directUrl, { headers: fetchHeaders });
    
    // Copy response headers for range requests
    if (finalResponse.status === 206) {
      headers.set('Content-Range', finalResponse.headers.get('Content-Range'));
      headers.set('Content-Length', finalResponse.headers.get('Content-Length'));
    } else if (finalResponse.headers.get('Content-Length')) {
      headers.set('Content-Length', finalResponse.headers.get('Content-Length'));
    }

    return new Response(finalResponse.body, {
      status: finalResponse.status,
      headers
    });

  } catch (error) {
    console.error('Serve error:', error);
    return new Response('Server error', { status: 500 });
  }
}
