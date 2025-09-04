export async function onRequest(context) {
  const { request, env, params } = context;
  const slug = params.slug;

  console.log('=== FILE SERVE REQUEST ===');
  console.log('Slug:', slug);
  console.log('Request URL:', request.url);

  try {
    // Get file URL from KV (as plain text)
    const directUrl = await env.FILES_KV.get(slug, 'text');
    const kvData = await env.FILES_KV.get(slug, { type: 'json' });

    console.log('KV lookup - URL found:', !!directUrl);
    console.log('KV lookup - Metadata found:', !!kvData);

    if (!directUrl) {
      console.log('File not found in KV');
      return new Response('File not found', { 
        status: 404,
        headers: {
          'Content-Type': 'text/plain',
          'Access-Control-Allow-Origin': '*'
        }
      });
    }

    console.log('Direct URL (first 50 chars):', directUrl.substring(0, 50) + '...');

    // Handle Range requests for video streaming
    const range = request.headers.get('Range');
    const fetchOptions = {};
    
    if (range) {
      console.log('Range request:', range);
      fetchOptions.headers = { 'Range': range };
    }

    console.log('Fetching from Telegram...');

    // Fetch from Telegram
    const response = await fetch(directUrl, fetchOptions);
    
    console.log('Telegram response status:', response.status);
    console.log('Telegram response OK:', response.ok);

    if (!response.ok) {
      console.error('Telegram fetch failed:', response.status, response.statusText);
      return new Response(`File not accessible (${response.status})`, { 
        status: response.status,
        headers: {
          'Content-Type': 'text/plain',
          'Access-Control-Allow-Origin': '*'
        }
      });
    }

    // Create response headers
    const headers = new Headers();

    // Copy essential headers from Telegram response
    const essentialHeaders = ['content-type', 'content-length', 'content-range', 'accept-ranges', 'last-modified', 'etag'];
    
    for (const [key, value] of response.headers.entries()) {
      const lowerKey = key.toLowerCase();
      if (essentialHeaders.includes(lowerKey)) {
        headers.set(key, value);
        console.log(`Copied header: ${key}: ${value}`);
      }
    }

    // Set CORS and caching headers
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
    headers.set('Accept-Ranges', 'bytes');
    headers.set('Cache-Control', 'public, max-age=31536000, immutable');

    // Content disposition
    const url = new URL(request.url);
    const isDownload = url.searchParams.has('dl');
    const filename = kvData?.metadata?.filename || slug;
    const contentType = headers.get('Content-Type') || kvData?.metadata?.contentType || 'application/octet-stream';

    console.log('Content-Type:', contentType);
    console.log('Is download request:', isDownload);
    console.log('Filename:', filename);

    if (isDownload) {
      headers.set('Content-Disposition', `attachment; filename="${filename}"`);
    } else {
      // For media files, show inline for streaming/viewing
      if (contentType.startsWith('image/') || 
          contentType.startsWith('video/') || 
          contentType.startsWith('audio/') ||
          contentType === 'application/pdf') {
        headers.set('Content-Disposition', 'inline');
      } else {
        headers.set('Content-Disposition', `attachment; filename="${filename}"`);
      }
    }

    console.log('Serving file successfully with status:', response.status);
    console.log('Final Content-Disposition:', headers.get('Content-Disposition'));
    console.log('=== END FILE SERVE ===');

    return new Response(response.body, {
      status: response.status,
      headers: headers
    });

  } catch (error) {
    console.error('File serve error:', error);
    return new Response(`Server error: ${error.message}`, { 
      status: 500,
      headers: {
        'Content-Type': 'text/plain',
        'Access-Control-Allow-Origin': '*'
      }
    });
  }
}
