import { BOT_TOKEN, CHANNEL_ID } from './_config.js';

export async function onRequest(context) {
  const { request, env, params } = context;
  const url = new URL(request.url);
  const pathParts = url.pathname.split('/').filter(Boolean);
  
  // Extract slug from path: /btf/slug/anything
  const slug = pathParts[1]; // btf = 0, slug = 1
  
  if (!slug) {
    return new Response('Invalid URL', { status: 400 });
  }

  try {
    console.log('Looking for slug:', slug);
    
    // Get Telegram URL from KV
    const telegramUrl = await env.FILES_KV.get(slug);
    
    if (!telegramUrl) {
      console.log('File not found in KV:', slug);
      return new Response('File not found', { status: 404 });
    }

    console.log('Found Telegram URL:', telegramUrl);

    // Handle Range requests for video streaming
    const headers = new Headers();
    const range = request.headers.get('Range');
    const fetchOptions = {};
    
    if (range) {
      console.log('Range request:', range);
      fetchOptions.headers = { 'Range': range };
    }

    // Fetch from Telegram
    const response = await fetch(telegramUrl, fetchOptions);
    
    if (!response.ok) {
      console.log('Telegram fetch failed:', response.status);
      return new Response('File not accessible', { status: response.status });
    }

    console.log('Telegram response OK, Content-Type:', response.headers.get('Content-Type'));

    // Copy all headers from Telegram response
    for (const [key, value] of response.headers.entries()) {
      headers.set(key, value);
    }

    // Set CORS and cache headers
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Accept-Ranges', 'bytes');
    headers.set('Cache-Control', 'public, max-age=86400');

    // Check if download is requested
    const isDownload = url.searchParams.has('dl');
    const contentType = headers.get('Content-Type') || 'application/octet-stream';
    
    if (isDownload) {
      headers.set('Content-Disposition', `attachment; filename="${slug}"`);
    } else {
      // For viewable content, set inline
      if (contentType.startsWith('image/') || 
          contentType.startsWith('video/') || 
          contentType.startsWith('audio/')) {
        headers.set('Content-Disposition', 'inline');
      }
    }

    console.log('Serving file successfully');

    return new Response(response.body, {
      status: response.status,
      headers: headers
    });

  } catch (error) {
    console.error('Error serving file:', error);
    return new Response(`Server error: ${error.message}`, { status: 500 });
  }
}
