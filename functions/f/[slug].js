export async function onRequest({ request, env, params }) {
  const slug = params.slug;
  const url = new URL(request.url);
  
  try {
    let directUrl;
    
    // Try to get from KV first
    if (env.FILES_KV) {
      directUrl = await env.FILES_KV.get(slug, 'text');
    }
    
    if (!directUrl) {
      return new Response('File not found', { status: 404 });
    }

    // Fetch from Telegram
    const response = await fetch(directUrl);
    
    if (!response.ok) {
      return new Response('File not accessible', { status: response.status });
    }

    const headers = new Headers();
    for (const [key, value] of response.headers.entries()) {
      if (['content-type', 'content-length'].includes(key.toLowerCase())) {
        headers.set(key, value);
      }
    }

    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Cache-Control', 'public, max-age=86400');
    
    // Download or view
    const isDownload = url.searchParams.has('dl');
    if (isDownload) {
      headers.set('Content-Disposition', `attachment; filename="${slug}"`);
    }

    return new Response(response.body, { headers });
    
  } catch (error) {
    return new Response(`Error: ${error.message}`, { status: 500 });
  }
}
