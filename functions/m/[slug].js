export async function onRequest({ params, request, env }) {
  try {
    const slug = params.slug;
    const directUrl = await env.FILES_KV.get(slug);
    
    if (!directUrl) {
      return new Response('File not found', { status: 404 });
    }

    // Fetch from Telegram
    const response = await fetch(directUrl);
    if (!response.ok) {
      return new Response('File not accessible', { status: 404 });
    }

    // Create headers
    const headers = new Headers(response.headers);
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Cache-Control', 'public, max-age=315360000');
    
    // Content type based on extension
    const contentType = response.headers.get('Content-Type') || getContentType(slug);
    headers.set('Content-Type', contentType);
    
    // Set disposition - inline for viewable files
    if (contentType.startsWith('image/') || contentType.startsWith('video/') || contentType.startsWith('audio/')) {
      headers.set('Content-Disposition', 'inline');
    } else {
      headers.set('Content-Disposition', `attachment; filename="${slug}"`);
    }

    return new Response(response.body, {
      status: response.status,
      headers: headers
    });

  } catch (error) {
    return new Response('Server error', { status: 500 });
  }
}

function getContentType(filename) {
  const ext = filename.split('.').pop()?.toLowerCase();
  const types = {
    'jpg': 'image/jpeg', 'jpeg': 'image/jpeg', 'png': 'image/png', 'gif': 'image/gif',
    'mp4': 'video/mp4', 'mov': 'video/quicktime', 'avi': 'video/x-msvideo',
    'mp3': 'audio/mpeg', 'wav': 'audio/wav', 'pdf': 'application/pdf'
  };
  return types[ext] || 'application/octet-stream';
}
