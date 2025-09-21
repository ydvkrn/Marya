export async function onRequest(context) {
  const { request, env } = context;

  if (request.method !== 'POST') {
    return new Response('Method Not Allowed', { status: 405 });
  }

  try {
    const { url } = await request.json();
    
    if (!url || !url.startsWith('http')) {
      throw new Error('Invalid URL provided');
    }

    console.log('Downloading from URL:', url);

    // Download file from URL
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`Failed to download file: ${response.status}`);
    }

    // Get filename from URL
    const filename = url.split('/').pop().split('?')[0] || 'download';
    const contentType = response.headers.get('Content-Type') || 'application/octet-stream';
    
    // Convert to File object
    const fileBuffer = await response.arrayBuffer();
    const file = new File([fileBuffer], filename, { type: contentType });

    // Create form data and forward to upload endpoint
    const formData = new FormData();
    formData.append('file', file);

    // Forward to existing upload handler
    const uploadRequest = new Request(new URL('/upload', request.url), {
      method: 'POST',
      body: formData,
      headers: {
        'Accept': 'application/json'
      }
    });

    return await context.env.ASSETS.fetch(uploadRequest);

  } catch (error) {
    console.error('URL upload error:', error);
    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' }
    });
  }
}
