export async function onRequest(context) {
  const { request, env } = context;

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type'
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  if (request.method !== 'POST') {
    return new Response(JSON.stringify({
      success: false,
      error: 'Method not allowed'
    }), {
      status: 405,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }

  try {
    const { url } = await request.json();
    
    if (!url || !url.startsWith('http')) {
      return new Response(JSON.stringify({
        success: false,
        error: 'Invalid URL provided'
      }), {
        status: 400,
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    console.log('Downloading from URL:', url);

    // Download file from URL
    const response = await fetch(url);
    if (!response.ok) {
      return new Response(JSON.stringify({
        success: false,
        error: `Failed to download file: ${response.status}`
      }), {
        status: 400,
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
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

    // Create new request to upload endpoint
    const uploadUrl = new URL('/upload', request.url);
    const uploadRequest = new Request(uploadUrl, {
      method: 'POST',
      body: formData
    });

    // Forward to upload handler
    const uploadContext = {
      request: uploadRequest,
      env: env
    };

    // Import and call upload function
    const { onRequest: uploadHandler } = await import('./upload.js');
    const uploadResponse = await uploadHandler(uploadContext);
    
    // Return the upload response
    return uploadResponse;

  } catch (error) {
    console.error('URL upload error:', error);
    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}
