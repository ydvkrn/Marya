// functions/api-upload.js
export async function onRequest(context) {
  const { request, env } = context;
  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, Accept',
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: corsHeaders });
  }

  // Accept GET requests with ?url= and optional ?filename=
  const urlObj = new URL(request.url);
  const fileUrl = urlObj.searchParams.get('url') || urlObj.searchParams.get('fileUrl');
  const filename = urlObj.searchParams.get('filename') || urlObj.searchParams.get('name');

  if (!fileUrl) {
    return new Response(
      JSON.stringify({ success: false, error: { message: 'Missing url or fileUrl parameter' } }),
      { status: 400, headers: { 'Content-Type': 'application/json', ...corsHeaders } }
    );
  }

  // Forward request to upload-from-url endpoint as POST
  try {
    const apiUrl = urlObj.origin + '/functions/upload-from-url';

    const response = await fetch(apiUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
      body: JSON.stringify({ fileUrl, filename }),
    });

    const data = await response.text();

    return new Response(data, {
      status: response.status,
      headers: { 'Content-Type': 'application/json', ...corsHeaders },
    });
  } catch (err) {
    return new Response(
      JSON.stringify({ success: false, error: { message: err.message || 'Internal error' } }),
      { status: 500, headers: { 'Content-Type': 'application/json', ...corsHeaders } }
    );
  }
}