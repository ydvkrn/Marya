// functions/upload-from-url-direct.js
export async function onRequest(context) {
  const { request } = context;

  // Allow CORS / OPTIONS
  if (request.method === 'OPTIONS') {
    return new Response(null, {
      status: 204,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization, Accept'
      }
    });
  }

  // Accept GET and POST both
  const urlObj = new URL(request.url);
  // try fileUrl or url as param
  const fileUrl = urlObj.searchParams.get('fileUrl') || urlObj.searchParams.get('url');
  const filename = urlObj.searchParams.get('filename') || urlObj.searchParams.get('name');

  if (!fileUrl) {
    return new Response(JSON.stringify({
      success: false, error: { message: 'Missing ?fileUrl= or ?url=' }
    }), { status: 400, headers: { 'Content-Type': 'application/json' }});
  }

  // Forward the request to the real function as POST JSON
  const forwardBody = {
    fileUrl,
    filename: filename || undefined
  };

  const resp = await fetch(urlObj.origin + '/functions/upload-from-url', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
    body: JSON.stringify(forwardBody)
  });

  const result = await resp.text();
  return new Response(result, {
    status: resp.status,
    headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }
  });
}