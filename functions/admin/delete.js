export async function onRequest(context) {
  const { request, env } = context;

  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Content-Type': 'application/json'
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { headers });
  }

  if (request.method !== 'POST') {
    return new Response(JSON.stringify({
      success: false,
      error: 'Only POST method allowed'
    }), { status: 405, headers });
  }

  try {
    const { fileId } = await request.json();
    
    if (!fileId) {
      throw new Error('File ID required');
    }

    if (!env.FILES_KV) {
      throw new Error('FILES_KV binding not found');
    }

    await env.FILES_KV.delete(fileId);

    return new Response(JSON.stringify({
      success: true,
      message: 'File deleted successfully'
    }), { headers });

  } catch (error) {
    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), { status: 500, headers });
  }
}
