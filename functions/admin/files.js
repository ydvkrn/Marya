export async function onRequest(context) {
  const { env } = context;

  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Content-Type': 'application/json'
  };

  if (context.request.method === 'OPTIONS') {
    return new Response(null, { headers });
  }

  try {
    if (!env.FILES_KV) {
      throw new Error('FILES_KV binding not found');
    }

    const listResult = await env.FILES_KV.list();
    
    const files = listResult.keys.map(key => ({
      name: key.name,
      metadata: key.metadata
    }));

    files.sort((a, b) => {
      const dateA = a.metadata?.uploadedAt || 0;
      const dateB = b.metadata?.uploadedAt || 0;
      return dateB - dateA;
    });

    return new Response(JSON.stringify({
      success: true,
      files: files,
      total: files.length
    }), { headers });

  } catch (error) {
    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), { status: 500, headers });
  }
}
