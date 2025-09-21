export async function onRequest(context) {
  const { request, env } = context;

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type'
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const url = new URL(request.url);
    const action = url.searchParams.get('action') || 'info';

    if (action === 'clear') {
      // Clear cache
      const cleared = chunkCache.size;
      chunkCache.clear();
      
      return new Response(JSON.stringify({
        success: true,
        message: `Cleared ${cleared} cached chunks`,
        cacheSize: 0
      }), {
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    // Return cache info
    return new Response(JSON.stringify({
      success: true,
      cacheSize: chunkCache.size,
      maxCacheSize: 50,
      cacheTTL: '30 minutes',
      cacheKeys: Array.from(chunkCache.keys()).slice(0, 10) // First 10 keys
    }), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (error) {
    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}
