export async function onRequest(context) {
  const { request, env } = context;

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type'
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    // Get admin key from environment variable
    const adminKey = env.KEYMSM || 'MARYA2025ADMIN';
    
    console.log('🔑 Admin key requested');
    
    return new Response(JSON.stringify({
      success: true,
      key: adminKey,
      timestamp: Date.now()
    }), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (error) {
    console.error('💥 Get key error:', error);
    return new Response(JSON.stringify({
      success: false,
      error: 'Failed to get admin key'
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}
