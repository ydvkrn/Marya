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
    console.log('🔑 GET-KEY API CALLED');
    console.log('🔑 Environment check:', {
      hasKEYMSM: !!env.KEYMSM,
      KEYMSM_value: env.KEYMSM,
      KEYMSM_length: env.KEYMSM ? env.KEYMSM.length : 0
    });
    
    // Get admin key from environment variable
    const adminKey = env.KEYMSM || 'MARYA2025ADMIN';
    
    console.log('🔑 Final admin key:', {
      key: adminKey,
      length: adminKey.length,
      source: env.KEYMSM ? 'environment' : 'fallback'
    });
    
    return new Response(JSON.stringify({
      success: true,
      key: adminKey,
      timestamp: Date.now(),
      debug: {
        environmentVariableExists: !!env.KEYMSM,
        keyLength: adminKey.length,
        keySource: env.KEYMSM ? 'environment' : 'fallback'
      }
    }), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (error) {
    console.error('💥 Get key error:', error);
    return new Response(JSON.stringify({
      success: false,
      error: error.message,
      fallbackKey: 'MARYA2025ADMIN',
      timestamp: Date.now()
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}
