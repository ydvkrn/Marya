export async function onRequest(context) {
  const { request, env } = context;
  
  console.log('🔐 Auth function called');
  console.log('Method:', request.method);
  console.log('URL:', request.url);
  
  // ✅ Complete CORS Headers
  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With, Accept',
    'Access-Control-Max-Age': '86400',
    'Content-Type': 'application/json'
  };

  // ✅ Handle OPTIONS (Preflight) - MANDATORY for CORS
  if (request.method === 'OPTIONS') {
    console.log('✅ OPTIONS preflight handled');
    return new Response(null, {
      status: 200,
      headers: corsHeaders
    });
  }

  // ✅ Handle POST Request
  if (request.method === 'POST') {
    console.log('📡 POST request received');
    
    try {
      // Parse request body
      const requestBody = await request.text();
      console.log('Request body received:', !!requestBody);
      
      if (!requestBody) {
        return new Response(JSON.stringify({
          success: false,
          error: 'Request body required'
        }), {
          status: 400,
          headers: corsHeaders
        });
      }

      const { password } = JSON.parse(requestBody);
      console.log('Password provided:', !!password);
      
      // ✅ Environment Variable Access
      const ADMIN_PASSWORD = env.ADMIN_PASSWORD;
      console.log('Environment variable exists:', !!ADMIN_PASSWORD);
      
      // ✅ Fallback Password (Remove after testing)
      const EXPECTED_PASSWORD = ADMIN_PASSWORD || 'Admin@MSM-Marya';
      console.log('Using fallback:', !ADMIN_PASSWORD);
      
      if (!password) {
        return new Response(JSON.stringify({
          success: false,
          error: 'Password is required'
        }), {
          status: 400,
          headers: corsHeaders
        });
      }

      // ✅ Password Verification
      if (password === EXPECTED_PASSWORD) {
        console.log('✅ Authentication successful');
        return new Response(JSON.stringify({
          success: true,
          message: 'Authentication successful',
          timestamp: Date.now()
        }), {
          status: 200,
          headers: corsHeaders
        });
      } else {
        console.log('❌ Invalid password attempt');
        return new Response(JSON.stringify({
          success: false,
          error: 'Invalid password'
        }), {
          status: 401,
          headers: corsHeaders
        });
      }

    } catch (error) {
      console.error('❌ Server error:', error);
      return new Response(JSON.stringify({
        success: false,
        error: 'Server error: ' + error.message,
        debug: error.stack
      }), {
        status: 500,
        headers: corsHeaders
      });
    }
  }

  // ✅ Handle GET Request (Health Check)
  if (request.method === 'GET') {
    return new Response(JSON.stringify({
      success: true,
      message: 'Auth service is running',
      timestamp: Date.now()
    }), {
      status: 200,
      headers: corsHeaders
    });
  }

  // ✅ Method Not Allowed
  console.log('❌ Method not supported:', request.method);
  return new Response(JSON.stringify({
    success: false,
    error: `Method ${request.method} not allowed`,
    allowed_methods: ['GET', 'POST', 'OPTIONS']
  }), {
    status: 405,
    headers: { 
      'Allow': 'GET, POST, OPTIONS',
      ...corsHeaders 
    }
  });
}
