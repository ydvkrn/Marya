export async function onRequest(context) {
  const { request, env } = context;
  
  // ✅ Complete CORS Headers
  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With',
    'Access-Control-Max-Age': '86400',
  };

  // ✅ Handle OPTIONS (Preflight) Request
  if (request.method === 'OPTIONS') {
    console.log('✅ OPTIONS preflight request handled');
    return new Response(null, {
      status: 200,
      headers: corsHeaders
    });
  }

  // ✅ Handle POST Request
  if (request.method === 'POST') {
    console.log('📡 POST request received');
    
    try {
      const requestBody = await request.text();
      console.log('Request body length:', requestBody.length);
      
      if (!requestBody) {
        return new Response(JSON.stringify({
          success: false,
          error: 'Request body is required'
        }), {
          status: 400,
          headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
      }

      const { password } = JSON.parse(requestBody);
      console.log('Password provided:', !!password);
      
      // ✅ Get Environment Variable
      const ADMIN_PASSWORD = env.ADMIN_PASSWORD;
      console.log('Environment variable exists:', !!ADMIN_PASSWORD);
      
      // ✅ Fallback Password for Testing
      const EXPECTED_PASSWORD = ADMIN_PASSWORD || 'Admin@MSM-Marya';
      
      if (!password) {
        return new Response(JSON.stringify({
          success: false,
          error: 'Password is required'
        }), {
          status: 400,
          headers: { 'Content-Type': 'application/json', ...corsHeaders }
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
          headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
      } else {
        console.log('❌ Invalid password');
        return new Response(JSON.stringify({
          success: false,
          error: 'Invalid password'
        }), {
          status: 401,
          headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
      }

    } catch (error) {
      console.error('❌ Server error:', error);
      return new Response(JSON.stringify({
        success: false,
        error: 'Server error: ' + error.message
      }), {
        status: 500,
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }
  }

  // ✅ Handle Other Methods
  console.log('❌ Method not allowed:', request.method);
  return new Response(JSON.stringify({
    success: false,
    error: `Method ${request.method} not allowed`
  }), {
    status: 405,
    headers: { 
      'Content-Type': 'application/json',
      'Allow': 'POST, OPTIONS',
      ...corsHeaders 
    }
  });
}
