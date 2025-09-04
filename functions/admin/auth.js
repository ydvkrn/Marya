export async function onRequest(context) {
  const { request, env } = context;
  
  console.log('=== SECURE AUTH REQUEST ===');
  console.log('Method:', request.method);
  console.log('Has env:', !!env);
  console.log('Has ADMIN_PASSWORD:', !!env.ADMIN_PASSWORD);
  
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
    const { password } = await request.json();
    
    // ✅ SECURE: Access secret environment variable
    const ADMIN_PASSWORD = env.ADMIN_PASSWORD;
    
    console.log('Password provided:', !!password);
    console.log('Environment variable configured:', !!ADMIN_PASSWORD);
    
    if (!ADMIN_PASSWORD) {
      console.error('❌ ADMIN_PASSWORD environment variable not configured');
      return new Response(JSON.stringify({
        success: false,
        error: 'Server configuration error'
      }), { status: 500, headers });
    }
    
    if (!password) {
      return new Response(JSON.stringify({
        success: false,
        error: 'Password required'
      }), { status: 400, headers });
    }
    
    // ✅ SECURE: Compare with environment variable
    if (password === ADMIN_PASSWORD) {
      console.log('✅ Authentication successful');
      return new Response(JSON.stringify({
        success: true,
        message: 'Authentication successful'
      }), { headers });
    } else {
      console.log('❌ Authentication failed - Invalid password');
      return new Response(JSON.stringify({
        success: false,
        error: 'Invalid password'
      }), { status: 401, headers });
    }
    
  } catch (error) {
    console.error('Auth error:', error);
    return new Response(JSON.stringify({
      success: false,
      error: 'Server error: ' + error.message
    }), { status: 500, headers });
  }
}
