export async function onRequest(context) {
  const { request, env } = context;
  
  // Basic CORS
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Content-Type': 'application/json'
  };

  // Handle preflight
  if (request.method === 'OPTIONS') {
    return new Response(null, { headers });
  }

  // Only POST allowed
  if (request.method !== 'POST') {
    return new Response(JSON.stringify({
      success: false,
      error: 'Only POST method allowed'
    }), { status: 405, headers });
  }

  try {
    // Parse request
    const data = await request.json();
    const { password } = data;
    
    // Get environment variable
    const ADMIN_PASS = env.ADMIN_PASS;
    
    // Debug info (remove in production)
    console.log('Password received:', !!password);
    console.log('Admin pass exists:', !!ADMIN_PASS);
    
    // Check if admin pass is configured
    if (!ADMIN_PASS) {
      return new Response(JSON.stringify({
        success: false,
        error: 'ADMIN_PASS environment variable not set'
      }), { status: 500, headers });
    }
    
    // Check password
    if (password === ADMIN_PASS) {
      return new Response(JSON.stringify({
        success: true,
        message: 'Login successful'
      }), { headers });
    } else {
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
