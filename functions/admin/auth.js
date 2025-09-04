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
    const { password } = await request.json();
    const ADMIN_PASS = env.ADMIN_PASS;
    
    if (!ADMIN_PASS) {
      return new Response(JSON.stringify({
        success: false,
        error: 'Admin password not configured'
      }), { status: 500, headers });
    }
    
    if (!password) {
      return new Response(JSON.stringify({
        success: false,
        error: 'Password required'
      }), { status: 400, headers });
    }
    
    if (password === ADMIN_PASS) {
      return new Response(JSON.stringify({
        success: true,
        message: 'Authentication successful'
      }), { headers });
    } else {
      return new Response(JSON.stringify({
        success: false,
        error: 'Invalid password'
      }), { status: 401, headers });
    }
    
  } catch (error) {
    return new Response(JSON.stringify({
      success: false,
      error: 'Server error: ' + error.message
    }), { status: 500, headers });
  }
}
