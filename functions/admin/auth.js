export async function onRequest(context) {
  const { request, env } = context;
  
  // Debug logging
  console.log('=== ADMIN AUTH DEBUG ===');
  console.log('Method:', request.method);
  console.log('URL:', request.url);
  console.log('Has ADMIN_PASS:', !!env.ADMIN_PASS);
  
  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type'
  };

  if (request.method === 'OPTIONS') {
    console.log('OPTIONS request handled');
    return new Response(null, { headers: corsHeaders });
  }

  if (request.method !== 'POST') {
    console.log('Invalid method:', request.method);
    return new Response(JSON.stringify({ 
      success: false, 
      error: 'Method not allowed' 
    }), { 
      status: 405,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }

  try {
    const body = await request.text();
    console.log('Request body:', body);
    
    const { password } = JSON.parse(body);
    const ADMIN_PASS = env.ADMIN_PASS;
    
    console.log('Password received:', !!password);
    console.log('Admin pass configured:', !!ADMIN_PASS);
    console.log('Password length:', password?.length);
    console.log('Admin pass length:', ADMIN_PASS?.length);

    if (!ADMIN_PASS) {
      console.log('ERROR: Admin password not configured');
      return new Response(JSON.stringify({ 
        success: false, 
        error: 'Admin password not configured in environment' 
      }), {
        status: 500,
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    if (!password) {
      console.log('ERROR: No password provided');
      return new Response(JSON.stringify({ 
        success: false, 
        error: 'Password is required' 
      }), {
        status: 400,
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    console.log('Comparing passwords...');
    if (password === ADMIN_PASS) {
      console.log('✅ Password match - Login successful');
      return new Response(JSON.stringify({ 
        success: true,
        message: 'Authentication successful' 
      }), {
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    } else {
      console.log('❌ Password mismatch - Login failed');
      return new Response(JSON.stringify({ 
        success: false, 
        error: 'Invalid password' 
      }), {
        status: 401,
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }
  } catch (error) {
    console.error('Auth error:', error);
    return new Response(JSON.stringify({ 
      success: false, 
      error: 'Authentication failed: ' + error.message 
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}
