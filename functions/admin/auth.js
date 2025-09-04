export async function onRequest(context) {
  const { request, env } = context;
  
  console.log('=== AUTH DEBUG INFO ===');
  console.log('Method:', request.method);
  console.log('URL:', request.url);
  console.log('Has env object:', !!env);
  console.log('Available env keys:', Object.keys(env || {}));
  console.log('ADMIN_PASSWORD exists:', !!env?.ADMIN_PASSWORD);
  console.log('ADMIN_PASSWORD value:', env?.ADMIN_PASSWORD ? '[HIDDEN]' : 'NOT_FOUND');
  
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Content-Type': 'application/json'
  };

  if (request.method === 'OPTIONS') {
    console.log('✅ OPTIONS request handled');
    return new Response(null, { headers });
  }

  if (request.method !== 'POST') {
    console.log('❌ Invalid method:', request.method);
    return new Response(JSON.stringify({
      success: false,
      error: 'Only POST method allowed'
    }), { status: 405, headers });
  }

  try {
    const requestBody = await request.text();
    console.log('Request body received:', !!requestBody);
    
    const { password } = JSON.parse(requestBody);
    console.log('Password provided:', !!password);
    console.log('Password length:', password?.length);
    
    // ✅ Multiple ways to access environment variable
    const ADMIN_PASSWORD = env.ADMIN_PASSWORD || env?.ADMIN_PASSWORD;
    
    console.log('Environment password found:', !!ADMIN_PASSWORD);
    
    // ✅ Fallback for testing if env var not working
    const FALLBACK_PASSWORD = 'Admin@MSM-Marya';
    const activePassword = ADMIN_PASSWORD || FALLBACK_PASSWORD;
    
    console.log('Using fallback password:', !ADMIN_PASSWORD);
    
    if (!password) {
      console.log('❌ No password provided');
      return new Response(JSON.stringify({
        success: false,
        error: 'Password is required'
      }), { status: 400, headers });
    }
    
    // ✅ Password comparison
    console.log('Comparing passwords...');
    const isValid = (password === activePassword);
    console.log('Password match:', isValid);
    
    if (isValid) {
      console.log('✅ Authentication successful');
      return new Response(JSON.stringify({
        success: true,
        message: 'Authentication successful',
        env_used: !!ADMIN_PASSWORD
      }), { headers });
    } else {
      console.log('❌ Authentication failed');
      return new Response(JSON.stringify({
        success: false,
        error: 'Invalid password'
      }), { status: 401, headers });
    }
    
  } catch (error) {
    console.error('❌ Auth error:', error);
    return new Response(JSON.stringify({
      success: false,
      error: 'Server error: ' + error.message,
      debug: {
        env_available: !!env,
        env_keys: Object.keys(env || {})
      }
    }), { status: 500, headers });
  }
}
