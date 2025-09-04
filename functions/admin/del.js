import { PASSCODE } from '../_config.js';

export async function onRequest({ request, env }) {
  const url = new URL(request.url);
  const code = url.searchParams.get('code');
  const key = url.searchParams.get('key');
  
  const headers = {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*'
  };
  
  if (code !== PASSCODE) {
    return new Response(JSON.stringify({ 
      success: false, 
      error: 'Invalid admin passcode' 
    }), {
      status: 401,
      headers
    });
  }

  if (!key) {
    return new Response(JSON.stringify({ 
      success: false, 
      error: 'No key provided' 
    }), {
      status: 400,
      headers
    });
  }

  try {
    await env.FILES_KV.delete(key);
    return new Response(JSON.stringify({ 
      success: true 
    }), {
      headers
    });
  } catch (error) {
    return new Response(JSON.stringify({ 
      success: false, 
      error: error.message 
    }), {
      status: 500,
      headers
    });
  }
}
