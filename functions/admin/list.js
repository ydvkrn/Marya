import { PASSCODE } from '../_config.js';

export async function onRequest({ request, env }) {
  const url = new URL(request.url);
  const code = url.searchParams.get('code');
  
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

  try {
    const list = await env.FILES_KV.list();
    const items = [];
    
    for (const key of list.keys) {
      items.push({
        key: key.name,
        metadata: key.metadata
      });
    }

    return new Response(JSON.stringify({ 
      success: true, 
      items: items 
    }), {
      headers
    });
  } catch (error) {
    console.error('Admin list error:', error);
    return new Response(JSON.stringify({ 
      success: false, 
      error: error.message 
    }), {
      status: 500,
      headers
    });
  }
}
