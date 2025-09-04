import { ADMIN_PASS } from './config.js';

export async function onRequest(context) {
  const { request, env } = context;
  const url = new URL(request.url);
  const action = url.searchParams.get('action');
  const pass = url.searchParams.get('pass');
  
  const headers = {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*'
  };

  if (pass !== ADMIN_PASS) {
    return new Response(JSON.stringify({ success: false, error: 'Invalid password' }), {
      status: 401,
      headers
    });
  }

  try {
    if (action === 'list') {
      // List all files
      const list = await env.VAULT_KV.list();
      const files = list.keys.map(key => ({
        slug: key.name,
        ...key.metadata
      }));
      
      return new Response(JSON.stringify({ success: true, files }), { headers });
      
    } else if (action === 'delete') {
      // Delete file
      const slug = url.searchParams.get('slug');
      if (!slug) {
        return new Response(JSON.stringify({ success: false, error: 'No slug provided' }), {
          status: 400,
          headers
        });
      }
      
      await env.VAULT_KV.delete(slug);
      return new Response(JSON.stringify({ success: true }), { headers });
      
    } else {
      return new Response(JSON.stringify({ success: false, error: 'Invalid action' }), {
        status: 400,
        headers
      });
    }
    
  } catch (error) {
    return new Response(JSON.stringify({ success: false, error: error.message }), {
      status: 500,
      headers
    });
  }
}
