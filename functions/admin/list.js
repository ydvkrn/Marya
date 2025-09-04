import { PASSCODE } from '../_config.js';

export async function onRequest({ request, env }) {
  const url = new URL(request.url);
  const code = url.searchParams.get('code');
  
  if (code !== PASSCODE) {
    return new Response(JSON.stringify({ success: false, error: 'Invalid code' }), {
      status: 401,
      headers: { 'Content-Type': 'application/json' }
    });
  }

  try {
    const list = await env.FILES_KV.list();
    const items = list.keys.map(key => ({
      slug: key.name,
      metadata: key.metadata,
      extension: key.name.includes('.') ? key.name.split('.').pop().toUpperCase() : 'FILE'
    }));

    return new Response(JSON.stringify({ success: true, items }), {
      headers: { 'Content-Type': 'application/json' }
    });
  } catch (error) {
    return new Response(JSON.stringify({ success: false, error: error.message }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' }
    });
  }
}
