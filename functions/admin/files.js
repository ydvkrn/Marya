const ADMIN_PASS = 'MSM@MARYA';

export async function onRequest(context) {
  const { request, env } = context;
  const url = new URL(request.url);
  const action = url.searchParams.get('action');
  const pass = url.searchParams.get('pass');

  const headers = {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, DELETE, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type'
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { headers });
  }

  if (pass !== ADMIN_PASS) {
    return new Response(JSON.stringify({ success: false, error: 'Invalid admin password' }), {
      status: 401,
      headers
    });
  }

  try {
    if (action === 'list') {
      const list = await env.FILES_KV.list();
      const files = list.keys.map(key => ({
        slug: key.name,
        ...key.metadata
      }));

      return new Response(JSON.stringify({ success: true, files }), { headers });

    } else if (action === 'delete') {
      const slug = url.searchParams.get('slug');
      if (!slug) {
        return new Response(JSON.stringify({ success: false, error: 'No slug provided' }), {
          status: 400,
          headers
        });
      }

      await env.FILES_KV.delete(slug);
      return new Response(JSON.stringify({ success: true }), { headers });

    } else {
      return new Response(JSON.stringify({ success: false, error: 'Invalid action' }), {
        status: 400,
        headers
      });
    }
  } catch (error) {
    console.error('Admin error:', error);
    return new Response(JSON.stringify({ success: false, error: error.message }), {
      status: 500,
      headers
    });
  }
}
