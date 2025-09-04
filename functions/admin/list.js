const ADMIN_PASS = 'MSM@MARYA';

export async function onRequest(context) {
  const { request, env } = context;
  const url = new URL(request.url);
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
    const list = await env.FILES_KV.list();
    const files = list.keys.map(key => ({
      slug: key.name,
      ...key.metadata
    }));

    return new Response(JSON.stringify({ success: true, files }), { headers });
  } catch (error) {
    return new Response(JSON.stringify({ success: false, error: error.message }), {
      status: 500,
      headers
    });
  }
}
