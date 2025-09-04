const ADMIN_PASS = 'MSM@MARYA';

export async function onRequest(context) {
  const { request, env } = context;
  const url = new URL(request.url);
  const pass = url.searchParams.get('pass');
  const slug = url.searchParams.get('slug');

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

  if (!slug) {
    return new Response(JSON.stringify({ success: false, error: 'No slug provided' }), {
      status: 400,
      headers
    });
  }

  try {
    await env.FILES_KV.delete(slug);
    return new Response(JSON.stringify({ success: true }), { headers });
  } catch (error) {
    return new Response(JSON.stringify({ success: false, error: error.message }), {
      status: 500,
      headers
    });
  }
}
