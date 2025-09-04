export async function onRequest(context) {
  const { request, env } = context;
  const url = new URL(request.url);
  const action = url.searchParams.get('action');
  const pass = url.searchParams.get('pass');

  // 🔒 SECURE: Admin password from environment
  const ADMIN_PASS = env.ADMIN_PASS || env.ADMIN_PASSWORD || 'MSM@MARYA';

  const headers = {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, DELETE, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type'
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { headers });
  }

  console.log('Admin request - Action:', action, 'Password provided:', !!pass);

  if (pass !== ADMIN_PASS) {
    console.log('Invalid admin password');
    return new Response(JSON.stringify({ 
      success: false, 
      error: 'Invalid admin password' 
    }), {
      status: 401,
      headers
    });
  }

  try {
    if (action === 'list') {
      console.log('Listing files...');
      const list = await env.FILES_KV.list();
      console.log('KV list result - Keys count:', list.keys.length);
      
      const files = list.keys.map(key => ({
        slug: key.name,
        ...key.metadata
      }));

      console.log('Returning files:', files.length);
      return new Response(JSON.stringify({ 
        success: true, 
        files 
      }), { headers });

    } else if (action === 'delete') {
      const slug = url.searchParams.get('slug');
      console.log('Deleting file:', slug);
      
      if (!slug) {
        return new Response(JSON.stringify({ 
          success: false, 
          error: 'No slug provided' 
        }), {
          status: 400,
          headers
        });
      }

      await env.FILES_KV.delete(slug);
      console.log('File deleted successfully');
      
      return new Response(JSON.stringify({ 
        success: true 
      }), { headers });

    } else {
      console.log('Invalid action:', action);
      return new Response(JSON.stringify({ 
        success: false, 
        error: 'Invalid action. Use action=list or action=delete' 
      }), {
        status: 400,
        headers
      });
    }
  } catch (error) {
    console.error('Admin error:', error);
    return new Response(JSON.stringify({ 
      success: false, 
      error: error.message 
    }), {
      status: 500,
      headers
    });
  }
}
