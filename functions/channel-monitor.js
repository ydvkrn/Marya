export async function onRequest(context) {
  const { request, env } = context;
  
  // ✅ Handle all requests properly
  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type'
  };

  console.log('=== WEBHOOK REQUEST ===');
  console.log('Method:', request.method);
  console.log('URL:', request.url);

  // Handle OPTIONS preflight
  if (request.method === 'OPTIONS') {
    return new Response('OK', { 
      status: 200, 
      headers: corsHeaders 
    });
  }

  // Handle GET requests (for testing)
  if (request.method === 'GET') {
    return new Response('Webhook is working! 🚀', { 
      status: 200, 
      headers: corsHeaders 
    });
  }

  // Handle POST requests (Telegram webhooks)
  if (request.method === 'POST') {
    try {
      const update = await request.json();
      console.log('Telegram update:', JSON.stringify(update));

      // Simple response to avoid 405
      return new Response('OK', { 
        status: 200, 
        headers: corsHeaders 
      });

    } catch (error) {
      console.error('Webhook error:', error);
      return new Response('Error processed', { 
        status: 200, 
        headers: corsHeaders 
      });
    }
  }

  // Fallback for other methods
  return new Response('Method handled', { 
    status: 200, 
    headers: corsHeaders 
  });
}
