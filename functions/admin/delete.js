export async function onRequest(context) {
  const { request, env } = context;

  if (request.method !== 'POST') {
    return new Response('Method not allowed', { status: 405 });
  }

  try {
    const { fileId } = await request.json();
    
    if (!fileId) {
      throw new Error('File ID is required');
    }

    // Delete from KV
    await env.FILES_KV.delete(fileId);

    return new Response(JSON.stringify({
      success: true,
      message: 'File deleted successfully'
    }), {
      headers: { 'Content-Type': 'application/json' }
    });

  } catch (error) {
    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' }
    });
  }
}
