export async function onRequest(context) {
  const { env } = context;

  try {
    // List all keys with metadata
    const listResult = await env.FILES_KV.list();
    
    const files = listResult.keys.map(key => ({
      name: key.name,
      metadata: key.metadata
    }));

    // Sort by upload date (newest first)
    files.sort((a, b) => {
      const dateA = a.metadata?.uploadedAt || 0;
      const dateB = b.metadata?.uploadedAt || 0;
      return dateB - dateA;
    });

    return new Response(JSON.stringify({
      success: true,
      files: files,
      total: files.length
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
