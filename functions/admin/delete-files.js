export async function onRequest(context) {
  const { request, env } = context;

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization'
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    // Quick auth check
    const authHeader = request.headers.get('Authorization');
    const adminKey = env.KEYMSM || 'MSMxMarya7';
    
    if (!authHeader || !authHeader.includes(adminKey)) {
      return new Response(JSON.stringify({
        success: false,
        error: 'Unauthorized'
      }), {
        status: 401,
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    const { fileIds } = await request.json();
    
    if (!Array.isArray(fileIds) || fileIds.length === 0) {
      return new Response(JSON.stringify({
        success: false,
        error: 'No file IDs provided'
      }), {
        status: 400,
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    console.log(`🗑️ Deleting ${fileIds.length} files:`, fileIds);

    // Get KV namespaces
    const kvs = [
      env.FILES_KV, env.FILES_KV2, env.FILES_KV3, env.FILES_KV4,
      env.FILES_KV5, env.FILES_KV6, env.FILES_KV7
    ].filter(kv => kv);

    let deleted = 0;
    let errors = [];

    // Process each file
    for (const fileId of fileIds) {
      if (!fileId || typeof fileId !== 'string') {
        errors.push(`Invalid file ID: ${fileId}`);
        continue;
      }

      try {
        let found = false;
        let fileData = null;

        // Find file in KVs
        for (const kv of kvs) {
          try {
            const data = await kv.get(fileId);
            if (data) {
              fileData = JSON.parse(data);
              found = true;
              
              // Delete all chunks
              if (fileData.chunks && Array.isArray(fileData.chunks)) {
                for (const chunk of fileData.chunks) {
                  if (chunk.keyName) {
                    const chunkKV = kvs.find((_, i) => `FILES_KV${i === 0 ? '' : i + 1}` === chunk.kvNamespace);
                    if (chunkKV) {
                      await chunkKV.delete(chunk.keyName);
                    }
                  }
                }
              }
              
              // Delete main file
              await kv.delete(fileId);
              deleted++;
              console.log(`✅ Deleted: ${fileId}`);
              break;
            }
          } catch (kvError) {
            console.error(`KV error for ${fileId}:`, kvError.message);
          }
        }

        if (!found) {
          errors.push(`File not found: ${fileId}`);
        }

      } catch (fileError) {
        console.error(`File error for ${fileId}:`, fileError.message);
        errors.push(`Failed to delete ${fileId}: ${fileError.message}`);
      }
    }

    console.log(`✅ Deletion complete: ${deleted}/${fileIds.length} deleted`);

    return new Response(JSON.stringify({
      success: true,
      deletedCount: deleted,
      totalRequested: fileIds.length,
      errors: errors.length > 0 ? errors : undefined
    }), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (error) {
    console.error('💥 Delete API error:', error);
    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}
