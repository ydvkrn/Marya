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

  // Auth check
  const authHeader = request.headers.get('Authorization');
  const adminKey = env.ADMIN_KEY || 'MARYA2025ADMIN';
  
  if (!authHeader || !authHeader.includes(adminKey)) {
    return new Response(JSON.stringify({
      success: false,
      error: 'Unauthorized access'
    }), {
      status: 401,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }

  try {
    const { fileIds } = await request.json();
    
    if (!Array.isArray(fileIds) || fileIds.length === 0) {
      throw new Error('No file IDs provided');
    }

    console.log(`🗑️ Deleting ${fileIds.length} files...`);

    const kvNamespaces = [
      { kv: env.FILES_KV, name: 'FILES_KV' },
      { kv: env.FILES_KV2, name: 'FILES_KV2' },
      { kv: env.FILES_KV3, name: 'FILES_KV3' },
      { kv: env.FILES_KV4, name: 'FILES_KV4' },
      { kv: env.FILES_KV5, name: 'FILES_KV5' },
      { kv: env.FILES_KV6, name: 'FILES_KV6' },
      { kv: env.FILES_KV7, name: 'FILES_KV7' }
    ].filter(item => item.kv);

    let deletedCount = 0;

    for (const fileId of fileIds) {
      try {
        console.log(`🗑️ Deleting file: ${fileId}`);

        // Find the file metadata
        let fileMetadata = null;
        let sourceKV = null;

        for (const kvNamespace of kvNamespaces) {
          try {
            const metadata = await kvNamespace.kv.get(fileId);
            if (metadata) {
              fileMetadata = JSON.parse(metadata);
              sourceKV = kvNamespace;
              break;
            }
          } catch (e) {
            continue;
          }
        }

        if (!fileMetadata) {
          console.log(`⚠️ File ${fileId} not found, skipping...`);
          continue;
        }

        // Delete all chunks if they exist
        if (fileMetadata.chunks && Array.isArray(fileMetadata.chunks)) {
          console.log(`🗑️ Deleting ${fileMetadata.chunks.length} chunks for ${fileId}...`);

          for (const chunkInfo of fileMetadata.chunks) {
            try {
              const chunkKV = kvNamespaces.find(ns => ns.name === chunkInfo.kvNamespace);
              if (chunkKV && chunkInfo.keyName) {
                await chunkKV.kv.delete(chunkInfo.keyName);
                console.log(`✅ Deleted chunk: ${chunkInfo.keyName}`);
              }
            } catch (chunkError) {
              console.error(`❌ Failed to delete chunk ${chunkInfo.keyName}:`, chunkError);
            }
          }
        }

        // Delete main file metadata
        await sourceKV.kv.delete(fileId);
        deletedCount++;
        console.log(`✅ Deleted file metadata: ${fileId}`);

      } catch (fileError) {
        console.error(`❌ Failed to delete file ${fileId}:`, fileError);
      }
    }

    console.log(`✅ Deletion complete: ${deletedCount}/${fileIds.length} files deleted`);

    return new Response(JSON.stringify({
      success: true,
      deletedCount: deletedCount,
      totalRequested: fileIds.length
    }), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (error) {
    console.error('💥 Delete files error:', error);
    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}
