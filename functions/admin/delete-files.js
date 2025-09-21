export async function onRequest(context) {
  const { request, env } = context;

  // CORS Headers
  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization'
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  if (request.method !== 'POST') {
    return new Response(JSON.stringify({
      success: false,
      error: 'Method not allowed'
    }), {
      status: 405,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }

  try {
    console.log('🗑️ Admin delete-files API called');

    // Auth check
    const authHeader = request.headers.get('Authorization');
    if (!authHeader || !authHeader.includes('MARYA2025ADMIN')) {
      console.log('❌ Unauthorized delete attempt');
      return new Response(JSON.stringify({
        success: false,
        error: 'Unauthorized access'
      }), {
        status: 401,
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    // Parse request body
    let requestData;
    try {
      const requestText = await request.text();
      console.log('📝 Request body:', requestText);
      
      if (!requestText || requestText.trim() === '') {
        throw new Error('Empty request body');
      }
      
      requestData = JSON.parse(requestText);
    } catch (parseError) {
      console.error('❌ Failed to parse request:', parseError);
      return new Response(JSON.stringify({
        success: false,
        error: 'Invalid JSON in request body'
      }), {
        status: 400,
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    const { fileIds } = requestData;
    console.log('🗑️ Parsed fileIds:', fileIds);
    
    if (!fileIds || !Array.isArray(fileIds) || fileIds.length === 0) {
      console.error('❌ No valid file IDs provided:', fileIds);
      return new Response(JSON.stringify({
        success: false,
        error: 'No file IDs provided or invalid format'
      }), {
        status: 400,
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    console.log(`🗑️ Deleting ${fileIds.length} files: ${fileIds.join(', ')}`);

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
    let errors = [];
    let deletedDetails = [];

    for (const fileId of fileIds) {
      try {
        console.log(`🗑️ Processing deletion: ${fileId}`);

        // Find the file metadata
        let fileMetadata = null;
        let sourceKV = null;

        for (const kvNamespace of kvNamespaces) {
          try {
            const metadata = await kvNamespace.kv.get(fileId);
            if (metadata) {
              fileMetadata = JSON.parse(metadata);
              sourceKV = kvNamespace;
              console.log(`📁 Found ${fileId} in ${kvNamespace.name}`);
              break;
            }
          } catch (e) {
            continue;
          }
        }

        if (!fileMetadata) {
          console.log(`⚠️ File ${fileId} not found in any KV namespace`);
          errors.push(`File ${fileId} not found`);
          continue;
        }

        let chunksDeleted = 0;

        // Delete all chunks if they exist
        if (fileMetadata.chunks && Array.isArray(fileMetadata.chunks)) {
          console.log(`🗑️ Deleting ${fileMetadata.chunks.length} chunks for ${fileId}...`);

          for (const chunkInfo of fileMetadata.chunks) {
            try {
              const chunkKV = kvNamespaces.find(ns => ns.name === chunkInfo.kvNamespace);
              if (chunkKV && chunkInfo.keyName) {
                await chunkKV.kv.delete(chunkInfo.keyName);
                chunksDeleted++;
                console.log(`✅ Deleted chunk: ${chunkInfo.keyName}`);
              }
            } catch (chunkError) {
              console.error(`❌ Failed to delete chunk ${chunkInfo.keyName}:`, chunkError.message);
              errors.push(`Failed to delete chunk ${chunkInfo.keyName}: ${chunkError.message}`);
            }
          }
        }

        // Delete main file metadata
        await sourceKV.kv.delete(fileId);
        deletedCount++;
        
        deletedDetails.push({
          fileId: fileId,
          filename: fileMetadata.filename || 'Unknown',
          chunksDeleted: chunksDeleted,
          kvNamespace: sourceKV.name
        });
        
        console.log(`✅ Successfully deleted file: ${fileId} (${chunksDeleted} chunks)`);

      } catch (fileError) {
        console.error(`❌ Failed to delete file ${fileId}:`, fileError.message);
        errors.push(`Failed to delete ${fileId}: ${fileError.message}`);
      }
    }

    console.log(`✅ Deletion summary: ${deletedCount}/${fileIds.length} files deleted`);

    return new Response(JSON.stringify({
      success: true,
      deletedCount: deletedCount,
      totalRequested: fileIds.length,
      deletedDetails: deletedDetails,
      errors: errors.length > 0 ? errors : undefined,
      timestamp: Date.now()
    }), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (error) {
    console.error('💥 Admin delete-files error:', error);
    return new Response(JSON.stringify({
      success: false,
      error: error.message,
      stack: error.stack,
      timestamp: Date.now()
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}
