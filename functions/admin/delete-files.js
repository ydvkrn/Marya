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
    console.log('🗑️ DELETE API CALLED - Starting debug...');

    // Get admin key from environment
    const adminKey = env.KEYMSM || 'MARYA2025ADMIN';
    const authHeader = request.headers.get('Authorization');
    
    console.log('🔑 Auth check:', {
      hasAuthHeader: !!authHeader,
      adminKeyExists: !!adminKey,
      authHeaderSample: authHeader ? authHeader.substring(0, 20) + '...' : 'none'
    });
    
    if (!authHeader || !authHeader.includes(adminKey)) {
      console.log('❌ AUTH FAILED');
      return new Response(JSON.stringify({
        success: false,
        error: 'Unauthorized access'
      }), {
        status: 401,
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    console.log('✅ AUTH SUCCESS');

    // Debug request body parsing
    const requestText = await request.text();
    console.log('📝 RAW REQUEST BODY:', {
      length: requestText.length,
      content: requestText,
      isEmpty: !requestText || requestText.trim() === ''
    });

    if (!requestText || requestText.trim() === '') {
      console.log('❌ EMPTY REQUEST BODY');
      return new Response(JSON.stringify({
        success: false,
        error: 'Empty request body received'
      }), {
        status: 400,
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    let requestData;
    try {
      requestData = JSON.parse(requestText);
      console.log('📊 PARSED REQUEST DATA:', requestData);
    } catch (parseError) {
      console.log('❌ JSON PARSE ERROR:', parseError.message);
      return new Response(JSON.stringify({
        success: false,
        error: `JSON parse error: ${parseError.message}`
      }), {
        status: 400,
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    const { fileIds } = requestData;
    console.log('🔍 EXTRACTED FILE IDS:', {
      fileIds: fileIds,
      isArray: Array.isArray(fileIds),
      length: fileIds ? fileIds.length : 'undefined'
    });

    if (!fileIds) {
      console.log('❌ NO FILE IDS FIELD');
      return new Response(JSON.stringify({
        success: false,
        error: 'fileIds field not found in request'
      }), {
        status: 400,
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    if (!Array.isArray(fileIds)) {
      console.log('❌ FILE IDS NOT ARRAY');
      return new Response(JSON.stringify({
        success: false,
        error: 'fileIds must be an array'
      }), {
        status: 400,
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    if (fileIds.length === 0) {
      console.log('❌ EMPTY FILE IDS ARRAY');
      return new Response(JSON.stringify({
        success: false,
        error: 'fileIds array is empty'
      }), {
        status: 400,
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    // Debug each file ID
    console.log('🔍 VALIDATING FILE IDS:');
    fileIds.forEach((id, index) => {
      console.log(`  [${index}] ID: "${id}" | Type: ${typeof id} | Valid: ${!!(id && typeof id === 'string' && id.trim().startsWith('MSM'))}`);
    });

    // Filter valid MSM IDs with detailed logging
    const validFileIds = fileIds.filter(id => {
      const isString = typeof id === 'string';
      const isNotEmpty = id && id.trim();
      const isMSM = isNotEmpty && id.trim().startsWith('MSM');
      const isValid = isString && isNotEmpty && isMSM;
      
      if (!isValid) {
        console.log(`❌ INVALID ID REJECTED: "${id}" (string: ${isString}, notEmpty: ${!!isNotEmpty}, MSM: ${isMSM})`);
      } else {
        console.log(`✅ VALID ID ACCEPTED: "${id}"`);
      }
      
      return isValid;
    });

    console.log('📊 VALIDATION RESULT:', {
      originalCount: fileIds.length,
      validCount: validFileIds.length,
      validIds: validFileIds
    });

    if (validFileIds.length === 0) {
      console.log('❌ NO VALID MSM FILE IDS FOUND');
      return new Response(JSON.stringify({
        success: false,
        error: 'No valid MSM file IDs found',
        debug: {
          originalFileIds: fileIds,
          validationResults: fileIds.map(id => ({
            id: id,
            type: typeof id,
            isString: typeof id === 'string',
            hasContent: !!(id && id.trim()),
            startsMSM: !!(id && id.trim && id.trim().startsWith('MSM'))
          }))
        }
      }), {
        status: 400,
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    console.log(`🗑️ PROCEEDING WITH DELETION OF ${validFileIds.length} FILES:`, validFileIds);

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

    for (const fileId of validFileIds) {
      try {
        console.log(`🗑️ PROCESSING DELETION: ${fileId}`);

        // Find file metadata
        let fileMetadata = null;
        let sourceKV = null;

        for (const kvNamespace of kvNamespaces) {
          try {
            const metadata = await kvNamespace.kv.get(fileId);
            if (metadata) {
              fileMetadata = JSON.parse(metadata);
              sourceKV = kvNamespace;
              console.log(`📁 FOUND ${fileId} in ${kvNamespace.name}`);
              break;
            }
          } catch (e) {
            console.log(`❌ Error checking ${fileId} in ${kvNamespace.name}:`, e.message);
            continue;
          }
        }

        if (!fileMetadata) {
          console.log(`⚠️ FILE ${fileId} NOT FOUND in any KV namespace`);
          errors.push(`File ${fileId} not found in KV`);
          continue;
        }

        let chunksDeletedFromKV = 0;

        // Delete all chunks
        if (fileMetadata.chunks && Array.isArray(fileMetadata.chunks)) {
          console.log(`🗑️ DELETING ${fileMetadata.chunks.length} chunks for ${fileId}...`);

          for (const chunkInfo of fileMetadata.chunks) {
            try {
              const chunkKV = kvNamespaces.find(ns => ns.name === chunkInfo.kvNamespace);
              if (chunkKV && chunkInfo.keyName) {
                await chunkKV.kv.delete(chunkInfo.keyName);
                chunksDeletedFromKV++;
                console.log(`✅ DELETED KV chunk: ${chunkInfo.keyName}`);
              }
            } catch (chunkError) {
              console.error(`❌ Failed to delete chunk ${chunkInfo.keyName}:`, chunkError.message);
              errors.push(`Failed to delete chunk ${chunkInfo.keyName}: ${chunkError.message}`);
            }
          }
        }

        // Delete main file metadata from KV
        await sourceKV.kv.delete(fileId);
        deletedCount++;

        deletedDetails.push({
          fileId: fileId,
          filename: fileMetadata.filename || 'Unknown',
          kvChunksDeleted: chunksDeletedFromKV,
          kvNamespace: sourceKV.name
        });

        console.log(`✅ DELETION SUCCESSFUL: ${fileId} (${chunksDeletedFromKV} chunks deleted)`);

      } catch (fileError) {
        console.error(`❌ Failed to delete file ${fileId}:`, fileError.message);
        errors.push(`Failed to delete ${fileId}: ${fileError.message}`);
      }
    }

    console.log(`✅ DELETION SUMMARY: ${deletedCount}/${validFileIds.length} files deleted`);

    return new Response(JSON.stringify({
      success: true,
      deletedCount: deletedCount,
      totalRequested: validFileIds.length,
      deletedDetails: deletedDetails,
      errors: errors.length > 0 ? errors : undefined,
      debug: {
        originalFileIds: fileIds,
        validFileIds: validFileIds,
        requestBodyLength: requestText.length
      },
      timestamp: Date.now()
    }), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (error) {
    console.error('💥 COMPLETE DELETE ERROR:', error);
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
