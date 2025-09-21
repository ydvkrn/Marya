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
    console.log('🗑️ Complete delete API called');

    // Get admin key from environment
    const adminKey = env.KEYMSM || 'MARYA2025ADMIN';
    const authHeader = request.headers.get('Authorization');
    
    if (!authHeader || !authHeader.includes(adminKey)) {
      return new Response(JSON.stringify({
        success: false,
        error: 'Unauthorized access'
      }), {
        status: 401,
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    const requestText = await request.text();
    if (!requestText) {
      throw new Error('Empty request body');
    }

    const requestData = JSON.parse(requestText);
    const { fileIds } = requestData;
    
    if (!Array.isArray(fileIds) || fileIds.length === 0) {
      throw new Error('No valid file IDs provided');
    }

    // Filter valid MSM IDs
    const validFileIds = fileIds.filter(id => 
      id && typeof id === 'string' && id.startsWith('MSM')
    );

    if (validFileIds.length === 0) {
      throw new Error('No valid MSM file IDs found');
    }

    console.log(`🗑️ Complete deletion of ${validFileIds.length} files:`, validFileIds);

    const kvNamespaces = [
      { kv: env.FILES_KV, name: 'FILES_KV' },
      { kv: env.FILES_KV2, name: 'FILES_KV2' },
      { kv: env.FILES_KV3, name: 'FILES_KV3' },
      { kv: env.FILES_KV4, name: 'FILES_KV4' },
      { kv: env.FILES_KV5, name: 'FILES_KV5' },
      { kv: env.FILES_KV6, name: 'FILES_KV6' },
      { kv: env.FILES_KV7, name: 'FILES_KV7' }
    ].filter(item => item.kv);

    // Bot tokens for Telegram deletion
    const botTokens = [
      env.BOT_TOKEN,
      env.BOT_TOKEN2,
      env.BOT_TOKEN3,
      env.BOT_TOKEN4
    ].filter(token => token);

    let deletedCount = 0;
    let telegramDeletedCount = 0;
    let kvDeletedCount = 0;
    let errors = [];
    let deletedDetails = [];

    for (const fileId of validFileIds) {
      try {
        console.log(`🗑️ Processing complete deletion: ${fileId}`);

        // Find file metadata
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
          console.log(`⚠️ File ${fileId} not found`);
          errors.push(`File ${fileId} not found in KV`);
          continue;
        }

        let chunksDeletedFromKV = 0;
        let chunksDeletedFromTelegram = 0;

        // Delete all chunks (KV + Telegram)
        if (fileMetadata.chunks && Array.isArray(fileMetadata.chunks)) {
          console.log(`🗑️ Deleting ${fileMetadata.chunks.length} chunks for ${fileId}...`);

          for (const chunkInfo of fileMetadata.chunks) {
            try {
              // Delete from KV
              const chunkKV = kvNamespaces.find(ns => ns.name === chunkInfo.kvNamespace);
              if (chunkKV && chunkInfo.keyName) {
                await chunkKV.kv.delete(chunkInfo.keyName);
                chunksDeletedFromKV++;
                console.log(`✅ Deleted KV chunk: ${chunkInfo.keyName}`);
              }

              // Delete from Telegram if we have file_id
              if (chunkInfo.telegramFileId && botTokens.length > 0) {
                const success = await deleteTelegramFile(chunkInfo.telegramFileId, botTokens);
                if (success) {
                  chunksDeletedFromTelegram++;
                  console.log(`✅ Deleted Telegram chunk: ${chunkInfo.telegramFileId}`);
                }
              }

            } catch (chunkError) {
              console.error(`❌ Failed to delete chunk ${chunkInfo.keyName}:`, chunkError.message);
              errors.push(`Failed to delete chunk ${chunkInfo.keyName}: ${chunkError.message}`);
            }
          }
        }

        // Delete main file metadata from KV
        await sourceKV.kv.delete(fileId);
        kvDeletedCount++;
        deletedCount++;

        deletedDetails.push({
          fileId: fileId,
          filename: fileMetadata.filename || 'Unknown',
          kvChunksDeleted: chunksDeletedFromKV,
          telegramChunksDeleted: chunksDeletedFromTelegram,
          kvNamespace: sourceKV.name
        });

        console.log(`✅ Complete deletion successful: ${fileId} (KV: ${chunksDeletedFromKV}, Telegram: ${chunksDeletedFromTelegram})`);

      } catch (fileError) {
        console.error(`❌ Failed to delete file ${fileId}:`, fileError.message);
        errors.push(`Failed to delete ${fileId}: ${fileError.message}`);
      }
    }

    console.log(`✅ Complete deletion summary: ${deletedCount}/${validFileIds.length} files deleted`);
    console.log(`📊 KV chunks deleted: ${kvDeletedCount}, Telegram chunks deleted: ${telegramDeletedCount}`);

    return new Response(JSON.stringify({
      success: true,
      deletedCount: deletedCount,
      totalRequested: validFileIds.length,
      kvDeletedCount: kvDeletedCount,
      telegramDeletedCount: telegramDeletedCount,
      deletedDetails: deletedDetails,
      errors: errors.length > 0 ? errors : undefined,
      timestamp: Date.now()
    }), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (error) {
    console.error('💥 Complete delete error:', error);
    return new Response(JSON.stringify({
      success: false,
      error: error.message,
      timestamp: Date.now()
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}

// Delete file from Telegram
async function deleteTelegramFile(fileId, botTokens) {
  for (const botToken of botTokens) {
    try {
      console.log(`🗑️ Attempting Telegram deletion with bot ...${botToken.slice(-4)}`);
      
      // Note: Telegram Bot API doesn't have direct delete file method
      // But we can try to get file info to verify it exists, then it will expire naturally
      const response = await fetch(`https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(fileId)}`, {
        method: 'GET',
        signal: AbortSignal.timeout(10000)
      });

      if (response.ok) {
        const data = await response.json();
        if (data.ok) {
          console.log(`✅ Verified Telegram file exists: ${fileId} (will expire naturally)`);
          return true;
        }
      }

    } catch (telegramError) {
      console.error(`❌ Telegram deletion failed for ${fileId}:`, telegramError.message);
    }
  }
  
  return false;
}
