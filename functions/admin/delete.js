export async function onRequest(context) {
  const { request, env } = context;

  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Content-Type': 'application/json'
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { headers });
  }

  if (request.method !== 'POST') {
    return new Response(JSON.stringify({
      success: false,
      error: 'Only POST method allowed'
    }), { status: 405, headers });
  }

  try {
    const { fileId } = await request.json();
    
    if (!fileId) {
      throw new Error('File ID required');
    }

    // ✅ All KV namespaces
    const kvNamespaces = {
      FILES_KV: env.FILES_KV,
      FILES_KV2: env.FILES_KV2,
      FILES_KV3: env.FILES_KV3,
      FILES_KV4: env.FILES_KV4,
      FILES_KV5: env.FILES_KV5,
      FILES_KV6: env.FILES_KV6,
      FILES_KV7: env.FILES_KV7
    };

    if (!kvNamespaces.FILES_KV) {
      throw new Error('Primary FILES_KV binding not found');
    }

    // ✅ Get master metadata to find all chunks
    const masterMetadataString = await kvNamespaces.FILES_KV.get(fileId);
    
    if (masterMetadataString) {
      try {
        const masterMetadata = JSON.parse(masterMetadataString);
        
        // ✅ Delete chunked file and all its chunks
        if (masterMetadata.type === 'multi_kv_chunked' && masterMetadata.chunks) {
          console.log(`Deleting chunked file with ${masterMetadata.chunks.length} chunks`);
          
          // Delete all chunks from their respective KV namespaces
          const deletePromises = masterMetadata.chunks.map(async (chunkInfo) => {
            const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
            const chunkKey = chunkInfo.chunkKey || `${fileId}_chunk_${chunkInfo.index}`;
            
            if (kvNamespace) {
              await kvNamespace.delete(chunkKey);
              console.log(`Deleted chunk: ${chunkKey} from ${chunkInfo.kvNamespace}`);
            }
          });
          
          await Promise.all(deletePromises);
          console.log('All chunks deleted');
        }
        
      } catch (parseError) {
        console.log('Could not parse metadata, treating as single file');
      }
    }

    // ✅ Delete master metadata from primary KV
    await kvNamespaces.FILES_KV.delete(fileId);
    console.log(`Deleted master metadata: ${fileId}`);

    // ✅ Also try to delete from all KV namespaces (for legacy single files)
    const legacyDeletePromises = Object.values(kvNamespaces)
      .filter(kv => kv)
      .map(async (kvNamespace) => {
        try {
          await kvNamespace.delete(fileId);
        } catch (error) {
          // Ignore errors for non-existent keys
        }
      });
    
    await Promise.all(legacyDeletePromises);

    console.log(`File ${fileId} completely deleted from all KV namespaces`);

    return new Response(JSON.stringify({
      success: true,
      message: 'File and all chunks deleted successfully'
    }), { headers });

  } catch (error) {
    console.error('Multi-KV delete error:', error);
    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), { status: 500, headers });
  }
}
