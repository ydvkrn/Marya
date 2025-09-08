export async function onRequest(context) {
  const { env } = context;

  console.log('=== MEDIA PRELOAD & CACHE WARMING ===');

  try {
    const kvNamespaces = {
      FILES_KV: env.FILES_KV, FILES_KV2: env.FILES_KV2, FILES_KV3: env.FILES_KV3,
      FILES_KV4: env.FILES_KV4, FILES_KV5: env.FILES_KV5, FILES_KV6: env.FILES_KV6, FILES_KV7: env.FILES_KV7
    };

    let preloaded = 0;

    // ✅ Preload recent media files for instant access
    const listResult = await kvNamespaces.FILES_KV.list({ limit: 50 });
    
    for (const key of listResult.keys) {
      try {
        const fileData = await kvNamespaces.FILES_KV.get(key.name);
        if (!fileData) continue;
        
        const metadata = JSON.parse(fileData);
        if (!metadata.type === 'multi_kv_chunked') continue;
        
        // ✅ Preload first chunk for instant playback
        if (metadata.chunks && metadata.chunks.length > 0) {
          const firstChunk = metadata.chunks[0];
          const kvNamespace = kvNamespaces[firstChunk.kvNamespace];
          const chunkKey = firstChunk.chunkKey;
          
          const chunkData = await kvNamespace.get(chunkKey);
          if (chunkData) {
            const chunkMeta = JSON.parse(chunkData);
            
            // ✅ Warm cache with first chunk
            await fetch(chunkMeta.directUrl, {
              cf: { cacheEverything: true, cacheTtl: 86400 }
            });
            
            preloaded++;
            console.log(`🔥 Preloaded first chunk for ${key.name}`);
          }
        }
        
      } catch (error) {
        console.log(`Skip preload for ${key.name}:`, error.message);
      }
    }

    console.log(`✅ Media preload completed: ${preloaded} files preloaded`);

    return new Response(JSON.stringify({
      success: true,
      preloaded: preloaded,
      timestamp: Date.now()
    }), {
      headers: { 'Content-Type': 'application/json' }
    });

  } catch (error) {
    console.error('❌ Preload error:', error);
    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), { status: 500 });
  }
}
