export async function onRequest(context) {
  const { env } = context;

  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Content-Type': 'application/json'
  };

  if (context.request.method === 'OPTIONS') {
    return new Response(null, { headers });
  }

  try {
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

    // List files from primary KV (contains master metadata)
    const listResult = await kvNamespaces.FILES_KV.list();
    
    const files = listResult.keys
      .filter(key => !key.name.includes('_chunk_')) // Exclude chunk entries
      .map(key => {
        let metadata = key.metadata;
        
        // Parse JSON metadata if stored as string
        if (typeof key.value === 'string') {
          try {
            const parsedValue = JSON.parse(key.value);
            if (parsedValue.filename) {
              metadata = parsedValue;
            }
          } catch (e) {
            // Ignore parse errors
          }
        }
        
        return {
          name: key.name,
          metadata: metadata
        };
      });

    // Sort by upload date (newest first)
    files.sort((a, b) => {
      const dateA = a.metadata?.uploadedAt || 0;
      const dateB = b.metadata?.uploadedAt || 0;
      return dateB - dateA;
    });

    console.log('Multi-KV files loaded:', files.length);

    return new Response(JSON.stringify({
      success: true,
      files: files,
      total: files.length,
      availableKVNamespaces: Object.keys(kvNamespaces).filter(k => kvNamespaces[k]).length
    }), { headers });

  } catch (error) {
    console.error('Multi-KV files list error:', error);
    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), { status: 500, headers });
  }
}
