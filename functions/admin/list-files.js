export async function onRequest(context) {
  const { request, env } = context;

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization'
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    console.log('⚡ Fast admin list-files API called');

    // Get admin key from environment
    const adminKey = env.KEYMSM || 'MARYA2025ADMIN';
    const authHeader = request.headers.get('Authorization');
    
    if (!authHeader || !authHeader.includes(adminKey)) {
      console.log('❌ Unauthorized access attempt');
      return new Response(JSON.stringify({
        success: false,
        error: 'Unauthorized access'
      }), {
        status: 401,
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    // All KV namespaces with parallel processing
    const kvNamespaces = [
      { kv: env.FILES_KV, name: 'FILES_KV' },
      { kv: env.FILES_KV2, name: 'FILES_KV2' },
      { kv: env.FILES_KV3, name: 'FILES_KV3' },
      { kv: env.FILES_KV4, name: 'FILES_KV4' },
      { kv: env.FILES_KV5, name: 'FILES_KV5' },
      { kv: env.FILES_KV6, name: 'FILES_KV6' },
      { kv: env.FILES_KV7, name: 'FILES_KV7' }
    ].filter(item => item.kv);

    console.log(`⚡ Fast scanning ${kvNamespaces.length} KV namespaces in parallel...`);

    // Parallel processing for speed
    const scanPromises = kvNamespaces.map(async (kvNamespace) => {
      const files = [];
      
      try {
        console.log(`⚡ Fast scanning ${kvNamespace.name}...`);
        
        // Fast list with limit for performance
        const listResponse = await kvNamespace.kv.list({ limit: 1000 });
        
        // Process files in batches for speed
        const fileKeys = listResponse.keys.filter(key => 
          key.name.startsWith('MSM') && 
          !key.name.includes('_chunk_') && 
          !key.name.includes('progress_')
        );

        console.log(`📊 Found ${fileKeys.length} files in ${kvNamespace.name}`);

        // Get metadata in parallel batches
        const batchSize = 10;
        for (let i = 0; i < fileKeys.length; i += batchSize) {
          const batch = fileKeys.slice(i, i + batchSize);
          
          const batchPromises = batch.map(async (key) => {
            try {
              const metadata = await kvNamespace.kv.get(key.name);
              if (metadata) {
                const fileData = JSON.parse(metadata);
                
                return {
                  id: key.name,
                  filename: fileData.filename || 'Unknown File',
                  size: fileData.size || 0,
                  contentType: fileData.contentType || 'application/octet-stream',
                  extension: fileData.extension || '',
                  uploadedAt: fileData.uploadedAt || Date.now(),
                  kvNamespace: kvNamespace.name,
                  neverExpires: fileData.neverExpires || false,
                  type: fileData.type || 'unknown',
                  chunks: fileData.chunks || [],
                  totalChunks: fileData.totalChunks || 0,
                  expiryDays: fileData.expiryDays,
                  expiresAt: fileData.expiresAt
                };
              }
            } catch (parseError) {
              console.error(`❌ Parse error for ${key.name}:`, parseError.message);
              return {
                id: key.name,
                filename: `Corrupted (${key.name})`,
                size: 0,
                contentType: 'application/octet-stream',
                extension: '',
                uploadedAt: Date.now(),
                kvNamespace: kvNamespace.name,
                neverExpires: false,
                type: 'corrupted',
                chunks: [],
                totalChunks: 0
              };
            }
          });
          
          const batchResults = await Promise.all(batchPromises);
          files.push(...batchResults.filter(file => file));
        }
        
      } catch (kvError) {
        console.error(`❌ Failed to scan ${kvNamespace.name}:`, kvError.message);
      }
      
      return files;
    });

    // Wait for all namespaces to complete
    const namespaceResults = await Promise.all(scanPromises);
    const allFiles = namespaceResults.flat();

    // Sort by upload date (newest first)
    allFiles.sort((a, b) => (b.uploadedAt || 0) - (a.uploadedAt || 0));

    const totalSize = allFiles.reduce((sum, file) => sum + (file.size || 0), 0);
    const permanentFiles = allFiles.filter(file => file.neverExpires).length;

    const stats = {
      totalFiles: allFiles.length,
      totalSize: totalSize,
      activeFiles: permanentFiles,
      kvNamespaces: kvNamespaces.length
    };

    console.log(`⚡ Fast scan complete: ${allFiles.length} files in ${Date.now() - Date.now()}ms`);

    return new Response(JSON.stringify({
      success: true,
      files: allFiles,
      stats: stats,
      timestamp: Date.now(),
      performance: 'optimized'
    }), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (error) {
    console.error('💥 Fast admin list-files error:', error);
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
