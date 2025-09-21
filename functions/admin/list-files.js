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

  try {
    console.log('🔍 Admin list-files API called');

    // Simple auth check
    const authHeader = request.headers.get('Authorization');
    if (!authHeader || !authHeader.includes('MARYA2025ADMIN')) {
      console.log('❌ Unauthorized access attempt');
      return new Response(JSON.stringify({
        success: false,
        error: 'Unauthorized access'
      }), {
        status: 401,
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    console.log('✅ Auth successful, scanning KV namespaces...');

    // All KV namespaces
    const kvNamespaces = [
      { kv: env.FILES_KV, name: 'FILES_KV' },
      { kv: env.FILES_KV2, name: 'FILES_KV2' },
      { kv: env.FILES_KV3, name: 'FILES_KV3' },
      { kv: env.FILES_KV4, name: 'FILES_KV4' },
      { kv: env.FILES_KV5, name: 'FILES_KV5' },
      { kv: env.FILES_KV6, name: 'FILES_KV6' },
      { kv: env.FILES_KV7, name: 'FILES_KV7' }
    ].filter(item => item.kv);

    let allFiles = [];
    let totalSize = 0;
    let activeFiles = 0;

    console.log(`📂 Scanning ${kvNamespaces.length} KV namespaces...`);

    // Scan each KV namespace
    for (const kvNamespace of kvNamespaces) {
      try {
        console.log(`🔍 Scanning ${kvNamespace.name}...`);

        // List all keys in this namespace
        const listResponse = await kvNamespace.kv.list();
        console.log(`📊 Found ${listResponse.keys.length} keys in ${kvNamespace.name}`);
        
        for (const key of listResponse.keys) {
          // Only process MSM format files (skip chunks and progress)
          if (key.name.startsWith('MSM') && !key.name.includes('_chunk_') && !key.name.includes('progress_')) {
            try {
              const metadata = await kvNamespace.kv.get(key.name);
              if (metadata) {
                const fileData = JSON.parse(metadata);
                
                // Add required fields
                fileData.id = key.name;
                fileData.kvNamespace = kvNamespace.name;
                
                allFiles.push(fileData);
                totalSize += fileData.size || 0;
                
                if (fileData.neverExpires) {
                  activeFiles++;
                }
                
                console.log(`📁 File found: ${fileData.filename} (${Math.round((fileData.size || 0)/1024/1024)}MB)`);
              }
            } catch (parseError) {
              console.error(`❌ Failed to parse ${key.name}:`, parseError.message);
            }
          }
        }
      } catch (kvError) {
        console.error(`❌ Failed to scan ${kvNamespace.name}:`, kvError.message);
      }
    }

    // Sort by upload date (newest first)
    allFiles.sort((a, b) => (b.uploadedAt || 0) - (a.uploadedAt || 0));

    const stats = {
      totalFiles: allFiles.length,
      totalSize: totalSize,
      activeFiles: activeFiles,
      kvNamespaces: kvNamespaces.length
    };

    console.log(`✅ Admin scan complete: ${allFiles.length} files, ${Math.round(totalSize/1024/1024)}MB total`);

    return new Response(JSON.stringify({
      success: true,
      files: allFiles,
      stats: stats,
      timestamp: Date.now()
    }), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (error) {
    console.error('💥 Admin list-files error:', error);
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
