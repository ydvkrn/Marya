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
    console.log('⏰ Admin update-expiry API called');

    // Auth check
    const authHeader = request.headers.get('Authorization');
    if (!authHeader || !authHeader.includes('MARYA2025ADMIN')) {
      return new Response(JSON.stringify({
        success: false,
        error: 'Unauthorized access'
      }), {
        status: 401,
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    const { fileId, expiryDays } = await request.json();
    
    if (!fileId) {
      throw new Error('File ID not provided');
    }

    console.log(`⏰ Setting expiry for ${fileId}: ${expiryDays} days`);

    const kvNamespaces = [
      { kv: env.FILES_KV, name: 'FILES_KV' },
      { kv: env.FILES_KV2, name: 'FILES_KV2' },
      { kv: env.FILES_KV3, name: 'FILES_KV3' },
      { kv: env.FILES_KV4, name: 'FILES_KV4' },
      { kv: env.FILES_KV5, name: 'FILES_KV5' },
      { kv: env.FILES_KV6, name: 'FILES_KV6' },
      { kv: env.FILES_KV7, name: 'FILES_KV7' }
    ].filter(item => item.kv);

    // Find the file
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
      throw new Error(`File ${fileId} not found`);
    }

    // Update expiry settings
    if (expiryDays === null || expiryDays === 'permanent') {
      fileMetadata.neverExpires = true;
      delete fileMetadata.expiresAt;
      delete fileMetadata.expiryDays;
    } else {
      const days = parseInt(expiryDays);
      if (isNaN(days) || days < 1) {
        throw new Error('Invalid expiry days');
      }
      
      fileMetadata.neverExpires = false;
      fileMetadata.expiryDays = days;
      fileMetadata.expiresAt = Date.now() + (days * 24 * 60 * 60 * 1000);
    }

    fileMetadata.lastModified = Date.now();

    // Save updated metadata
    await sourceKV.kv.put(fileId, JSON.stringify(fileMetadata));

    console.log(`✅ Updated expiry for ${fileId}: ${expiryDays === null ? 'Permanent' : expiryDays + ' days'}`);

    return new Response(JSON.stringify({
      success: true,
      fileId: fileId,
      filename: fileMetadata.filename,
      expiryDays: fileMetadata.expiryDays,
      expiresAt: fileMetadata.expiresAt,
      neverExpires: fileMetadata.neverExpires,
      timestamp: Date.now()
    }), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (error) {
    console.error('💥 Admin update-expiry error:', error);
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
