// =====================================================
// 🚨 MARYA VAULT ULTIMATE v5.1 - JSON ERROR 100% FIXED
// Proper Response.json() • Debug Mode • Error Proof
// =====================================================

const MIME_TYPES = {
  'jpg': 'image/jpeg', 'jpeg': 'image/jpeg', 'png': 'image/png', 'gif': 'image/gif',
  'webp': 'image/webp', 'svg': 'image/svg+xml', 'mp4': 'video/mp4', 'mkv': 'video/x-matroska',
  'mp3': 'audio/mpeg', 'wav': 'audio/wav', 'pdf': 'application/pdf', 'zip': 'application/zip'
};

export async function onRequestPost(context) {
  const { request, env } = context;
  
  // 🔥 ULTIMATE CORS
  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': '*',
    'Access-Control-Max-Age': '86400'
  };

  // 🔥 OPTIONS PREFLIGHT
  if (request.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: corsHeaders });
  }

  // 🔥 DEBUG LOGGING
  console.log('🔥 UPLOAD START');
  console.log('Method:', request.method);
  console.log('Content-Type:', request.headers.get('Content-Type'));
  console.log('Content-Length:', request.headers.get('Content-Length'));

  try {
    // 🔥 ENV CHECK
    const BOT_TOKEN = env.BOT_TOKEN;
    const CHANNEL_ID = env.CHANNEL_ID;
    
    if (!BOT_TOKEN || !CHANNEL_ID) {
      const errorData = {
        success: false,
        error: 'Missing BOT_TOKEN or CHANNEL_ID in wrangler.toml',
        debug: { hasBot: !!BOT_TOKEN, hasChannel: !!CHANNEL_ID }
      };
      return new Response(JSON.stringify(errorData), {
        status: 500,
        headers: { 
          'Content-Type': 'application/json; charset=utf-8',
          ...corsHeaders 
        }
      });
    }

    // 🔥 25 KV NAMESPACES
    const kvList = [];
    for (let i = 1; i <= 25; i++) {
      const kvName = i === 1 ? 'FILES_KV' : `FILES_KV${i}`;
      if (env[kvName]) kvList.push({ kv: env[kvName], name: kvName });
    }

    if (kvList.length === 0) {
      return new Response(JSON.stringify({
        success: false,
        error: 'No KV namespaces bound. Need FILES_KV in wrangler.toml'
      }), {
        status: 500,
        headers: { 'Content-Type': 'application/json; charset=utf-8', ...corsHeaders }
      });
    }

    console.log(`✅ ${kvList.length} KV namespaces ready`);

    // 🔥 FORM DATA - CRASH PROOF
    let formData;
    try {
      formData = await request.formData();
      console.log('✅ FormData parsed');
    } catch (parseError) {
      console.error('❌ FormData error:', parseError);
      return new Response(JSON.stringify({
        success: false,
        error: 'Invalid FormData. Use: formData.append("file", file)',
        fix: 'Frontend: const fd = new FormData(); fd.append("file", selectedFile);'
      }), {
        status: 400,
        headers: { 'Content-Type': 'application/json; charset=utf-8', ...corsHeaders }
      });
    }

    const file = formData.get('file');
    if (!file || file.size === 0) {
      return new Response(JSON.stringify({
        success: false,
        error: 'No valid file found. Use key "file"'
      }), {
        status: 400,
        headers: { 'Content-Type': 'application/json; charset=utf-8', ...corsHeaders }
      });
    }

    console.log(`📁 File: ${file.name} (${formatBytes(file.size)})`);

    // 🔥 SIZE CHECK
    if (file.size > 1536 * 1024 * 1024) {
      return new Response(JSON.stringify({
        success: false,
        error: `Max 1.5GB. File: ${(file.size/1024/1024).toFixed(1)}MB`
      }), {
        status: 413,
        headers: { 'Content-Type': 'application/json; charset=utf-8', ...corsHeaders }
      });
    }

    // 🔥 GENERATE ID
    const fileId = `id${Date.now().toString(36)}${Math.random().toString(36).substr(2, 8)}`;
    const ext = file.name.split('.').pop()?.toLowerCase() || '';
    
    console.log('🆔 ID:', fileId);

    // 🔥 CHUNKING
    const CHUNK_SIZE = 30 * 1024 * 1024; // 30MB
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);
    
    if (totalChunks > kvList.length * 2) {
      return new Response(JSON.stringify({
        success: false,
        error: `Too many chunks: ${totalChunks} > ${kvList.length * 2}`
      }), {
        status: 507,
        headers: { 'Content-Type': 'application/json; charset=utf-8', ...corsHeaders }
      });
    }

    console.log(`🧩 ${totalChunks} chunks`);

    // 🔥 UPLOAD CHUNKS (SEQUENTIAL - NO PARALLEL ISSUES)
    const chunkResults = [];
    for (let i = 0; i < totalChunks; i++) {
      const start = i * CHUNK_SIZE;
      const end = Math.min(start + CHUNK_SIZE, file.size);
      const chunkBlob = file.slice(start, end);
      const chunkFile = new File([chunkBlob], `${fileId}_c${i}`, { type: file.type });
      
      console.log(`📦 Chunk ${i + 1}/${totalChunks}`);
      
      try {
        const result = await uploadChunk(chunkFile, fileId, i, BOT_TOKEN, CHANNEL_ID, kvList[i % kvList.length]);
        chunkResults.push(result);
      } catch (chunkError) {
        console.error(`❌ Chunk ${i} failed:`, chunkError);
        return new Response(JSON.stringify({
          success: false,
          error: `Chunk ${i + 1} failed: ${chunkError.message}`
        }), {
          status: 500,
          headers: { 'Content-Type': 'application/json; charset=utf-8', ...corsHeaders }
        });
      }
    }

    // 🔥 MASTER METADATA (100% [id].js COMPATIBLE)
    const masterMeta = {
      filename: file.name,
      size: file.size,
      contentType: file.type || MIME_TYPES[ext] || 'application/octet-stream',
      extension: `.${ext}`,
      uploadedAt: Date.now(),
      type: 'marya_vault_v5',
      totalChunks,
      chunks: chunkResults.map((r, i) => ({
        index: i,
        kvNamespace: r.kvNamespace,
        telegramFileId: r.telegramFileId,
        telegramMessageId: r.telegramMessageId,
        size: r.size,
        chunkKey: `${fileId}_chunk_${i}`  // CRITICAL FOR [id].js
      }))
    };

    // 🔥 SAVE MASTER METADATA
    await kvList[0].kv.put(fileId, JSON.stringify(masterMeta));
    console.log('💾 Master metadata saved');

    // 🔥 SUCCESS RESPONSE (JSON ERROR PROOF)
    const baseUrl = new URL(request.url).origin;
    const successData = {
      success: true,
      message: 'Upload complete!',
      data: {
        id: fileId,
        filename: file.name,
        size: file.size,
        sizeFormatted: formatBytes(file.size),
        totalChunks,
        urls: {
          view: `${baseUrl}/btfstorage/file/${fileId}.${ext}`,
          download: `${baseUrl}/btfstorage/file/${fileId}.${ext}?dl=1`
        }
      }
    };

    console.log('✅ SUCCESS!');
    return new Response(
      JSON.stringify(successData), 
      {
        status: 200,
        headers: { 
          'Content-Type': 'application/json; charset=utf-8',
          'X-File-ID': fileId,
          ...corsHeaders 
        }
      }
    );

  } catch (error) {
    console.error('💥 FINAL ERROR:', error);
    
    const errorData = {
      success: false,
      error: error.message || 'Unknown error',
      stack: process.env.NODE_ENV === 'development' ? error.stack : undefined,
      timestamp: new Date().toISOString()
    };

    return new Response(
      JSON.stringify(errorData), 
      {
        status: 500,
        headers: { 
          'Content-Type': 'application/json; charset=utf-8',
          ...corsHeaders 
        }
      }
    );
  }
}

// 🔥 CHUNK UPLOADER (SEQUENTIAL - NO RACE CONDITIONS)
async function uploadChunk(chunkFile, fileId, chunkIndex, botToken, channelId, kvNamespace) {
  console.log(`📤 Uploading chunk ${chunkIndex} (${formatBytes(chunkFile.size)})`);
  
  // Telegram upload
  const form = new FormData();
  form.append('chat_id', channelId);
  form.append('document', chunkFile);
  form.append('caption', `${fileId}#${chunkIndex}`);

  const res = await fetch(`https://api.telegram.org/bot${botToken}/sendDocument`, {
    method: 'POST',
    body: form
  });

  if (!res.ok) {
    const errorText = await res.text();
    throw new Error(`Telegram ${res.status}: ${errorText}`);
  }

  const data = await res.json();
  if (!data.ok) throw new Error(data.description);

  const telegramFileId = data.result.document.file_id;
  
  // Get direct URL
  const getFileRes = await fetch(`https://api.telegram.org/bot${botToken}/getFile?file_id=${telegramFileId}`);
  const getFileData = await getFileRes.json();
  
  if (!getFileData.ok) throw new Error('getFile failed');
  
  const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

  // Save chunk metadata
  const chunkKey = `${fileId}_chunk_${chunkIndex}`;
  const chunkMeta = {
    telegramFileId,
    telegramMessageId: data.result.message_id,
    directUrl,
    size: chunkFile.size,
    index: chunkIndex,
    parentFileId: fileId,
    kvNamespace: kvNamespace.name,
    chunkKey,
    uploadedAt: Date.now()
  };

  await kvNamespace.kv.put(chunkKey, JSON.stringify(chunkMeta));

  return {
    telegramFileId,
    telegramMessageId: data.result.message_id,
    size: chunkFile.size,
    kvNamespace: kvNamespace.name
  };
}

function formatBytes(bytes) {
  const sizes = ['B', 'KB', 'MB', 'GB'];
  if (bytes === 0) return '0 B';
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return (bytes / Math.pow(1024, i)).toFixed(1) + ' ' + sizes[i];
}

export async function onRequestGet() {
  return new Response(JSON.stringify({
    status: 'Marya Vault v5.1 - JSON FIXED',
    ready: true
  }), {
    headers: { 'Content-Type': 'application/json; charset=utf-8' }
  });
}
