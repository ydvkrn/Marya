// /functions/upload-chunk.js - Single Chunk Handler
export async function onRequest(context) {
  const { request, env } = context;

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, X-File-Id, X-Chunk-Index, X-Total-Chunks, X-Filename'
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: corsHeaders });
  }

  if (request.method !== 'POST') {
    return new Response(JSON.stringify({ success: false, error: 'Method not allowed' }), {
      status: 405,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }

  try {
    const BOT_TOKEN = env.BOT_TOKEN;
    const CHANNEL_ID = env.CHANNEL_ID;

    // Get KV namespaces
    const kvNamespaces = [];
    for (let i = 1; i <= 25; i++) {
      const kvKey = i === 1 ? 'FILES_KV' : `FILES_KV${i}`;
      if (env[kvKey]) kvNamespaces.push({ kv: env[kvKey], name: kvKey });
    }

    if (!BOT_TOKEN || !CHANNEL_ID || kvNamespaces.length === 0) {
      throw new Error('Missing credentials');
    }

    // Get headers
    const fileId = request.headers.get('X-File-Id');
    const chunkIndex = parseInt(request.headers.get('X-Chunk-Index'));
    const totalChunks = parseInt(request.headers.get('X-Total-Chunks'));
    const filename = request.headers.get('X-Filename');

    if (!fileId || isNaN(chunkIndex) || isNaN(totalChunks) || !filename) {
      throw new Error('Missing headers');
    }

    // Get chunk data
    const chunkBlob = await request.blob();
    
    if (chunkBlob.size === 0) {
      throw new Error('Empty chunk');
    }

    console.log(`Chunk ${chunkIndex + 1}/${totalChunks} received (${chunkBlob.size} bytes)`);

    const chunkFile = new File([chunkBlob], `${filename}.part${chunkIndex}`, { type: chunkBlob.type });
    const targetKV = kvNamespaces[chunkIndex % kvNamespaces.length];

    // Upload to Telegram
    const telegramForm = new FormData();
    telegramForm.append('chat_id', CHANNEL_ID);
    telegramForm.append('document', chunkFile);
    telegramForm.append('caption', `Chunk ${chunkIndex} - ${fileId}`);

    const telegramResponse = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/sendDocument`, {
      method: 'POST',
      body: telegramForm
    });

    if (!telegramResponse.ok) {
      throw new Error(`Telegram error: ${telegramResponse.status}`);
    }

    const telegramData = await telegramResponse.json();
    if (!telegramData.ok || !telegramData.result?.document?.file_id) {
      throw new Error('Invalid Telegram response');
    }

    const telegramFileId = telegramData.result.document.file_id;
    const telegramMessageId = telegramData.result.message_id;

    // Get file URL
    const getFileResponse = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/getFile?file_id=${encodeURIComponent(telegramFileId)}`);
    const getFileData = await getFileResponse.json();

    if (!getFileData.ok || !getFileData.result?.file_path) {
      throw new Error('Failed to get file path');
    }

    const directUrl = `https://api.telegram.org/file/bot${BOT_TOKEN}/${getFileData.result.file_path}`;

    // Store in KV
    const chunkKey = `${fileId}_chunk_${chunkIndex}`;
    const chunkMetadata = {
      telegramFileId,
      telegramMessageId,
      directUrl,
      size: chunkBlob.size,
      index: chunkIndex,
      parentFileId: fileId,
      kvNamespace: targetKV.name,
      uploadedAt: Date.now(),
      version: '5.0'
    };

    await targetKV.kv.put(chunkKey, JSON.stringify(chunkMetadata));

    console.log(`✅ Chunk ${chunkIndex} uploaded to ${targetKV.name}`);

    return new Response(JSON.stringify({
      success: true,
      chunkIndex,
      kvNamespace: targetKV.name,
      telegramFileId
    }), {
      status: 200,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (error) {
    console.error('Chunk upload error:', error.message);
    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}
