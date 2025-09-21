export async function onRequest(context) {
  const { request, env } = context;

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type'
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  if (request.method !== 'POST') {
    return new Response(JSON.stringify({
      success: false,
      error: `Method ${request.method} not allowed`
    }), {
      status: 405,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }

  try {
    const formData = await request.formData();
    const file = formData.get('file');
    const isChunk = formData.get('isChunk') === 'true';

    if (!file) {
      throw new Error('No file provided');
    }

    if (isChunk) {
      return await handleChunkedUpload(formData, env, request, corsHeaders);
    } else {
      return await handleDirectUpload(file, env, request, corsHeaders);
    }

  } catch (error) {
    console.error('Upload error:', error);
    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}

// Handle chunked upload with 2GB support
async function handleChunkedUpload(formData, env, request, corsHeaders) {
  const file = formData.get('file');
  const chunkIndex = parseInt(formData.get('chunkIndex'));
  const totalChunks = parseInt(formData.get('totalChunks'));
  const originalFilename = formData.get('originalFilename');
  const originalSize = parseInt(formData.get('originalSize'));
  const fileId = formData.get('fileId');

  console.log(`📦 Processing chunk ${chunkIndex + 1}/${totalChunks} for ${originalFilename} (${Math.round(originalSize/1024/1024)}MB)`);

  // Check file size limit (2GB = 2147483648 bytes)
  const MAX_SIZE = 2 * 1024 * 1024 * 1024; // 2GB
  if (originalSize > MAX_SIZE) {
    throw new Error(`File too large: ${Math.round(originalSize/1024/1024/1024)}GB. Maximum allowed: 2GB`);
  }

  // Bot tokens
  const botTokens = [
    env.BOT_TOKEN,
    env.BOT_TOKEN2,
    env.BOT_TOKEN3,
    env.BOT_TOKEN4,
    env.BOT_TOKEN5,
    env.BOT_TOKEN6
  ].filter(token => token);

  const CHANNEL_ID = env.CHANNEL_ID;

  if (botTokens.length === 0 || !CHANNEL_ID) {
    throw new Error('Bot tokens or channel ID not configured');
  }

  // Extended KV namespaces for larger files
  const kvNamespaces = [
    { kv: env.FILES_KV, name: 'FILES_KV' },
    { kv: env.FILES_KV2, name: 'FILES_KV2' },
    { kv: env.FILES_KV3, name: 'FILES_KV3' },
    { kv: env.FILES_KV4, name: 'FILES_KV4' },
    { kv: env.FILES_KV5, name: 'FILES_KV5' },
    { kv: env.FILES_KV6, name: 'FILES_KV6' },
    { kv: env.FILES_KV7, name: 'FILES_KV7' },
    { kv: env.FILES_KV8, name: 'FILES_KV8' },
    { kv: env.FILES_KV9, name: 'FILES_KV9' },
    { kv: env.FILES_KV10, name: 'FILES_KV10' }
  ].filter(item => item.kv);

  // Upload chunk
  const kvIndex = Math.floor(chunkIndex / 50); // 50 keys per KV for larger files
  const targetKV = kvNamespaces[kvIndex];
  const botToken = botTokens[chunkIndex % botTokens.length];

  if (!targetKV) {
    throw new Error(`KV namespace ${kvIndex} not available for chunk ${chunkIndex}`);
  }

  const chunkResult = await uploadSingleChunk(
    file, fileId, chunkIndex, kvIndex, chunkIndex % 50,
    botToken, CHANNEL_ID, targetKV, originalFilename
  );

  // Progress tracking
  const progressKey = `progress_${fileId}`;
  let progressData;
  
  try {
    const existing = await kvNamespaces[0].kv.get(progressKey);
    progressData = existing ? JSON.parse(existing) : {
      originalFilename,
      originalSize,
      totalChunks,
      uploadedChunks: new Array(totalChunks).fill(null),
      startTime: Date.now()
    };
  } catch (e) {
    progressData = {
      originalFilename,
      originalSize,
      totalChunks,
      uploadedChunks: new Array(totalChunks).fill(null),
      startTime: Date.now()
    };
  }

  // Record chunk
  progressData.uploadedChunks[
