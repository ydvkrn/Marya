// /functions/finalize-upload.js - Store Master Metadata
export async function onRequest(context) {
  const { request, env } = context;

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type'
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: corsHeaders });
  }

  try {
    const body = await request.json();
    const { fileId, filename, size, contentType, extension, chunks } = body;

    // Get KV
    const FILES_KV = env.FILES_KV;
    if (!FILES_KV) throw new Error('No KV');

    const masterMetadata = {
      filename,
      size,
      contentType,
      extension,
      uploadedAt: Date.now(),
      type: 'multi_kv_frontend_chunked',
      version: '5.0',
      totalChunks: chunks.length,
      chunks
    };

    await FILES_KV.put(fileId, JSON.stringify(masterMetadata));

    const baseUrl = new URL(request.url).origin;

    return new Response(JSON.stringify({
      success: true,
      data: {
        id: fileId,
        filename,
        size,
        urls: {
          view: `${baseUrl}/btfstorage/file/${fileId}${extension}`,
          download: `${baseUrl}/btfstorage/file/${fileId}${extension}?dl=1`,
          stream: `${baseUrl}/btfstorage/file/${fileId}${extension}?stream=1`
        }
      }
    }), {
      status: 200,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (error) {
    return new Response(JSON.stringify({ success: false, error: error.message }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}
