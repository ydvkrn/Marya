// functions/upload.js
export async function onRequest(context) {
  const { request, env } = context;

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: corsHeaders });
  }

  try {
    const formData = await request.formData();
    const file = formData.get('file');

    if (!file || file.size === 0) {
      throw new Error('No file provided');
    }

    // Generate filename
    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).substring(2, 8);
    const ext = file.name.includes('.') ? file.name.substring(file.name.lastIndexOf('.')) : '';
    const baseName = file.name.substring(0, file.name.lastIndexOf('.') || file.name.length);
    const sanitized = baseName.toLowerCase().replace(/[^a-z0-9-]/g, '_').substring(0, 40);
    const finalFilename = `${sanitized}_${timestamp}${random}${ext}`;

    // Upload to Telegram
    const telegramFormData = new FormData();
    telegramFormData.append('chat_id', env.CHAT_ID);
    telegramFormData.append('document', file);
    telegramFormData.append('caption', `📁 ${file.name}`);

    const response = await fetch(
      `https://api.telegram.org/bot${env.BOT_TOKEN}/sendDocument`,
      { method: 'POST', body: telegramFormData }
    );

    const data = await response.json();

    if (!data.ok) {
      throw new Error('Telegram upload failed');
    }

    // Store in KV
    const metadata = {
      filename: finalFilename,
      originalName: file.name,
      size: file.size,
      contentType: file.type || 'application/octet-stream',
      telegramFileId: data.result.document.file_id,
      uploadedAt: Date.now()
    };

    await env.FILES_KV.put(finalFilename, JSON.stringify(metadata));

    // Return response matching frontend expectations
    const baseUrl = new URL(request.url).origin;
    
    return new Response(JSON.stringify({
      success: true,
      filename: finalFilename,
      id: timestamp + random,
      originalName: file.name,
      size: file.size,
      contentType: file.type || 'application/octet-stream',
      url: `${baseUrl}/btfstorage/file/${finalFilename}`,
      download: `${baseUrl}/btfstorage/file/${finalFilename}?dl=1`,
      uploadedAt: new Date().toISOString()
    }), {
      status: 200,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });

  } catch (error) {
    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), {
      status: 500,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });
  }
}