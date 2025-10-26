// functions/upload-from-url.js
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
    const { url } = await request.json();

    if (!url) {
      throw new Error('No URL provided');
    }

    // Download file
    const downloadResponse = await fetch(url, {
      headers: { 'User-Agent': 'Mozilla/5.0' }
    });

    if (!downloadResponse.ok) {
      throw new Error(`Download failed: ${downloadResponse.status}`);
    }

    // Get filename
    let filename = 'download';
    const contentDisposition = downloadResponse.headers.get('Content-Disposition');
    if (contentDisposition) {
      const match = contentDisposition.match(/filename[*]?=([^;

"']+)/);
      if (match) {
        filename = match[1].replace(/['"]/g, '').trim();
      }
    } else {
      const urlObj = new URL(url);
      const urlFilename = urlObj.pathname.split('/').pop();
      if (urlFilename) {
        filename = urlFilename;
      }
    }

    const arrayBuffer = await downloadResponse.arrayBuffer();
    const contentType = downloadResponse.headers.get('Content-Type') || 'application/octet-stream';

    // Generate filename
    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).substring(2, 8);
    const ext = filename.includes('.') ? filename.substring(filename.lastIndexOf('.')) : '';
    const baseName = filename.substring(0, filename.lastIndexOf('.') || filename.length);
    const sanitized = baseName.toLowerCase().replace(/[^a-z0-9-]/g, '_').substring(0, 40);
    const finalFilename = `${sanitized}_${timestamp}${random}${ext}`;

    // Create file
    const file = new File([arrayBuffer], filename, { type: contentType });

    // Upload to Telegram
    const telegramFormData = new FormData();
    telegramFormData.append('chat_id', env.CHAT_ID);
    telegramFormData.append('document', file);
    telegramFormData.append('caption', `🌐 ${filename}`);

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
      originalName: filename,
      size: arrayBuffer.byteLength,
      contentType: contentType,
      telegramFileId: data.result.document.file_id,
      uploadedAt: Date.now()
    };

    await env.FILES_KV.put(finalFilename, JSON.stringify(metadata));

    // Return response
    const baseUrl = new URL(request.url).origin;
    
    return new Response(JSON.stringify({
      success: true,
      filename: finalFilename,
      id: timestamp + random,
      originalName: filename,
      size: arrayBuffer.byteLength,
      contentType: contentType,
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