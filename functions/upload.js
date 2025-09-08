export async function onRequest(context) {
  const { request, env } = context;

  console.log('=== SIMPLE BULLETPROOF UPLOAD ===');

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type'
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const BOT_TOKEN = env.BOT_TOKEN;
    const CHANNEL_ID = env.CHANNEL_ID;
    const FILES_KV = env.FILES_KV;

    if (!BOT_TOKEN || !CHANNEL_ID || !FILES_KV) {
      throw new Error('Missing environment variables');
    }

    const formData = await request.formData();
    const file = formData.get('file');

    if (!file) {
      throw new Error('No file provided');
    }

    console.log(`📤 Uploading: ${file.name} (${file.size} bytes)`);

    // Generate simple ID
    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).slice(2, 8);
    const fileId = `file${timestamp}${random}`;
    const extension = file.name.includes('.') ? file.name.slice(file.name.lastIndexOf('.')) : '';

    // ✅ SIMPLE STRATEGY - No complex chunking
    if (file.size > 50 * 1024 * 1024) { // 50MB limit
      throw new Error('File too large. Max 50MB allowed.');
    }

    // Upload to Telegram with retry
    let uploadSuccess = false;
    let telegramFileId, directUrl;
    
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        console.log(`📡 Upload attempt ${attempt}/3`);
        
        const telegramForm = new FormData();
        telegramForm.append('chat_id', CHANNEL_ID);
        telegramForm.append('document', file);

        const telegramResponse = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/sendDocument`, {
          method: 'POST',
          body: telegramForm
        });

        if (telegramResponse.ok) {
          const telegramData = await telegramResponse.json();
          
          if (telegramData.ok && telegramData.result?.document?.file_id) {
            telegramFileId = telegramData.result.document.file_id;

            // Get direct URL
            const getFileResponse = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/getFile?file_id=${encodeURIComponent(telegramFileId)}`);
            
            if (getFileResponse.ok) {
              const getFileData = await getFileResponse.json();
              
              if (getFileData.ok && getFileData.result?.file_path) {
                directUrl = `https://api.telegram.org/file/bot${BOT_TOKEN}/${getFileData.result.file_path}`;
                uploadSuccess = true;
                console.log(`✅ Upload successful on attempt ${attempt}`);
                break;
              }
            }
          }
        }
      } catch (error) {
        console.log(`❌ Attempt ${attempt} failed:`, error.message);
        if (attempt < 3) {
          await new Promise(resolve => setTimeout(resolve, 1000 * attempt));
        }
      }
    }

    if (!uploadSuccess) {
      throw new Error('Upload failed after 3 attempts');
    }

    // ✅ Store simple metadata in KV
    const simpleMetadata = {
      filename: file.name,
      size: file.size,
      contentType: file.type,
      extension: extension,
      uploadedAt: Date.now(),
      telegramFileId: telegramFileId,
      directUrl: directUrl,
      type: 'simple_bulletproof'
    };

    await FILES_KV.put(fileId, JSON.stringify(simpleMetadata));

    const baseUrl = new URL(request.url).origin;
    const fileUrl = `${baseUrl}/file/${fileId}${extension}`;

    console.log('✅ BULLETPROOF upload completed successfully!');

    return new Response(JSON.stringify({
      success: true,
      filename: file.name,
      size: file.size,
      contentType: file.type,
      url: fileUrl,
      download: `${fileUrl}?dl=1`,
      id: fileId,
      message: 'File uploaded successfully!'
    }), {
      status: 200,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (error) {
    console.error('❌ Upload error:', error);
    return new Response(JSON.stringify({
      success: false,
      error: error.message,
      message: 'Upload failed. Please try again.'
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}
