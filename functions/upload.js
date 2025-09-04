export async function onRequest(context) {
  const { request, env } = context;

  console.log('=== UPLOAD REQUEST START ===');

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
      error: 'Method not allowed' 
    }), {
      status: 405,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }

  try {
    // Bot credentials
    const BOT_TOKEN = '8360624116:AAEEJha8CRgL8TnrEKk5zOuCNXXRawmbuaE';
    const CHANNEL_ID = '-1003071466750';

    console.log('Getting form data...');
    const formData = await request.formData();
    const file = formData.get('file');

    if (!file) {
      throw new Error('No file provided');
    }

    console.log('File details:', {
      name: file.name,
      size: file.size,
      type: file.type
    });

    if (file.size > 2147483648) { // 2GB
      throw new Error('File too large (max 2GB)');
    }

    // ✅ CRITICAL FIX: Proper Telegram upload
    console.log('Creating Telegram form data...');
    const telegramForm = new FormData();
    telegramForm.append('chat_id', CHANNEL_ID);
    telegramForm.append('document', file, file.name);

    console.log('Uploading to Telegram...');
    const telegramResponse = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/sendDocument`, {
      method: 'POST',
      body: telegramForm
    });

    console.log('Telegram response status:', telegramResponse.status);
    console.log('Telegram response headers:', Object.fromEntries(telegramResponse.headers.entries()));

    if (!telegramResponse.ok) {
      const errorText = await telegramResponse.text();
      console.error('Telegram API error response:', errorText);
      throw new Error(`Telegram upload failed (${telegramResponse.status}): ${errorText}`);
    }

    // ✅ CRITICAL FIX: Safe JSON parsing
    let telegramData;
    const responseText = await telegramResponse.text();
    console.log('Telegram response text (first 200 chars):', responseText.substring(0, 200));

    try {
      telegramData = JSON.parse(responseText);
      console.log('Parsed Telegram response successfully');
    } catch (parseError) {
      console.error('JSON parse error:', parseError.message);
      console.error('Response was:', responseText);
      throw new Error(`Invalid JSON response from Telegram: ${parseError.message}`);
    }

    // ✅ CRITICAL FIX: Validate response structure
    if (!telegramData || typeof telegramData !== 'object') {
      throw new Error('Telegram returned non-object response');
    }

    if (!telegramData.ok) {
      const errorMsg = telegramData.description || 'Unknown Telegram error';
      console.error('Telegram API error:', errorMsg);
      throw new Error(`Telegram API error: ${errorMsg}`);
    }

    if (!telegramData.result) {
      console.error('No result in Telegram response:', telegramData);
      throw new Error('No result in Telegram response');
    }

    if (!telegramData.result.document) {
      console.error('No document in result:', telegramData.result);
      throw new Error('No document in Telegram result');
    }

    const document = telegramData.result.document;
    const fileId = document.file_id;

    if (!fileId) {
      console.error('No file_id in document:', document);
      throw new Error('No file_id in Telegram document');
    }

    console.log('File uploaded to Telegram successfully, file_id:', fileId);

    // ✅ CRITICAL FIX: Get file URL with proper error handling
    console.log('Getting file URL from Telegram...');
    const getFileResponse = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/getFile?file_id=${encodeURIComponent(fileId)}`);

    console.log('GetFile response status:', getFileResponse.status);

    if (!getFileResponse.ok) {
      const errorText = await getFileResponse.text();
      console.error('GetFile API error:', errorText);
      throw new Error(`GetFile failed (${getFileResponse.status}): ${errorText}`);
    }

    // ✅ CRITICAL FIX: Safe JSON parsing for getFile
    let getFileData;
    const getFileText = await getFileResponse.text();
    console.log('GetFile response text (first 200 chars):', getFileText.substring(0, 200));

    try {
      getFileData = JSON.parse(getFileText);
      console.log('Parsed GetFile response successfully');
    } catch (parseError) {
      console.error('GetFile JSON parse error:', parseError.message);
      console.error('GetFile response was:', getFileText);
      throw new Error(`Invalid JSON response from GetFile: ${parseError.message}`);
    }

    // ✅ Validate getFile response
    if (!getFileData || !getFileData.ok) {
      const errorMsg = getFileData?.description || 'GetFile failed';
      console.error('GetFile API error:', errorMsg);
      throw new Error(`GetFile API error: ${errorMsg}`);
    }

    if (!getFileData.result || !getFileData.result.file_path) {
      console.error('No file_path in getFile result:', getFileData);
      throw new Error('No file_path in GetFile result');
    }

    const filePath = getFileData.result.file_path;
    const directUrl = `https://api.telegram.org/file/bot${BOT_TOKEN}/${filePath}`;

    console.log('Direct URL created successfully');

    // Generate unique slug
    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).substr(2, 8);
    const extension = file.name.includes('.') ? '.' + file.name.split('.').pop().toLowerCase() : '';
    const slug = `${timestamp}${random}${extension}`;

    console.log('Generated slug:', slug);

    // ✅ Store in KV with error handling
    try {
      if (env.FILES_KV) {
        await env.FILES_KV.put(slug, directUrl, {
          metadata: {
            filename: file.name,
            size: file.size,
            contentType: file.type,
            uploadedAt: Date.now()
          }
        });
        console.log('File stored in KV successfully');
      } else {
        console.warn('FILES_KV not available');
      }
    } catch (kvError) {
      console.error('KV storage error:', kvError.message);
      // Don't fail the upload, continue without KV
    }

    // Generate URLs
    const baseUrl = new URL(request.url).origin;
    const viewUrl = `${baseUrl}/f/${slug}`;
    const downloadUrl = `${baseUrl}/f/${slug}?dl=1`;

    console.log('URLs generated:', { viewUrl, downloadUrl });

    const result = {
      success: true,
      filename: file.name,
      size: file.size,
      contentType: file.type,
      url: viewUrl,
      download: downloadUrl
    };

    console.log('Upload completed successfully');
    console.log('=== UPLOAD REQUEST END ===');

    return new Response(JSON.stringify(result), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (error) {
    console.error('=== UPLOAD ERROR ===');
    console.error('Error type:', error.constructor.name);
    console.error('Error message:', error.message);
    console.error('Error stack:', error.stack);
    console.error('=== END ERROR ===');

    return new Response(JSON.stringify({
      success: false,
      error: error.message || 'Unknown error occurred',
      debug: process.env.NODE_ENV === 'development' ? {
        stack: error.stack,
        name: error.constructor.name
      } : undefined
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}
