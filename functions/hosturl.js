import { BOT_TOKEN, CHANNEL_ID, MAX_SIZE } from './_config.js';

const cors = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type'
};

export async function onRequest({ request, env }) {
  if (request.method === 'OPTIONS') {
    return new Response(null, { headers: cors });
  }

  try {
    const url = new URL(request.url);
    const fileUrl = url.searchParams.get('url');
    
    if (!fileUrl) {
      throw new Error('No URL provided');
    }

    const fileResponse = await fetch(fileUrl);
    if (!fileResponse.ok) {
      throw new Error('Failed to fetch file from URL');
    }

    const fileBlob = await fileResponse.blob();
    const filename = fileUrl.split('/').pop() || 'download';
    
    if (fileBlob.size > MAX_SIZE) {
      throw new Error('File too large (max 2GB)');
    }

    // Upload to Telegram
    const formData = new FormData();
    formData.append('chat_id', CHANNEL_ID);
    formData.append('document', fileBlob, filename);

    const telegramResponse = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/sendDocument`, {
      method: 'POST',
      body: formData
    });

    const telegramResult = await telegramResponse.json();

    if (!telegramResult.ok) {
      throw new Error(telegramResult.description || 'Telegram upload failed');
    }

    const fileId = telegramResult.result.document.file_id;
    const getFileResponse = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/getFile?file_id=${fileId}`);
    const getFileResult = await getFileResponse.json();

    if (!getFileResult.ok) {
      throw new Error('Failed to get file path');
    }

    const filePath = getFileResult.result.file_path;
    const directUrl = `https://api.telegram.org/file/bot${BOT_TOKEN}/${filePath}`;

    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).substr(2, 8);
    const lastDot = filename.lastIndexOf('.');
    const extension = lastDot !== -1 ? filename.substring(lastDot) : '';
    const nameWithoutExt = lastDot !== -1 ? filename.substring(0, lastDot) : filename;
    const cleanName = nameWithoutExt.replace(/[^a-zA-Z0-9]/g, '').substr(0, 15);
    const slug = `${timestamp}-${random}-${cleanName}${extension}`.toLowerCase();

    const fileNumber = Math.floor(Math.random() * 100);
    const randomPart1 = Math.random().toString(36).substr(2, 3);
    const randomPart2 = Math.random().toString(36).substr(2, 3);
    const fileIdCode = `MSMfile${fileNumber}/${randomPart1}-${randomPart2}`;

    await env.FILES_KV.put(slug, directUrl, {
      metadata: {
        filename: filename,
        size: fileBlob.size,
        contentType: fileBlob.type,
        uploadedAt: Date.now(),
        fileIdCode: fileIdCode
      }
    });

    const baseUrl = new URL(request.url).origin;
    const streamUrl = `${baseUrl}/btf/${slug}/${fileIdCode}`;
    const downloadUrl = `${baseUrl}/btf/${slug}/${fileIdCode}?dl=1`;

    return new Response(JSON.stringify({
      success: true,
      filename: filename,
      size: fileBlob.size,
      contentType: fileBlob.type,
      view_url: streamUrl,
      stream_url: streamUrl,
      download_url: downloadUrl,
      file_id: fileIdCode
    }), {
      headers: { 'Content-Type': 'application/json', ...cors }
    });

  } catch (error) {
    return new Response(JSON.stringify({ 
      success: false, 
      error: error.message 
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...cors }
    });
  }
}
