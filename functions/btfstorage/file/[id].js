const MIME_TYPES = {
  // Images
  'jpg': 'image/jpeg', 'jpeg': 'image/jpeg', 'png': 'image/png', 
  'gif': 'image/gif', 'webp': 'image/webp', 'svg': 'image/svg+xml',
  'bmp': 'image/bmp', 'tiff': 'image/tiff', 'ico': 'image/x-icon',
  
  // Videos
  'mp4': 'video/mp4', 'webm': 'video/webm', 'mkv': 'video/x-matroska',
  'mov': 'video/quicktime', 'avi': 'video/x-msvideo', 'm4v': 'video/x-m4v',
  
  // Audio
  'mp3': 'audio/mpeg', 'm4a': 'audio/mp4', 'wav': 'audio/wav',
  'flac': 'audio/flac', 'aac': 'audio/aac', 'ogg': 'audio/ogg',
  
  // Documents
  'pdf': 'application/pdf', 'txt': 'text/plain', 'json': 'application/json',
  'csv': 'text/csv', 'xml': 'application/xml', 'html': 'text/html',
  'css': 'text/css', 'js': 'application/javascript',
  'doc': 'application/msword',
  'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
  'rtf': 'application/rtf',
  
  // Archives
  'zip': 'application/zip', 'rar': 'application/vnd.rar', 
  '7z': 'application/x-7z-compressed', 'tar': 'application/x-tar',
  'gz': 'application/gzip',
  
  // Code
  'py': 'text/x-python', 'java': 'text/x-java-source'
};

function getMimeType(extension) {
  const ext = extension.toLowerCase().replace('.', '');
  return MIME_TYPES[ext] || 'application/octet-stream';
}

export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.id;

  console.log('=== FILE SERVE REQUEST ===');
  console.log('File ID:', fileId);

  try {
    const actualId = fileId.includes('.') ? fileId.substring(0, fileId.lastIndexOf('.')) : fileId;
    const extension = fileId.includes('.') ? fileId.substring(fileId.lastIndexOf('.')) : '';

    // ✅ Use getWithMetadata to avoid JSON parsing errors
    const kvResult = await env.FILES_KV.getWithMetadata(actualId, { type: 'text' });
    
    if (!kvResult.value) {
      console.log('File not found in KV');
      return new Response('File not found', { 
        status: 404,
        headers: { 'Content-Type': 'text/plain' }
      });
    }

    let directUrl = kvResult.value; // Current Telegram URL
    const metadata = kvResult.metadata; // Metadata object
    
    console.log('Found file, testing URL...');

    // ✅ Handle Range requests for video streaming
    const range = request.headers.get('Range');
    const fetchOptions = {};
    if (range) {
      console.log('Range request:', range);
      fetchOptions.headers = { 'Range': range };
    }

    // Try to fetch the file
    let response = await fetch(directUrl, fetchOptions);
    
    // ✅ If URL expired (403/404), refresh it automatically
    if (!response.ok && (response.status === 403 || response.status === 404 || response.status === 410)) {
      console.log('URL expired (status:', response.status, '), refreshing...');
      
      const BOT_TOKEN = env.BOT_TOKEN;
      const telegramFileId = metadata?.telegramFileId;
      
      if (BOT_TOKEN && telegramFileId) {
        try {
          console.log('Getting fresh URL from Telegram...');
          const getFileResponse = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/getFile?file_id=${encodeURIComponent(telegramFileId)}`);
          
          if (getFileResponse.ok) {
            const getFileData = await getFileResponse.json();
            
            if (getFileData.ok && getFileData.result?.file_path) {
              // ✅ Generate fresh URL
              const freshUrl = `https://api.telegram.org/file/bot${BOT_TOKEN}/${getFileData.result.file_path}`;
              
              // ✅ Update KV with fresh URL
              await env.FILES_KV.put(actualId, freshUrl, { metadata });
              directUrl = freshUrl;
              
              console.log('✅ URL refreshed successfully');
              
              // Try fetch again with fresh URL
              response = await fetch(directUrl, fetchOptions);
            }
          }
        } catch (refreshError) {
          console.error('Failed to refresh URL:', refreshError);
        }
      }
      
      if (!response.ok) {
        console.error('File still not accessible after refresh attempt');
        return new Response(`File not accessible: ${response.status}`, { 
          status: response.status,
          headers: { 'Content-Type': 'text/plain' }
        });
      }
    }

    if (!response.ok) {
      return new Response(`File not accessible: ${response.status}`, { 
        status: response.status,
        headers: { 'Content-Type': 'text/plain' }
      });
    }

    // ✅ Setup response headers
    const headers = new Headers();

    // Copy important headers from Telegram
    for (const [key, value] of response.headers.entries()) {
      const lowerKey = key.toLowerCase();
      if (['content-length', 'content-range', 'accept-ranges', 'last-modified', 'etag'].includes(lowerKey)) {
        headers.set(key, value);
      }
    }

    // Set correct content-type
    const mimeType = getMimeType(extension);
    headers.set('Content-Type', mimeType);
    console.log('Set Content-Type:', mimeType);

    // Set CORS and cache headers
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Accept-Ranges', 'bytes');
    headers.set('Cache-Control', 'public, max-age=3600'); // 1 hour cache

    // Handle view vs download
    const url = new URL(request.url);
    const isDownload = url.searchParams.has('dl');
    const filename = metadata?.filename || fileId;

    if (isDownload) {
      headers.set('Content-Disposition', `attachment; filename="${filename}"`);
    } else {
      if (mimeType.startsWith('image/') || mimeType.startsWith('video/') || 
          mimeType.startsWith('audio/') || mimeType === 'application/pdf' ||
          mimeType.startsWith('text/')) {
        headers.set('Content-Disposition', 'inline');
      } else {
        headers.set('Content-Disposition', `attachment; filename="${filename}"`);
      }
    }

    console.log('✅ Serving file successfully');

    return new Response(response.body, {
      status: response.status,
      headers: headers
    });

  } catch (error) {
    console.error('File serve error:', error);
    return new Response(`Server error: ${error.message}`, { 
      status: 500,
      headers: { 'Content-Type': 'text/plain' }
    });
  }
}
