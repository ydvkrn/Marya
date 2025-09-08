const MIME_TYPES = {
  'mp4': 'video/mp4', 'webm': 'video/webm', 'mov': 'video/quicktime',
  'avi': 'video/x-msvideo', 'mkv': 'video/x-matroska',
  'mp3': 'audio/mpeg', 'wav': 'audio/wav', 'flac': 'audio/flac',
  'jpg': 'image/jpeg', 'jpeg': 'image/jpeg', 'png': 'image/png',
  'gif': 'image/gif', 'webp': 'image/webp', 'svg': 'image/svg+xml',
  'pdf': 'application/pdf', 'txt': 'text/plain', 'json': 'application/json',
  'zip': 'application/zip', 'rar': 'application/vnd.rar'
};

function getMimeType(extension) {
  const ext = extension.toLowerCase().replace('.', '');
  return MIME_TYPES[ext] || 'application/octet-stream';
}

export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.id;

  console.log('=== FAST FILE SERVING ===');
  console.log('File ID:', fileId);

  try {
    const actualId = fileId.includes('.') ? fileId.substring(0, fileId.lastIndexOf('.')) : fileId;
    const extension = fileId.includes('.') ? fileId.substring(fileId.lastIndexOf('.')) : '';
    
    const FILES_KV = env.FILES_KV;
    
    // Get file metadata
    const metadataString = await FILES_KV.get(actualId);
    if (!metadataString) {
      return new Response('File not found', { 
        status: 404,
        headers: { 'Content-Type': 'text/plain' }
      });
    }

    let metadata;
    try {
      metadata = JSON.parse(metadataString);
    } catch {
      // Legacy format - direct URL
      metadata = {
        directUrl: metadataString,
        filename: actualId,
        type: 'legacy'
      };
    }

    console.log(`📁 Serving: ${metadata.filename || actualId}`);

    let directUrl = metadata.directUrl;
    
    // ✅ Try to fetch the file
    let response = await fetch(directUrl, {
      headers: request.headers
    });

    // ✅ If URL expired, refresh it
    if (!response.ok && (response.status === 403 || response.status === 404 || response.status === 410)) {
      console.log('🔄 URL expired, refreshing...');
      
      if (metadata.telegramFileId && env.BOT_TOKEN) {
        try {
          const getFileResponse = await fetch(`https://api.telegram.org/bot${env.BOT_TOKEN}/getFile?file_id=${encodeURIComponent(metadata.telegramFileId)}`);
          
          if (getFileResponse.ok) {
            const getFileData = await getFileResponse.json();
            
            if (getFileData.ok && getFileData.result?.file_path) {
              const freshUrl = `https://api.telegram.org/file/bot${env.BOT_TOKEN}/${getFileData.result.file_path}`;
              
              // Update KV with fresh URL
              const updatedMetadata = {
                ...metadata,
                directUrl: freshUrl,
                lastRefreshed: Date.now()
              };
              
              await FILES_KV.put(actualId, JSON.stringify(updatedMetadata));
              
              console.log('✅ URL refreshed successfully');
              response = await fetch(freshUrl, {
                headers: request.headers
              });
            }
          }
        } catch (refreshError) {
          console.error('❌ Failed to refresh URL:', refreshError);
        }
      }
    }

    if (!response.ok) {
      return new Response(`File not accessible: ${response.status}`, { 
        status: response.status,
        headers: { 'Content-Type': 'text/plain' }
      });
    }

    // ✅ Create optimized response
    const headers = new Headers();
    const mimeType = getMimeType(extension);
    
    // Copy important headers
    if (response.headers.get('content-length')) {
      headers.set('Content-Length', response.headers.get('content-length'));
    }
    if (response.headers.get('content-range')) {
      headers.set('Content-Range', response.headers.get('content-range'));
    }
    if (response.headers.get('accept-ranges')) {
      headers.set('Accept-Ranges', response.headers.get('accept-ranges'));
    }

    headers.set('Content-Type', mimeType);
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Cache-Control', 'public, max-age=31536000');
    
    // ✅ Optimize for media files
    if (mimeType.startsWith('video/') || mimeType.startsWith('audio/')) {
      headers.set('Accept-Ranges', 'bytes');
      headers.set('Content-Disposition', 'inline');
    } else if (mimeType.startsWith('image/')) {
      headers.set('Content-Disposition', 'inline');
    }

    // Handle download parameter
    const url = new URL(request.url);
    if (url.searchParams.has('dl')) {
      const filename = metadata.filename || 'download';
      headers.set('Content-Disposition', `attachment; filename="${filename}"`);
    }

    console.log('✅ File served successfully');

    return new Response(response.body, {
      status: response.status,
      headers: headers
    });

  } catch (error) {
    console.error('❌ File serving error:', error);
    return new Response(`Server error: ${error.message}`, { 
      status: 500,
      headers: { 'Content-Type': 'text/plain' }
    });
  }
}
