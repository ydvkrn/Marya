// MIME type mapping for proper content-type detection
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
  'py': 'text/x-python',
  'java': 'text/x-java-source'
};

function getMimeType(extension) {
  const ext = extension.toLowerCase().replace('.', '');
  return MIME_TYPES[ext] || 'application/octet-stream';
}

export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.id; // This will be like "id1234abc5.png"

  console.log('=== FILE SERVE REQUEST ===');
  console.log('File ID:', fileId);

  try {
    // Extract the actual ID (remove extension)
    const actualId = fileId.includes('.') ? fileId.substring(0, fileId.lastIndexOf('.')) : fileId;
    const extension = fileId.includes('.') ? fileId.substring(fileId.lastIndexOf('.')) : '';

    console.log('Actual ID:', actualId);
    console.log('Extension:', extension);

    // Get file URL from KV
    const directUrl = await env.FILES_KV.get(actualId, 'text');
    const metadata = await env.FILES_KV.get(actualId, { type: 'json' });

    if (!directUrl) {
      console.log('File not found in KV');
      return new Response('File not found', { 
        status: 404,
        headers: { 'Content-Type': 'text/plain' }
      });
    }

    console.log('Found file, fetching from Telegram...');

    // Handle Range requests for video streaming
    const range = request.headers.get('Range');
    const fetchOptions = {};

    if (range) {
      console.log('Range request:', range);
      fetchOptions.headers = { 'Range': range };
    }

    // Fetch from Telegram
    const response = await fetch(directUrl, fetchOptions);

    console.log('Telegram response:', response.status, response.ok);

    if (!response.ok) {
      return new Response(`File not accessible: ${response.status}`, { 
        status: response.status,
        headers: { 'Content-Type': 'text/plain' }
      });
    }

    // Setup response headers
    const headers = new Headers();

    // Copy important headers from Telegram
    for (const [key, value] of response.headers.entries()) {
      const lowerKey = key.toLowerCase();
      if (['content-length', 'content-range', 'accept-ranges', 'last-modified', 'etag'].includes(lowerKey)) {
        headers.set(key, value);
      }
    }

    // ✅ CRITICAL: Set correct content-type based on extension
    const mimeType = getMimeType(extension);
    headers.set('Content-Type', mimeType);
    console.log('Set Content-Type:', mimeType);

    // Set CORS and cache headers
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Accept-Ranges', 'bytes');
    headers.set('Cache-Control', 'public, max-age=31536000, immutable');

    // ✅ Handle view vs download
    const url = new URL(request.url);
    const isDownload = url.searchParams.has('dl');
    const filename = metadata?.metadata?.filename || fileId;

    console.log('Is download?', isDownload);
    console.log('Filename:', filename);

    if (isDownload) {
      // Force download
      headers.set('Content-Disposition', `attachment; filename="${filename}"`);
      console.log('Set as attachment for download');
    } else {
      // Show inline for images, videos, PDFs, text
      if (mimeType.startsWith('image/') || 
          mimeType.startsWith('video/') || 
          mimeType.startsWith('audio/') ||
          mimeType === 'application/pdf' ||
          mimeType.startsWith('text/')) {
        headers.set('Content-Disposition', 'inline');
        console.log('Set as inline for viewing');
      } else {
        // Force download for other file types
        headers.set('Content-Disposition', `attachment; filename="${filename}"`);
        console.log('Set as attachment for unknown type');
      }
    }

    console.log('Serving file successfully');

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
