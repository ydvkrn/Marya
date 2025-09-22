// Add this function at the end of your existing upload.js

// Handle URL uploads (like ddl.safone.co URLs)
async function handleUrlUpload(url, env) {
  console.log('🌐 URL Upload:', url);
  
  try {
    // Download the file from URL
    const response = await fetch(url, {
      signal: AbortSignal.timeout(60000), // 60 second timeout
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
      }
    });
    
    if (!response.ok) {
      throw new Error(`Failed to download: ${response.status} ${response.statusText}`);
    }
    
    const contentLength = response.headers.get('content-length');
    const contentType = response.headers.get('content-type') || 'application/octet-stream';
    
    if (!contentLength) {
      throw new Error('Content length not available');
    }
    
    const fileSize = parseInt(contentLength, 10);
    const maxSize = 2 * 1024 * 1024 * 1024; // 2GB limit
    
    if (fileSize > maxSize) {
      throw new Error(`File too large: ${Math.round(fileSize/1024/1024/1024)}GB (max 2GB)`);
    }
    
    // Extract filename from URL
    const urlParts = new URL(url);
    let filename = urlParts.pathname.split('/').pop() || 'download';
    
    // If no extension, try to guess from content-type
    if (!filename.includes('.')) {
      if (contentType.includes('video/mp4')) filename += '.mp4';
      else if (contentType.includes('video/')) filename += '.mp4';
      else if (contentType.includes('audio/')) filename += '.mp3';
      else if (contentType.includes('image/')) filename += '.jpg';
    }
    
    console.log(`📁 URL file: ${filename} (${Math.round(fileSize/1024/1024)}MB)`);
    
    // Create File object from response
    const fileBuffer = await response.arrayBuffer();
    const file = new File([fileBuffer], filename, { type: contentType });
    
    return file;
    
  } catch (error) {
    console.error('❌ URL upload error:', error);
    throw new Error(`URL upload failed: ${error.message}`);
  }
}

// Update your main upload function to handle both file and URL uploads
// Add this at the beginning of your onRequest function:

// Check if it's a URL upload
const contentType = request.headers.get('content-type');
if (contentType && contentType.includes('application/json')) {
  const data = await request.json();
  if (data.url) {
    console.log('🌐 URL upload detected:', data.url);
    
    try {
      const file = await handleUrlUpload(data.url, env);
      // Continue with normal upload process using this file
      // (rest of your upload code remains the same)
    } catch (urlError) {
      return new Response(JSON.stringify({
        success: false,
        error: `URL upload failed: ${urlError.message}`
      }), {
        status: 400,
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }
  }
}