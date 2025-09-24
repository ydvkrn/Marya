// functions/upload-from-url.js
// ENHANCED URL UPLOAD - Fixed memory limit issues

export async function onRequest(context) {
    const { request, env } = context;

    const corsHeaders = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type'
    };

    if (request.method === 'OPTIONS') {
        return new Response(null, { headers: corsHeaders });
    }

    if (request.method !== 'POST') {
        return new Response(JSON.stringify({ success: false, error: 'Method not allowed' }), {
            status: 405, headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
    }

    const requestStartTime = Date.now();

    try {
        const { url, streaming = false, maxSize = 100 * 1024 * 1024 } = await request.json();

        if (!url || !url.trim()) {
            throw new Error('No URL provided');
        }

        const cleanUrl = url.trim();
        console.log('ðŸŒ Processing URL:', cleanUrl.substring(0, 100) + '...');

        if (!cleanUrl.startsWith('http://') && !cleanUrl.startsWith('https://')) {
            throw new Error('URL must start with http:// or https://');
        }

        // Enhanced URL validation for problematic URLs
        const parsedUrl = new URL(cleanUrl);

        // Block potentially problematic domains/patterns that cause memory issues
        const problematicPatterns = [
            'pest.aws',
            'mega.nz',
            'mediafire.com'
        ];

        const isProblematic = problematicPatterns.some(pattern => 
            parsedUrl.hostname.includes(pattern)
        );

        if (isProblematic) {
            throw new Error('This URL appears to be from a service that may cause memory issues. Please try direct upload instead.');
        }

        // Step 1: HEAD request to check file size
        console.log('ðŸ“¡ Checking file info...');
        let fileSize = 0;
        let contentType = 'application/octet-stream';
        let filename = 'download';

        try {
            const headResponse = await fetch(cleanUrl, { 
                method: 'HEAD',
                headers: {
                    'User-Agent': 'MARYA-VAULT-ULTIMATE/2.0',
                    'Accept': '*/*',
                    'Accept-Encoding': 'identity'
                },
                signal: AbortSignal.timeout(30000) // 30 second timeout
            });

            if (headResponse.ok) {
                const contentLength = headResponse.headers.get('Content-Length');
                if (contentLength) {
                    fileSize = parseInt(contentLength);
                    if (fileSize > maxSize) {
                        throw new Error(`File too large: ${Math.round(fileSize / 1024 / 1024)}MB (max ${Math.round(maxSize / 1024 / 1024)}MB for URL uploads)`);
                    }
                }

                contentType = headResponse.headers.get('Content-Type') || contentType;

                // Extract filename
                const disposition = headResponse.headers.get('Content-Disposition');
                if (disposition && disposition.includes('filename')) {
                    const match = disposition.match(/filename[*]?=([^;]+)/);
                    if (match) {
                        filename = decodeURIComponent(match[1].replace(/['"]/g, '').trim());
                    }
                }
            }
        } catch (headError) {
            console.warn('HEAD request failed:', headError.message);
        }

        // Fallback filename extraction from URL
        if (filename === 'download') {
            try {
                const urlPath = parsedUrl.pathname;
                const urlFilename = urlPath.split('/').pop();
                if (urlFilename && urlFilename.includes('.')) {
                    filename = decodeURIComponent(urlFilename);
                }
            } catch (e) {
                filename = `url_download_${Date.now()}`;
            }
        }

        // Clean filename
        filename = filename.replace(/[<>:"/\\|?*]/g, '_').trim();
        if (!filename.includes('.')) {
            const ext = getExtensionFromMimeType(contentType);
            if (ext) filename += ext;
        }

        console.log(`ðŸ“ File info: ${filename}, ${fileSize} bytes, ${contentType}`);

        // Step 2: Download with streaming and size limits
        console.log('â¬‡ï¸ Starting download with memory optimization...');

        const downloadResponse = await fetch(cleanUrl, {
            method: 'GET',
            headers: {
                'User-Agent': 'MARYA-VAULT-ULTIMATE/2.0',
                'Accept': '*/*',
                'Accept-Encoding': 'identity',
                'Range': fileSize > maxSize ? `bytes=0-${maxSize - 1}` : undefined
            },
            signal: AbortSignal.timeout(300000) // 5 minute timeout for download
        });

        if (!downloadResponse.ok) {
            throw new Error(`Download failed: ${downloadResponse.status} ${downloadResponse.statusText}`);
        }

        // Get actual content info from response
        const actualContentType = downloadResponse.headers.get('Content-Type') || contentType;
        const actualContentLength = downloadResponse.headers.get('Content-Length');
        const actualSize = actualContentLength ? parseInt(actualContentLength) : fileSize;

        // Double check size limit
        if (actualSize > maxSize) {
            throw new Error(`File too large: ${Math.round(actualSize / 1024 / 1024)}MB (max ${Math.round(maxSize / 1024 / 1024)}MB for URL uploads)`);
        }

        // Read response with memory management
        const chunks = [];
        const reader = downloadResponse.body?.getReader();

        if (!reader) {
            throw new Error('Cannot read response stream');
        }

        let downloadedSize = 0;
        const downloadStartTime = Date.now();

        try {
            while (true) {
                const { done, value } = await reader.read();
                if (done) break;

                downloadedSize += value.length;

                // Enforce size limit during download
                if (downloadedSize > maxSize) {
                    reader.releaseLock();
                    throw new Error(`File exceeded size limit during download: ${Math.round(downloadedSize / 1024 / 1024)}MB`);
                }

                chunks.push(value);

                // Progress logging
                if (downloadedSize % (10 * 1024 * 1024) < value.length) { // Every 10MB
                    const elapsed = Date.now() - downloadStartTime;
                    const speed = downloadedSize / (elapsed / 1000);
                    console.log(`ðŸ“¥ Downloaded: ${Math.round(downloadedSize / 1024 / 1024)}MB at ${Math.round(speed / 1024 / 1024)}MB/s`);
                }
            }
        } finally {
            reader.releaseLock();
        }

        const downloadTime = Date.now() - downloadStartTime;
        console.log(`âœ… Download completed: ${Math.round(downloadedSize / 1024 / 1024)}MB in ${Math.round(downloadTime / 1000)}s`);

        // Combine chunks efficiently
        const combinedArray = new Uint8Array(downloadedSize);
        let offset = 0;
        for (const chunk of chunks) {
            combinedArray.set(chunk, offset);
            offset += chunk.length;
        }

        // Create file object
        const file = new File([combinedArray.buffer], filename, { 
            type: actualContentType,
            lastModified: Date.now()
        });

        console.log('ðŸ“¤ Forwarding to upload handler...');

        // Forward to existing upload handler
        const uploadFormData = new FormData();
        uploadFormData.append('file', file);

        const uploadResponse = await fetch(new URL('/upload', request.url), {
            method: 'POST',
            body: uploadFormData,
            headers: {
                'X-Forwarded-Proto': new URL(request.url).protocol.slice(0, -1),
                'X-Forwarded-Host': new URL(request.url).hostname,
                'X-URL-Upload': 'true'
            }
        });

        if (!uploadResponse.ok) {
            const errorText = await uploadResponse.text();
            throw new Error(`Upload handler failed: ${uploadResponse.status} - ${errorText}`);
        }

        const uploadResult = await uploadResponse.json();

        if (!uploadResult.success) {
            throw new Error(uploadResult.error || 'Upload handler returned error');
        }

        // Return enhanced response
        const totalProcessingTime = Date.now() - requestStartTime;

        return new Response(JSON.stringify({
            success: true,
            url: uploadResult.url,
            download: uploadResult.download,
            filename: filename,
            size: downloadedSize,
            contentType: actualContentType,
            source: 'url_upload',
            stats: {
                downloadTime: downloadTime,
                uploadTime: totalProcessingTime - downloadTime,
                totalTime: totalProcessingTime,
                downloadSpeed: Math.round(downloadedSize / (downloadTime / 1000)),
                originalUrl: cleanUrl.substring(0, 100) + (cleanUrl.length > 100 ? '...' : '')
            }
        }), {
            headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });

    } catch (error) {
        console.error('ðŸ’¥ URL upload error:', error);

        const processingTime = Date.now() - requestStartTime;
        let userFriendlyError = error.message;

        // Make errors more user-friendly
        if (error.message.includes('Memory limit')) {
            userFriendlyError = 'File is too large for URL upload. Please try direct upload or use a smaller file.';
        } else if (error.message.includes('timeout')) {
            userFriendlyError = 'Download timed out. The source server may be slow or the file too large.';
        } else if (error.message.includes('problematic')) {
            userFriendlyError = error.message;
        } else if (error.message.includes('too large')) {
            userFriendlyError = error.message;
        } else if (error.message.includes('Network')) {
            userFriendlyError = 'Network error. Please check the URL and try again.';
        }

        return new Response(JSON.stringify({
            success: false,
            error: userFriendlyError,
            processingTime: processingTime,
            debug: {
                originalError: error.message,
                timestamp: new Date().toISOString()
            }
        }), {
            status: 500,
            headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
    }
}

// Helper function for MIME type to extension mapping
function getExtensionFromMimeType(mimeType) {
    const mimeMap = {
        'video/mp4': '.mp4', 'video/webm': '.webm', 'video/x-msvideo': '.avi', 
        'video/quicktime': '.mov', 'video/x-matroska': '.mkv',
        'audio/mpeg': '.mp3', 'audio/wav': '.wav', 'audio/aac': '.aac',
        'image/jpeg': '.jpg', 'image/png': '.png', 'image/gif': '.gif', 
        'image/webp': '.webp', 'image/svg+xml': '.svg',
        'application/pdf': '.pdf', 'application/zip': '.zip', 
        'application/x-rar-compressed': '.rar', 'application/x-7z-compressed': '.7z',
        'text/plain': '.txt', 'text/html': '.html', 'text/css': '.css',
        'application/javascript': '.js', 'application/json': '.json'
    };

    return mimeMap[mimeType.toLowerCase()] || '';
}