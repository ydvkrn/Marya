/**
 * 🚀 MARYA VAULT - UPLOAD.JS
 * Cloudflare Worker Handler for BTFSTORAGE + Telegram Upload
 * Designed for [id].js integration with perfect URL matching
 */

export default {
    async fetch(request, env, ctx) {
        // CORS Headers
        const corsHeaders = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization'
        };

        // CORS Preflight
        if (request.method === 'OPTIONS') {
            return new Response(null, { headers: corsHeaders });
        }

        // POST - Upload Handler
        if (request.method === 'POST' && request.url.includes('/api/upload')) {
            return await handleUpload(request, env, ctx, corsHeaders);
        }

        // Default 404
        return new Response(JSON.stringify({ error: 'Not found' }), {
            status: 404,
            headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
    }
};

/**
 * Handle File Upload
 * 1. Receive FormData with file + metadata
 * 2. Split into 20MB chunks
 * 3. Upload chunks to Telegram via bot
 * 4. Store metadata in KV namespace
 * 5. Return fileId + chunk info for [id].js
 */
async function handleUpload(request, env, ctx, corsHeaders) {
    try {
        // Parse FormData
        const formData = await request.formData();
        const file = formData.get('file');
        const filename = formData.get('filename') || 'upload.bin';
        const chunkSize = parseInt(formData.get('chunkSize')) || 20 * 1024 * 1024;

        // Validation
        if (!file) {
            return jsonResponse({ error: 'No file provided' }, 400, corsHeaders);
        }

        const fileBuffer = await file.arrayBuffer();
        const fileSize = fileBuffer.byteLength;

        // Validate size (max 500MB)
        if (fileSize > 500 * 1024 * 1024) {
            return jsonResponse(
                { error: 'File exceeds 500MB limit' },
                413,
                corsHeaders
            );
        }

        // Generate unique file ID (matches [id].js pattern)
        const fileId = `file_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
        const fileExt = getFileExtension(filename);

        // Split into chunks
        const chunks = [];
        const totalChunks = Math.ceil(fileSize / chunkSize);

        for (let i = 0; i < totalChunks; i++) {
            const start = i * chunkSize;
            const end = Math.min(start + chunkSize, fileSize);
            const chunkBuffer = fileBuffer.slice(start, end);
            chunks.push(new Uint8Array(chunkBuffer));
        }

        // Upload chunks to Telegram & KV
        const uploadPromises = chunks.map((chunk, index) =>
            uploadChunkToTelegram(
                env,
                fileId,
                index,
                chunk,
                filename,
                totalChunks
            )
        );

        const chunkResults = await Promise.all(uploadPromises);

        // Store metadata in KV
        const metadata = {
            fileId,
            filename,
            fileExtension: fileExt,
            originalSize: fileSize,
            chunkSize,
            totalChunks,
            uploadedAt: new Date().toISOString(),
            mimeType: getMimeType(filename),
            chunks: chunkResults.map(r => ({
                index: r.index,
                telegramFileId: r.telegramFileId,
                size: r.size,
                hash: r.hash
            })),
            status: 'complete'
        };

        // Store in KV (key: file_{fileId})
        await env.STORAGE.put(
            `file_${fileId}`,
            JSON.stringify(metadata),
            {
                expirationTtl: 30 * 24 * 60 * 60 // 30 days
            }
        );

        // Perfect response for [id].js integration
        return jsonResponse(
            {
                success: true,
                fileId,
                filename,
                fileExtension: fileExt,
                originalSize: fileSize,
                totalChunks,
                uploadedAt: metadata.uploadedAt,
                storageUrl: `/btfstorage/file/${fileId}.${fileExt}`,
                apiUrl: `/api/file/${fileId}`,
                chunkInfo: {
                    size: chunkSize,
                    count: totalChunks,
                    hashes: chunkResults.map(r => r.hash)
                },
                message: '✅ File uploaded successfully! Ready for [id].js retrieval.'
            },
            200,
            corsHeaders
        );

    } catch (error) {
        console.error('Upload error:', error);
        return jsonResponse(
            {
                error: error.message || 'Upload failed',
                details: error.stack
            },
            500,
            corsHeaders
        );
    }
}

/**
 * Upload single chunk to Telegram
 * Uses bot token from env.TELEGRAM_BOT_TOKEN
 */
async function uploadChunkToTelegram(env, fileId, index, chunkData, filename, totalChunks) {
    try {
        const botToken = env.TELEGRAM_BOT_TOKEN;
        const chatId = env.TELEGRAM_CHAT_ID || '-1001234567890'; // Your storage chat

        if (!botToken || !chatId) {
            throw new Error('Telegram credentials not configured');
        }

        // Create blob for upload
        const blob = new Blob([chunkData], { type: 'application/octet-stream' });

        // Create FormData for Telegram
        const telegramForm = new FormData();
        telegramForm.append('chat_id', chatId);
        telegramForm.append('document', blob, `${fileId}_chunk_${index}.bin`);
        telegramForm.append('caption', `FILE: ${filename}\nCHUNK: ${index + 1}/${totalChunks}`);

        // Send to Telegram
        const telegramResponse = await fetch(
            `https://api.telegram.org/bot${botToken}/sendDocument`,
            {
                method: 'POST',
                body: telegramForm
            }
        );

        const telegramResult = await telegramResponse.json();

        if (!telegramResult.ok) {
            throw new Error(`Telegram upload failed: ${telegramResult.description}`);
        }

        // Extract file_id from response
        const telegramFileId = telegramResult.result.document.file_id;

        return {
            index,
            telegramFileId,
            size: chunkData.length,
            hash: await hashChunk(chunkData)
        };

    } catch (error) {
        console.error(`Chunk ${index} upload error:`, error);
        throw error;
    }
}

/**
 * Simple hash for chunk verification
 */
async function hashChunk(data) {
    const hashBuffer = await crypto.subtle.digest('SHA-256', data);
    return Array.from(new Uint8Array(hashBuffer))
        .map(b => b.toString(16).padStart(2, '0'))
        .join('')
        .substring(0, 16);
}

/**
 * Get file extension
 */
function getFileExtension(filename) {
    return filename.split('.').pop()?.toLowerCase() || 'bin';
}

/**
 * Get MIME type
 */
function getMimeType(filename) {
    const ext = getFileExtension(filename);
    const mimeTypes = {
        'mp4': 'video/mp4',
        'mkv': 'video/x-matroska',
        'avi': 'video/x-msvideo',
        'mov': 'video/quicktime',
        'webm': 'video/webm',
        'mp3': 'audio/mpeg',
        'wav': 'audio/wav',
        'pdf': 'application/pdf',
        'zip': 'application/zip',
        'jpg': 'image/jpeg',
        'png': 'image/png',
        'gif': 'image/gif'
    };
    return mimeTypes[ext] || 'application/octet-stream';
}

/**
 * JSON Response helper
 */
function jsonResponse(data, status = 200, headers = {}) {
    return new Response(JSON.stringify(data), {
        status,
        headers: {
            'Content-Type': 'application/json',
            ...headers
        }
    });
}
