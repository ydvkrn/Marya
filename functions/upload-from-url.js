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
        return new Response(JSON.stringify({
            success: false,
            error: 'Method not allowed'
        }), {
            status: 405,
            headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
    }

    try {
        const kvNamespaces = [
            { kv: env.FILES_KV, name: 'FILES_KV' },
            { kv: env.FILES_KV2, name: 'FILES_KV2' },
            { kv: env.FILES_KV3, name: 'FILES_KV3' },
            { kv: env.FILES_KV4, name: 'FILES_KV4' },
            { kv: env.FILES_KV5, name: 'FILES_KV5' },
            { kv: env.FILES_KV6, name: 'FILES_KV6' },
            { kv: env.FILES_KV7, name: 'FILES_KV7' }
        ].filter(item => item.kv);

        if (kvNamespaces.length === 0) {
            throw new Error('No KV namespaces available');
        }

        const body = await request.json();
        const { fileUrl, filename } = body;

        if (!fileUrl) {
            throw new Error('No file URL provided');
        }

        // Validate URL
        try {
            new URL(fileUrl);
        } catch {
            throw new Error('Invalid URL format');
        }

        // Fetch file metadata
        let fileSize = 0;
        let contentType = 'application/octet-stream';
        let originalFilename = filename || extractFilenameFromUrl(fileUrl);

        // Try HEAD request for size and content-type
        const headResponse = await fetch(fileUrl, { 
            method: 'HEAD',
            headers: { 'User-Agent': 'Mozilla/5.0' }
        });

        if (headResponse.ok) {
            fileSize = parseInt(headResponse.headers.get('content-length') || '0');
            contentType = headResponse.headers.get('content-type') || contentType;
        }

        // Fallback: Download a small portion to estimate size if content-length is missing
        if (fileSize === 0) {
            const rangeResponse = await fetch(fileUrl, { 
                headers: { 
                    'Range': 'bytes=0-1023',
                    'User-Agent': 'Mozilla/5.0'
                }
            });
            if (!rangeResponse.ok) {
                throw new Error(`Failed to access file: ${rangeResponse.status}`);
            }
            const contentRange = rangeResponse.headers.get('content-range');
            if (contentRange) {
                const match = contentRange.match(/\/(\d+)/);
                if (match) fileSize = parseInt(match[1]);
            }
        }

        // Size validation - 175MB max
        const MAX_FILE_SIZE = 175 * 1024 * 1024;
        if (fileSize > MAX_FILE_SIZE || fileSize === 0) {
            throw new Error(fileSize > MAX_FILE_SIZE 
                ? `File too large: ${Math.round(fileSize / 1024 / 1024)}MB (max 175MB)`
                : 'Unable to determine file size');
        }

        // Generate unique file ID
        const timestamp = Date.now().toString(36);
        const random = Math.random().toString(36).slice(2, 8);
        const newFileId = `url${timestamp}${random}`;
        const extension = originalFilename.includes('.') ? originalFilename.slice(originalFilename.lastIndexOf('.')) : '';

        // Download file
        const fileResponse = await fetch(fileUrl, {
            headers: { 'User-Agent': 'Mozilla/5.0' }
        });
        if (!fileResponse.ok) {
            throw new Error(`Failed to download file: ${fileResponse.status}`);
        }

        const fileBlob = await fileResponse.blob();
        const fileBuffer = await fileBlob.arrayBuffer();
        if (fileBuffer.byteLength !== fileSize) {
            throw new Error('Downloaded file size does not match expected size');
        }

        // Chunking strategy
        const CHUNK_SIZE = 20 * 1024 * 1024;
        const totalChunks = Math.ceil(fileBuffer.byteLength / CHUNK_SIZE);

        if (totalChunks > kvNamespaces.length) {
            throw new Error(`File requires ${totalChunks} chunks, but only ${kvNamespaces.length} KV namespaces available`);
        }

        // Store chunks in KV
        const chunkPromises = [];
        for (let i = 0; i < totalChunks; i++) {
            const start = i * CHUNK_SIZE;
            const end = Math.min(start + CHUNK_SIZE, fileBuffer.byteLength);
            const chunkBuffer = fileBuffer.slice(start, end);
            const chunkBlob = new Blob([chunkBuffer]);
            const chunkKey = `${newFileId}_chunk_${i}`;
            const targetKV = kvNamespaces[i % kvNamespaces.length];

            const chunkPromise = targetKV.kv.put(chunkKey, chunkBlob, {
                metadata: {
                    index: i,
                    parentFileId: newFileId,
                    size: chunkBlob.size,
                    kvNamespace: targetKV.name,
                    uploadedAt: Date.now()
                }
            }).then(() => ({
                chunkKey,
                size: chunkBlob.size,
                kvNamespace: targetKV.name
            }));

            chunkPromises.push(chunkPromise);
        }

        const chunkResults = await Promise.all(chunkPromises);

        // Store master metadata in primary KV
        const masterMetadata = {
            filename: originalFilename,
            size: fileBuffer.byteLength,
            contentType: contentType,
            extension: extension,
            uploadedAt: Date.now(),
            type: 'url_import_multi_kv',
            sourceUrl: fileUrl,
            totalChunks: totalChunks,
            chunks: chunkResults.map((result, index) => ({
                index: index,
                kvNamespace: result.kvNamespace,
                chunkKey: result.chunkKey,
                size: result.size
            }))
        };

        await kvNamespaces[0].kv.put(newFileId, JSON.stringify(masterMetadata));

        const baseUrl = new URL(request.url).origin;
        const customUrl = `${baseUrl}/btfstorage/file/${newFileId}${extension}`;
        const downloadUrl = `${baseUrl}/btfstorage/file/${newFileId}${extension}?dl=1`;

        const result = {
            success: true,
            data: {
                filename: originalFilename,
                size: fileBuffer.byteLength,
                contentType: contentType,
                url: customUrl,
                download: downloadUrl,
                id: newFileId,
                strategy: 'url_import_multi_kv',
                chunks: totalChunks,
                kvDistribution: chunkResults.map(r => r.kvNamespace)
            }
        };

        return new Response(JSON.stringify(result), {
            headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });

    } catch (error) {
        return new Response(JSON.stringify({
            success: false,
            error: error.message
        }), {
            status: 500,
            headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
    }
}

function extractFilenameFromUrl(url) {
    try {
        const urlPath = new URL(url).pathname;
        const parts = urlPath.split('/');
        return parts[parts.length - 1] || 'downloaded_file';
    } catch {
        return 'downloaded_file';
    }
}