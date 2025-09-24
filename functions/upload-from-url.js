// functions/upload-from-url.js
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

    try {
        const { url } = await request.json();

        if (!url || !url.trim()) {
            throw new Error('No URL provided');
        }

        const cleanUrl = url.trim();

        if (!cleanUrl.startsWith('http://') && !cleanUrl.startsWith('https://')) {
            throw new Error('URL must start with http:// or https://');
        }

        console.log('ðŸŒ Processing URL:', cleanUrl);

        // Download file from URL
        const response = await fetch(cleanUrl, {
            headers: {
                'User-Agent': 'MARYA-VAULT-ULTIMATE/1.0'
            }
        });

        if (!response.ok) {
            throw new Error(`Failed to fetch file: ${response.status} ${response.statusText}`);
        }

        // Get file info
        const contentLength = response.headers.get('Content-Length');
        const contentType = response.headers.get('Content-Type') || 'application/octet-stream';

        let filename = 'download';
        const disposition = response.headers.get('Content-Disposition');
        if (disposition && disposition.includes('filename')) {
            const match = disposition.match(/filename[*]?=([^;]+)/);
            if (match) {
                filename = match[1].replace(/['"]/g, '').trim();
            }
        } else {
            const urlPath = new URL(cleanUrl).pathname;
            const urlFilename = urlPath.split('/').pop();
            if (urlFilename && urlFilename.includes('.')) {
                filename = decodeURIComponent(urlFilename);
            }
        }

        const arrayBuffer = await response.arrayBuffer();
        const file = new File([arrayBuffer], filename, { type: contentType });

        // Use existing upload logic
        const formData = new FormData();
        formData.append('file', file);

        const uploadResponse = await fetch(new URL('/upload', request.url), {
            method: 'POST',
            body: formData,
            headers: {
                'X-Forwarded-Proto': 'https',
                'X-Forwarded-Host': new URL(request.url).hostname
            }
        });

        const uploadResult = await uploadResponse.json();

        if (uploadResult.success) {
            return new Response(JSON.stringify({
                success: true,
                url: uploadResult.url,
                download: uploadResult.download,
                filename: filename,
                size: arrayBuffer.byteLength,
                contentType: contentType,
                source: 'url_upload'
            }), {
                headers: { 'Content-Type': 'application/json', ...corsHeaders }
            });
        } else {
            throw new Error(uploadResult.error || 'Upload failed');
        }

    } catch (error) {
        console.error('URL upload error:', error);
        return new Response(JSON.stringify({
            success: false,
            error: error.message
        }), {
            status: 500,
            headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
    }
}