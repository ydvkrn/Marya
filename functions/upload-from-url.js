import { nanoid } from 'nanoid';

const CHUNK_SIZE = 20 * 1024 * 1024; // 20MB chunks
const MAX_SIZE = 175 * 1024 * 1024; // 175MB max file size
const KV_NAMESPACES = [
  'FILES_KV', 'FILES_KV2', 'FILES_KV3', 'FILES_KV4',
  'FILES_KV5', 'FILES_KV6', 'FILES_KV7'
];

export default {
  async fetch(request, env, ctx) {
    if (request.method !== 'POST') {
      return new Response(JSON.stringify({ success: false, error: 'Method not allowed' }), {
        status: 405,
        headers: { 'Content-Type': 'application/json' }
      });
    }

    try {
      const { fileUrl, filename } = await request.json();
      if (!fileUrl) {
        return new Response(JSON.stringify({ success: false, error: 'URL is required' }), {
          status: 400,
          headers: { 'Content-Type': 'application/json' }
        });
      }

      console.log(`Fetching URL: ${fileUrl}`);
      const response = await fetch(fileUrl, { timeout: 30000 });
      if (!response.ok) {
        console.error(`Fetch failed: HTTP ${response.status} - ${response.statusText}`);
        return new Response(JSON.stringify({ success: false, error: `Failed to fetch file: HTTP ${response.status}` }), {
          status: 400,
          headers: { 'Content-Type': 'application/json' }
        });
      }

      const contentLength = parseInt(response.headers.get('content-length') || '0', 10);
      if (contentLength === 0) {
        console.error('File is empty (Content-Length: 0)');
        return new Response(JSON.stringify({ success: false, error: 'File is empty' }), {
          status: 400,
          headers: { 'Content-Type': 'application/json' }
        });
      }
      if (contentLength > MAX_SIZE) {
        console.error(`File too large: ${contentLength} bytes`);
        return new Response(JSON.stringify({ success: false, error: 'File exceeds 175MB limit' }), {
          status: 400,
          headers: { 'Content-Type': 'application/json' }
        });
      }

      const contentType = response.headers.get('content-type') || 'application/octet-stream';
      const urlParts = new URL(fileUrl).pathname.split('/');
      const originalFilename = filename || urlParts[urlParts.length - 1] || 'downloaded_file';
      const fileId = nanoid(14);
      const fileExtension = originalFilename.split('.').pop()?.toLowerCase() || '';
      const storedFilename = fileExtension ? `${fileId}.${fileExtension}` : fileId;

      console.log(`Processing file: ${storedFilename}, size: ${contentLength} bytes`);

      const buffer = await response.arrayBuffer();
      if (buffer.byteLength === 0) {
        console.error('Downloaded file is empty');
        return new Response(JSON.stringify({ success: false, error: 'Downloaded file is empty' }), {
          status: 400,
          headers: { 'Content-Type': 'application/json' }
        });
      }

      const chunks = [];
      for (let i = 0; i < buffer.byteLength; i += CHUNK_SIZE) {
        const chunk = buffer.slice(i, i + CHUNK_SIZE);
        const chunkId = `${fileId}_chunk_${chunks.length}`;
        const kvNamespace = env[KV_NAMESPACES[chunks.length % KV_NAMESPACES.length]];
        if (!kvNamespace) {
          console.error(`KV namespace ${KV_NAMESPACES[chunks.length % KV_NAMESPACES.length]} not found`);
          return new Response(JSON.stringify({ success: false, error: 'Internal server error: KV namespace missing' }), {
            status: 500,
            headers: { 'Content-Type': 'application/json' }
          });
        }
        await kvNamespace.put(chunkId, chunk);
        chunks.push(chunkId);
        console.log(`Stored chunk ${chunkId} in ${KV_NAMESPACES[chunks.length % KV_NAMESPACES.length]}`);
      }

      const metadata = {
        filename: storedFilename,
        size: buffer.byteLength,
        contentType,
        chunks,
        createdAt: new Date().toISOString()
      };

      await env.FILES_KV.put(fileId, JSON.stringify(metadata));
      console.log(`Stored metadata for ${fileId}`);

      const fileUrlResponse = `https://${request.headers.get('host')}/btfstorage/file/${storedFilename}`;
      const downloadUrl = `https://${request.headers.get('host')}/btfstorage/download/${storedFilename}`;

      return new Response(JSON.stringify({
        success: true,
        data: {
          filename: storedFilename,
          size: buffer.byteLength,
          chunks: chunks.length,
          url: fileUrlResponse,
          download: downloadUrl
        }
      }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' }
      });

    } catch (error) {
      console.error('Error in upload-from-url:', error.message, error.stack);
      return new Response(JSON.stringify({ success: false, error: `Internal server error: ${error.message}` }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' }
      });
    }
  }
};