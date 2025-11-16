// functions/btfstorage/file/[id].js
// 🎬 Cloudflare Pages Functions - Advanced File Streaming Handler
// MINIMAL FIX - Original structure maintained

const MIME_TYPES = {
// Video formats
'mp4': 'video/mp4',
'mkv': 'video/x-matroska',
'avi': 'video/x-msvideo',
'mov': 'video/quicktime',
'm4v': 'video/mp4',
'wmv': 'video/x-ms-wmv',
'flv': 'video/x-flv',
'3gp': 'video/3gpp',
'webm': 'video/webm',
'ogv': 'video/ogg',

// Audio formats
'mp3': 'audio/mpeg',
'wav': 'audio/wav',
'aac': 'audio/mp4',
'm4a': 'audio/mp4',
'ogg': 'audio/ogg',
'flac': 'audio/flac',
'wma': 'audio/x-ms-wma',

// Image formats
'jpg': 'image/jpeg',
'jpeg': 'image/jpeg',
'png': 'image/png',
'gif': 'image/gif',
'webp': 'image/webp',
'svg': 'image/svg+xml',
'bmp': 'image/bmp',
'tiff': 'image/tiff',

// Document formats
'pdf': 'application/pdf',
'doc': 'application/msword',
'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
'txt': 'text/plain',
'zip': 'application/zip',
'rar': 'application/x-rar-compressed',

// Streaming formats
'm3u8': 'application/x-mpegURL',
'ts': 'video/mp2t',
'mpd': 'application/dash+xml'
};

export async function onRequest(context) {
const { request, env, params } = context;
const fileId = params.id;

console.log('🎬 TOP TIER STREAMING STARTED:', fileId);
console.log('📍 Request URL:', request.url);
console.log('🔗 Method:', request.method);
console.log('📊 User-Agent:', request.headers.get('User-Agent') || 'Unknown');

// Handle CORS preflight requests
if (request.method === 'OPTIONS') {
const corsHeaders = new Headers();
corsHeaders.set('Access-Control-Allow-Origin', '*');
corsHeaders.set('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
corsHeaders.set('Access-Control-Allow-Headers', 'Range, Content-Type, Authorization');
corsHeaders.set('Access-Control-Max-Age', '86400');
corsHeaders.set('Access-Control-Expose-Headers', 'Content-Length, Content-Range, Accept-Ranges');

console.log('✅ CORS preflight handled');
return new Response(null, { status: 204, headers: corsHeaders });
}

try {
// Parse file ID and extract components
let actualId = fileId;
let extension = '';
let isHlsPlaylist = false;
let isHlsSegment = false;
let segmentIndex = -1;

// Handle file extensions and special formats
if (fileId.includes('.')) {
const parts = fileId.split('.');
extension = parts.pop().toLowerCase();
actualId = parts.join('.');

// HLS Playlist (.m3u8)
if (extension === 'm3u8') {
isHlsPlaylist = true;
console.log('📼 HLS Playlist requested:', actualId);
}
// HLS Segment (.ts with index)
else if (extension === 'ts' && actualId.includes('-')) {
const segParts = actualId.split('-');
const lastPart = segParts[segParts.length - 1];

if (!isNaN(parseInt(lastPart))) {
segmentIndex = parseInt(segParts.pop(), 10);
actualId = segParts.join('-');
isHlsSegment = true;
console.log('📼 HLS Segment requested:', actualId, 'Index:', segmentIndex);
}
}
// Regular file with extension
else {
actualId = fileId.substring(0, fileId.lastIndexOf('.'));
extension = fileId.substring(fileId.lastIndexOf('.') + 1).toLowerCase();
console.log('📁 Regular file requested:', actualId, 'Extension:', extension);
}
}

// Fetch metadata from KV storage
console.log('🔍 Fetching metadata for:', actualId);
const metadataString = await env.FILES_KV.get(actualId);

if (!metadataString) {
console.error('❌ File not found in KV storage:', actualId);
return createErrorResponse('File not found', 404);
}

const metadata = JSON.parse(metadataString);

// Validate metadata structure
if (!metadata.filename || !metadata.size) {
console.error('❌ Invalid metadata structure:', metadata);
return createErrorResponse('Invalid file metadata', 400);
}

// Handle backward compatibility for field names
metadata.telegramFileId = metadata.telegramFileId || metadata.fileIdCode;

// Validate file source (either single file or chunks)
if (!metadata.telegramFileId && (!metadata.chunks || metadata.chunks.length === 0)) {
console.error('❌ No telegramFileId or chunks in metadata:', actualId);
return createErrorResponse('Missing file source data', 400);
}

// Determine MIME type
const mimeType = metadata.contentType || MIME_TYPES[extension] || 'application/octet-stream';

// Log file information
console.log(`📁 File Info:
📝 Name: ${metadata.filename}
📊 Size: ${Math.round(metadata.size/1024/1024)}MB (${metadata.size} bytes)
🏷️ MIME: ${mimeType}
🧩 Chunks: ${metadata.chunks?.length || 0}
📅 Uploaded: ${metadata.uploadedAt || 'N/A'}
🎵 HLS Playlist: ${isHlsPlaylist}
📼 HLS Segment: ${isHlsSegment} (Index: ${segmentIndex})
🔗 Has Telegram ID: ${!!metadata.telegramFileId}`);

// Route to appropriate handler based on request type
if (isHlsPlaylist) {
return await handleHlsPlaylist(request, env, metadata, actualId);
}

if (isHlsSegment && segmentIndex >= 0) {
return await handleHlsSegment(request, env, metadata, segmentIndex);
}

if (metadata.telegramFileId && (!metadata.chunks || metadata.chunks.length === 0)) {
return await handleSingleFile(request, env, metadata, mimeType);
}

if (metadata.chunks && metadata.chunks.length > 0) {
return await handleChunkedFile(request, env, metadata, mimeType, extension);
}

return createErrorResponse('Invalid file format or configuration', 400);

} catch (error) {
console.error('❌ Critical streaming error:', error);
console.error('🔍 Error stack:', error.stack);
return createErrorResponse(`Streaming error: ${error.message}`, 500);
}
}

/**
* Handle HLS playlist generation (.m3u8)
*/
async function handleHlsPlaylist(request, env, metadata, actualId) {
console.log('📼 Generating HLS playlist for:', actualId);

if (!metadata.chunks || metadata.chunks.length === 0) {
console.error('❌ HLS playlist requested for non-chunked file');
return createErrorResponse('HLS not supported for single files', 400);
}

const chunks = metadata.chunks;
const segmentDuration = 6;
const baseUrl = new URL(request.url).origin;

let playlist = '#EXTM3U
';
playlist += '#EXT-X-VERSION:3
';
playlist += `#EXT-X-TARGETDURATION:${segmentDuration}
`;
playlist += '#EXT-X-MEDIA-SEQUENCE:0
';
playlist += '#EXT-X-PLAYLIST-TYPE:VOD
';

for (let i = 0; i < chunks.length; i++) {
playlist += `#EXTINF:${segmentDuration.toFixed(1)},
`;
playlist += `${baseUrl}/btfstorage/file/${actualId}-${i}.ts
`;
}

playlist += '#EXT-X-ENDLIST
';

const headers = new Headers();
headers.set('Content-Type', 'application/x-mpegURL');
headers.set('Access-Control-Allow-Origin', '*');
headers.set('Cache-Control', 'no-cache, no-store, must-revalidate');
headers.set('Pragma', 'no-cache');
headers.set('Expires', '0');

console.log(`📼 HLS playlist generated: ${chunks.length} segments`);

return new Response(playlist, { status: 200, headers });
}

/**
* Handle HLS segment serving (.ts)
*/
async function handleHlsSegment(request, env, metadata, segmentIndex) {
console.log('📼 Serving HLS segment:', segmentIndex);

if (!metadata.chunks || segmentIndex >= metadata.chunks.length || segmentIndex < 0) {
console.error('❌ Invalid segment index:', segmentIndex);
return createErrorResponse('Segment not found', 404);
}

try {
const chunkInfo = metadata.chunks[segmentIndex];
const chunkData = await loadSingleChunk(env, chunkInfo);

const headers = new Headers();
headers.set('Content-Type', 'video/mp2t');
headers.set('Content-Length', chunkData.byteLength.toString());
headers.set('Access-Control-Allow-Origin', '*');
headers.set('Cache-Control', 'public, max-age=31536000, immutable');
headers.set('Content-Disposition', 'inline');
headers.set('Accept-Ranges', 'bytes');

console.log(`✅ HLS segment ${segmentIndex} served`);

return new Response(chunkData, { status: 200, headers });

} catch (error) {
console.error('❌ HLS segment error:', error);
return createErrorResponse(`Segment loading failed: ${error.message}`, 500);
}
}

/**
* Handle single file streaming
*/
async function handleSingleFile(request, env, metadata, mimeType) {
console.log('🚀 Single file streaming initiated');

const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);

if (botTokens.length === 0) {
console.error('❌ No bot tokens configured');
return createErrorResponse('Service configuration error', 503);
}

console.log(`🤖 Available bot tokens: ${botTokens.length}`);

for (let botIndex = 0; botIndex < botTokens.length; botIndex++) {
const botToken = botTokens[botIndex];
console.log(`🤖 Trying bot ${botIndex + 1}/${botTokens.length}`);

try {
const getFileResponse = await fetchWithRetry(
`https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(metadata.telegramFileId)}`,
{ signal: AbortSignal.timeout(15000) }
);

const getFileData = await getFileResponse.json();

if (!getFileData.ok || !getFileData.result?.file_path) {
console.error(`🤖 Bot ${botIndex + 1} failed:`, getFileData.description);
continue;
}

const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;
console.log('📡 Telegram direct URL obtained');

const requestHeaders = {};
const rangeHeader = request.headers.get('Range');

if (rangeHeader) {
requestHeaders['Range'] = rangeHeader;
console.log('🎯 Range request:', rangeHeader);
}

const telegramResponse = await fetchWithRetry(directUrl, {
headers: requestHeaders,
signal: AbortSignal.timeout(45000)
});

if (!telegramResponse.ok) {
console.error(`📡 Telegram fetch failed: ${telegramResponse.status}`);
continue;
}

const responseHeaders = new Headers();

['content-length', 'content-range', 'accept-ranges'].forEach(header => {
const value = telegramResponse.headers.get(header);
if (value) {
responseHeaders.set(header, value);
}
});

responseHeaders.set('Content-Type', mimeType);
responseHeaders.set('Accept-Ranges', 'bytes');
responseHeaders.set('Access-Control-Allow-Origin', '*');
responseHeaders.set('Cache-Control', 'public, max-age=31536000');

const url = new URL(request.url);
if (url.searchParams.has('dl') || url.searchParams.has('download')) {
responseHeaders.set('Content-Disposition', `attachment; filename="${metadata.filename}"`);
} else {
responseHeaders.set('Content-Disposition', 'inline');
}

console.log(`✅ Single file streaming successful with bot ${botIndex + 1}`);

return new Response(telegramResponse.body, {
status: telegramResponse.status,
headers: responseHeaders
});

} catch (botError) {
console.error(`❌ Bot ${botIndex + 1} error:`, botError.message);
continue;
}
}

console.error('❌ All bot tokens failed');
return createErrorResponse('All streaming servers failed', 503);
}

/**
* Handle chunked file streaming - FIXED VERSION
*/
async function handleChunkedFile(request, env, metadata, mimeType, extension) {
const chunks = metadata.chunks;
const totalSize = metadata.size;
const chunkSize = metadata.chunkSize || 20971520;

const rangeHeader = request.headers.get('Range');
const url = new URL(request.url);
const isDownload = url.searchParams.has('dl') || url.searchParams.has('download');

console.log(`🎬 Chunked file streaming:
🧩 Total chunks: ${chunks.length}
📊 Total size: ${Math.round(totalSize/1024/1024)}MB
📦 Chunk size: ${Math.round(chunkSize/1024/1024)}MB
🎯 Range request: ${rangeHeader || 'None'}
📥 Download mode: ${isDownload}`);

if (rangeHeader) {
return await handleSmartRange(request, env, metadata, rangeHeader, mimeType, chunkSize, isDownload);
}

if (isDownload) {
return await handleFullStreamDownload(request, env, metadata, mimeType);
}

return await handleInstantPlay(request, env, metadata, mimeType, totalSize);
}

/**
* Handle instant play streaming - FIXED
*/
async function handleInstantPlay(request, env, metadata, mimeType, totalSize) {
const chunks = metadata.chunks;
console.log('⚡ INSTANT PLAY MODE');

try {
const maxInitialChunks = Math.min(3, chunks.length);
let chunkIndex = 0;
let streamedBytes = 0;

// FIX: Use TransformStream instead of ReadableStream
const { readable, writable } = new TransformStream();
const writer = writable.getWriter();

// Stream chunks in background
(async () => {
try {
while (chunkIndex < maxInitialChunks) {
console.log(`⚡ Loading chunk ${chunkIndex + 1}/${maxInitialChunks}`);
const chunkData = await loadSingleChunk(env, chunks[chunkIndex]);
const uint8Array = new Uint8Array(chunkData);

await writer.write(uint8Array);
streamedBytes += uint8Array.byteLength;

console.log(`⚡ Sent ${Math.round(uint8Array.byteLength/1024/1024)}MB`);
chunkIndex++;
}

console.log('⚡ Initial play complete');
await writer.close();

} catch (error) {
console.error('❌ Instant play error:', error);
await writer.abort(error).catch(() => {});
}
})();

const headers = new Headers();
headers.set('Content-Type', mimeType);
headers.set('Accept-Ranges', 'bytes');
headers.set('Access-Control-Allow-Origin', '*');
headers.set('Content-Disposition', 'inline');
headers.set('Cache-Control', 'public, max-age=31536000');
headers.set('X-Streaming-Mode', 'instant-play');

return new Response(readable, { status: 200, headers });

} catch (error) {
console.error('❌ Instant play failed:', error);
return createErrorResponse(`Instant play failed: ${error.message}`, 500);
}
}

/**
* Handle smart range requests - FIXED
*/
async function handleSmartRange(request, env, metadata, rangeHeader, mimeType, chunkSize, isDownload = false) {
const totalSize = metadata.size;
const chunks = metadata.chunks;

console.log('🎯 SMART RANGE REQUEST:', rangeHeader);

const rangeMatch = rangeHeader.match(/bytes=(d+)-(d*)/);
if (!rangeMatch) {
return createErrorResponse('Invalid range format', 416, {
'Content-Range': `bytes */${totalSize}`
});
}

const start = parseInt(rangeMatch[1], 10);
let end = rangeMatch[2] ? parseInt(rangeMatch[2], 10) : totalSize - 1;

if (end >= totalSize) end = totalSize - 1;
if (start >= totalSize || start > end) {
return createErrorResponse('Range not satisfiable', 416, {
'Content-Range': `bytes */${totalSize}`
});
}

const requestedSize = end - start + 1;
console.log(`🎯 Range: ${start}-${end} = ${Math.round(requestedSize/1024/1024)}MB`);

const startChunk = Math.floor(start / chunkSize);
const endChunk = Math.floor(end / chunkSize);

console.log(`🧩 Chunks needed: ${startChunk} to ${endChunk}`);

let currentChunkIndex = startChunk;
let currentPosition = startChunk * chunkSize;

// FIX: Use TransformStream
const { readable, writable } = new TransformStream();
const writer = writable.getWriter();

(async () => {
try {
while (currentChunkIndex <= endChunk) {
console.log(`🎯 Chunk ${currentChunkIndex + 1}/${chunks.length}`);

const chunkData = await loadSingleChunk(env, chunks[currentChunkIndex]);
const uint8Array = new Uint8Array(chunkData);

const chunkStart = Math.max(start - currentPosition, 0);
const chunkEnd = Math.min(uint8Array.length, end - currentPosition + 1);

if (chunkStart < chunkEnd) {
const slice = uint8Array.slice(chunkStart, chunkEnd);
await writer.write(slice);
console.log(`✅ Sent ${slice.length} bytes`);
}

currentPosition += chunkSize;
currentChunkIndex++;

if (currentPosition > end) break;
}

console.log('🎯 Range complete');
await writer.close();

} catch (error) {
console.error('❌ Range error:', error);
await writer.abort(error).catch(() => {});
}
})();

const headers = new Headers();
headers.set('Content-Type', mimeType);
headers.set('Content-Length', requestedSize.toString());
headers.set('Content-Range', `bytes ${start}-${end}/${totalSize}`);
headers.set('Accept-Ranges', 'bytes');
headers.set('Access-Control-Allow-Origin', '*');
headers.set('Content-Disposition', isDownload ? `attachment; filename="${metadata.filename}"` : 'inline');
headers.set('Cache-Control', 'public, max-age=31536000');

return new Response(readable, { status: 206, headers });
}

/**
* Handle full stream download - FIXED
*/
async function handleFullStreamDownload(request, env, metadata, mimeType) {
const chunks = metadata.chunks;
const totalSize = metadata.size;

console.log('📥 FULL DOWNLOAD MODE');

let chunkIndex = 0;

// FIX: Use TransformStream
const { readable, writable } = new TransformStream();
const writer = writable.getWriter();

(async () => {
try {
while (chunkIndex < chunks.length) {
console.log(`📥 Chunk ${chunkIndex + 1}/${chunks.length}`);

const chunkData = await loadSingleChunk(env, chunks[chunkIndex]);
const uint8Array = new Uint8Array(chunkData);

await writer.write(uint8Array);
console.log(`✅ Sent ${Math.round(uint8Array.byteLength/1024/1024)}MB`);

chunkIndex++;
}

console.log('📥 Download complete');
await writer.close();

} catch (error) {
console.error('❌ Download error:', error);
await writer.abort(error).catch(() => {});
}
})();

const headers = new Headers();
headers.set('Content-Type', mimeType);
headers.set('Content-Length', totalSize.toString());
headers.set('Content-Disposition', `attachment; filename="${metadata.filename}"`);
headers.set('Accept-Ranges', 'bytes');
headers.set('Access-Control-Allow-Origin', '*');
headers.set('Cache-Control', 'public, max-age=31536000');

return new Response(readable, { status: 200, headers });
}

/**
* Load a single chunk from storage
*/
async function loadSingleChunk(env, chunkInfo) {
const kvNamespace = env[chunkInfo.kvNamespace] || env.FILES_KV;
const chunkKey = chunkInfo.keyName || chunkInfo.chunkKey;

console.log(`📥 Loading chunk: ${chunkKey}`);

const metadataString = await kvNamespace.get(chunkKey);
if (!metadataString) {
throw new Error(`Chunk metadata not found: ${chunkKey}`);
}

const chunkMetadata = JSON.parse(metadataString);
chunkMetadata.telegramFileId = chunkMetadata.telegramFileId || chunkMetadata.fileIdCode;

// Try existing direct URL first
if (chunkMetadata.directUrl) {
try {
const response = await fetchWithRetry(chunkMetadata.directUrl, {
signal: AbortSignal.timeout(30000)
});

if (response.ok) {
console.log(`✅ Chunk loaded from cached URL`);
return response.arrayBuffer();
}

console.log(`🔄 Cached URL expired, refreshing...`);
} catch (error) {
console.log(`🔄 Cached URL failed:`, error.message);
}
}

// Refresh URL
console.log(`🔄 Refreshing URL for chunk: ${chunkKey}`);

const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);

for (let botIndex = 0; botIndex < botTokens.length; botIndex++) {
const botToken = botTokens[botIndex];

try {
const getFileResponse = await fetchWithRetry(
`https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(chunkMetadata.telegramFileId)}`,
{ signal: AbortSignal.timeout(15000) }
);

const getFileData = await getFileResponse.json();

if (!getFileData.ok || !getFileData.result?.file_path) {
continue;
}

const freshUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

const response = await fetchWithRetry(freshUrl, {
signal: AbortSignal.timeout(30000)
});

if (response.ok) {
// Update KV store with fresh URL
kvNamespace.put(chunkKey, JSON.stringify({
...chunkMetadata,
directUrl: freshUrl,
lastRefreshed: Date.now()
})).catch(() => {});

console.log(`✅ URL refreshed with bot ${botIndex + 1}`);
return response.arrayBuffer();
}

} catch (botError) {
console.error(`❌ Bot ${botIndex + 1} failed:`, botError.message);
continue;
}
}

throw new Error(`All refresh attempts failed for chunk: ${chunkKey}`);
}

/**
* Fetch with retry logic
*/
async function fetchWithRetry(url, options = {}, retries = 3) {
for (let attempt = 0; attempt < retries; attempt++) {
try {
const response = await fetch(url, options);

if (response.ok) {
return response;
}

if (response.status === 429) {
const retryAfter = parseInt(response.headers.get('Retry-After')) || 5;
console.warn(`⏳ Rate limited, waiting ${retryAfter}s`);
await new Promise(resolve => setTimeout(resolve, retryAfter * 1000));
continue;
}

if (response.status >= 500) {
if (attempt < retries - 1) {
await new Promise(resolve => setTimeout(resolve, Math.pow(2, attempt) * 1000));
continue;
}
}

if (response.status >= 400 && response.status < 500) {
return response;
}

} catch (error) {
if (attempt === retries - 1) {
throw error;
}
}

if (attempt < retries - 1) {
const delay = Math.min(Math.pow(2, attempt) * 1000, 5000);
await new Promise(resolve => setTimeout(resolve, delay));
}
}

throw new Error(`All ${retries} fetch attempts failed`);
}

/**
* Create standardized error response
*/
function createErrorResponse(message, status = 500, additionalHeaders = {}) {
const headers = new Headers({
'Content-Type': 'application/json',
'Access-Control-Allow-Origin', '*',
...additionalHeaders
});

const errorResponse = {
error: message,
status: status,
timestamp: new Date().toISOString(),
service: 'BTF Storage Streaming'
};

console.error(`❌ Error response: ${status} - ${message}`);

return new Response(JSON.stringify(errorResponse, null, 2), {
status: status,
headers: headers
});
}