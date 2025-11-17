// functions/btfstorage/file/[id].js
// 🎬 Cloudflare Pages Functions - Advanced File Streaming Handler
// URL: marya-hosting.pages.dev/btfstorage/file/MSM221-48U91C62-no.mp4

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

console.log('🚀 Advanced Streaming Started:', fileId);
console.log('📍 Request URL:', request.url);
console.log('🔗 Method:', request.method);
console.log('📊 User-Agent:', request.headers.get('User-Agent') || 'Unknown');

if (request.method === 'OPTIONS') {
const corsHeaders = new Headers();
corsHeaders.set('Access-Control-Allow-Origin', '*');
corsHeaders.set('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
corsHeaders.set('Access-Control-Allow-Headers', 'Range, Content-Type, Authorization');
corsHeaders.set('Access-Control-Max-Age', '86400');
corsHeaders.set('Access-Control-Expose-Headers', 'Content-Length, Content-Range, Accept-Ranges');
return new Response(null, { status: 204, headers: corsHeaders });
}

try {
let actualId = fileId;
let extension = '';

if (fileId.includes('.')) {
actualId = fileId.substring(0, fileId.lastIndexOf('.'));
extension = fileId.substring(fileId.lastIndexOf('.') + 1).toLowerCase();
console.log('📄 File extension detected:', extension);
}

console.log('🔍 Fetching metadata for ID:', actualId);
const metadataString = await env.FILES_KV.get(actualId);

if (!metadataString) {
console.error('❌ File not found in KV storage:', actualId);
return createErrorResponse('File not found', 404);
}

const metadata = JSON.parse(metadataString);
console.log('📦 Metadata retrieved:', {
filename: metadata.filename,
size: metadata.size,
chunks: metadata.chunks?.length || 0
});

if (!metadata.filename || !metadata.size) {
console.error('❌ Invalid metadata structure:', metadata);
return createErrorResponse('Invalid file metadata', 400);
}

metadata.telegramFileId = metadata.telegramFileId || metadata.fileIdCode;

if (!metadata.telegramFileId && (!metadata.chunks || metadata.chunks.length === 0)) {
console.error('❌ No telegramFileId or chunks in metadata:', actualId);
return createErrorResponse('Missing file source data', 400);
}

const mimeType = metadata.contentType || MIME_TYPES[extension] || 'application/octet-stream';
console.log('🏷️ MIME Type:', mimeType);

if (metadata.telegramFileId && (!metadata.chunks || metadata.chunks.length === 0)) {
console.log('📡 Single file streaming mode');
return await handleSingleFile(request, env, metadata, mimeType);
}

if (metadata.chunks && metadata.chunks.length > 0) {
console.log('🎬 Chunked file streaming mode');
return await handleChunkedFile(request, env, metadata, mimeType, extension);
}

return createErrorResponse('Invalid file format or configuration', 400);

} catch (error) {
console.error('❌ Critical streaming error:', error);
console.error('📍 Error stack:', error.stack);
return createErrorResponse('Streaming error: ' + error.message, 500);
}
}

async function handleSingleFile(request, env, metadata, mimeType) {
console.log('📡 Single file handler started');

const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);

if (botTokens.length === 0) {
console.error('❌ No bot tokens configured');
return createErrorResponse('Service configuration error', 503);
}

console.log('🤖 Available bot tokens:', botTokens.length);

for (let botIndex = 0; botIndex < botTokens.length; botIndex++) {
const botToken = botTokens[botIndex];
console.log('🤖 Trying bot ' + (botIndex + 1) + '/' + botTokens.length);

try {
const getFileResponse = await fetchWithRetry(
'https://api.telegram.org/bot' + botToken + '/getFile?file_id=' + encodeURIComponent(metadata.telegramFileId),
{ signal: AbortSignal.timeout(15000) }
);

const getFileData = await getFileResponse.json();

if (!getFileData.ok || !getFileData.result?.file_path) {
console.error('🤖 Bot ' + (botIndex + 1) + ' API error:', getFileData.error_code, '-', getFileData.description);
continue;
}

const directUrl = 'https://api.telegram.org/file/bot' + botToken + '/' + getFileData.result.file_path;
console.log('📡 Telegram direct URL obtained');

const requestHeaders = {};
const rangeHeader = request.headers.get('Range');

if (rangeHeader) {
requestHeaders['Range'] = rangeHeader;
console.log('🎯 Range request detected:', rangeHeader);
}

const telegramResponse = await fetchWithRetry(directUrl, {
headers: requestHeaders,
signal: AbortSignal.timeout(45000)
});

if (!telegramResponse.ok) {
console.error('📡 Telegram file fetch failed:', telegramResponse.status, telegramResponse.statusText);
continue;
}

const responseHeaders = new Headers();

['content-length', 'content-range', 'accept-ranges'].forEach(function(header) {
const value = telegramResponse.headers.get(header);
if (value) {
responseHeaders.set(header, value);
console.log('📤 Header copied:', header, '=', value);
}
});

responseHeaders.set('Content-Type', mimeType);
responseHeaders.set('Accept-Ranges', 'bytes');
responseHeaders.set('Access-Control-Allow-Origin', '*');
responseHeaders.set('Cache-Control', 'public, max-age=31536000');

const url = new URL(request.url);
if (url.searchParams.has('dl') || url.searchParams.has('download')) {
responseHeaders.set('Content-Disposition', 'attachment; filename="' + metadata.filename + '"');
console.log('📥 Download mode enabled');
} else {
responseHeaders.set('Content-Disposition', 'inline');
console.log('👁️ Inline display mode');
}

console.log('✅ Single file streaming successful via bot ' + (botIndex + 1));

return new Response(telegramResponse.body, {
status: telegramResponse.status,
headers: responseHeaders
});

} catch (botError) {
console.error('❌ Bot ' + (botIndex + 1) + ' failed:', botError.message);
continue;
}
}

console.error('❌ All bot tokens failed');
return createErrorResponse('All streaming servers failed', 503);
}

async function handleChunkedFile(request, env, metadata, mimeType, extension) {
const chunks = metadata.chunks;
const totalSize = metadata.size;
const chunkSize = metadata.chunkSize || 20971520;

const rangeHeader = request.headers.get('Range');
const url = new URL(request.url);
const isDownload = url.searchParams.has('dl') || url.searchParams.has('download');

console.log('🎬 Chunked file streaming:', {
totalChunks: chunks.length,
totalSize: totalSize,
chunkSize: chunkSize,
hasRange: !!rangeHeader,
isDownload: isDownload
});

if (rangeHeader) {
return await handleSmartRange(request, env, metadata, rangeHeader, mimeType, chunkSize, isDownload);
}

if (isDownload) {
return await handleFullStreamDownload(request, env, metadata, mimeType);
}

return await handleInstantPlay(request, env, metadata, mimeType, totalSize);
}

async function handleInstantPlay(request, env, metadata, mimeType, totalSize) {
const chunks = metadata.chunks;
console.log('⚡ Instant play handler started');

try {
const maxInitialBytes = 50 * 1024 * 1024;
const maxInitialChunks = Math.min(3, chunks.length);

console.log('⚡ Loading initial ' + maxInitialChunks + ' chunks for instant playback');

let loadedBytes = 0;
let chunkIndex = 0;

const stream = new ReadableStream({
async pull(controller) {
while (chunkIndex < maxInitialChunks && loadedBytes < maxInitialBytes) {
try {
console.log('⚡ Loading chunk ' + (chunkIndex + 1) + '/' + maxInitialChunks);
const chunkData = await loadSingleChunk(env, chunks[chunkIndex]);
const uint8Array = new Uint8Array(chunkData);

controller.enqueue(uint8Array);
loadedBytes += uint8Array.byteLength;
console.log('⚡ Chunk ' + (chunkIndex + 1) + ' loaded: ' + Math.round(uint8Array.byteLength / 1024 / 1024) + 'MB');
chunkIndex++;

} catch (error) {
console.error('❌ Initial chunk ' + (chunkIndex + 1) + ' failed:', error);
controller.error(error);
return;
}
}

console.log('⚡ Instant play stream completed: ' + Math.round(loadedBytes / 1024 / 1024) + 'MB loaded');
controller.close();
},

cancel(reason) {
console.log('⚡ Instant play stream cancelled:', reason);
}
});

const headers = new Headers();
headers.set('Content-Type', mimeType);
headers.set('Content-Length', Math.min(loadedBytes || maxInitialBytes, totalSize).toString());
headers.set('Content-Range', 'bytes 0-' + (Math.min(loadedBytes || maxInitialBytes, totalSize) - 1) + '/' + totalSize);
headers.set('Accept-Ranges', 'bytes');
headers.set('Access-Control-Allow-Origin', '*');
headers.set('Content-Disposition', 'inline');
headers.set('Cache-Control', 'public, max-age=31536000');

console.log('✅ Instant play response ready');
return new Response(stream, { status: 206, headers });

} catch (error) {
console.error('❌ Instant play error:', error);
return createErrorResponse('Instant play failed: ' + error.message, 500);
}
}

async function handleSmartRange(request, env, metadata, rangeHeader, mimeType, chunkSize, isDownload) {
const totalSize = metadata.size;
const chunks = metadata.chunks;

console.log('🎯 Smart range request handler started');
console.log('🎯 Range header:', rangeHeader);

const rangeMatch = rangeHeader.match(/bytes=(d+)-(d*)/);
if (!rangeMatch) {
console.error('❌ Invalid range format:', rangeHeader);
return createErrorResponse('Invalid range format', 416, {
'Content-Range': 'bytes */' + totalSize
});
}

const start = parseInt(rangeMatch[1], 10);
let end = rangeMatch[2] ? parseInt(rangeMatch[2], 10) : totalSize - 1;

if (end >= totalSize) end = totalSize - 1;
if (start >= totalSize || start > end) {
console.error('❌ Range not satisfiable:', start + '-' + end + '/' + totalSize);
return createErrorResponse('Range not satisfiable', 416, {
'Content-Range': 'bytes */' + totalSize
});
}

const requestedSize = end - start + 1;

console.log('🎯 Range details:', {
start: start,
end: end,
requestedSize: requestedSize,
totalSize: totalSize
});

const startChunk = Math.floor(start / chunkSize);
const endChunk = Math.floor(end / chunkSize);
const neededChunks = chunks.slice(startChunk, endChunk + 1);

console.log('🎯 Chunks needed:', neededChunks.length, 'from chunk', startChunk, 'to', endChunk);

let currentPosition = startChunk * chunkSize;

const stream = new ReadableStream({
async pull(controller) {
for (let i = 0; i < neededChunks.length; i++) {
const chunkInfo = neededChunks[i];

try {
console.log('🎯 Loading range chunk ' + (i + 1) + '/' + neededChunks.length);
const chunkData = await loadSingleChunk(env, chunkInfo);
const uint8Array = new Uint8Array(chunkData);

const chunkStart = Math.max(start - currentPosition, 0);
const chunkEnd = Math.min(uint8Array.length, end - currentPosition + 1);

if (chunkStart < chunkEnd) {
const chunkSlice = uint8Array.slice(chunkStart, chunkEnd);
controller.enqueue(chunkSlice);
console.log('🎯 Range chunk ' + (i + 1) + ' processed: ' + chunkSlice.length + ' bytes');
}

currentPosition += chunkSize;
if (currentPosition > end) break;

} catch (error) {
console.error('❌ Range chunk ' + (i + 1) + ' failed:', error);
controller.error(error);
return;
}
}

console.log('✅ Range stream completed');
controller.close();
},

cancel(reason) {
console.log('🎯 Range stream cancelled:', reason);
}
});

const headers = new Headers();
headers.set('Content-Type', mimeType);
headers.set('Content-Length', requestedSize.toString());
headers.set('Content-Range', 'bytes ' + start + '-' + end + '/' + totalSize);
headers.set('Accept-Ranges', 'bytes');
headers.set('Access-Control-Allow-Origin', '*');
headers.set('Content-Disposition', isDownload ? ('attachment; filename="' + metadata.filename + '"') : 'inline');
headers.set('Cache-Control', 'public, max-age=31536000');

return new Response(stream, { status: 206, headers });
}

async function handleFullStreamDownload(request, env, metadata, mimeType) {
const chunks = metadata.chunks;
const filename = metadata.filename;
const totalSize = metadata.size;

console.log('📥 Full download stream started for:', filename);
console.log('📥 Total size:', Math.round(totalSize / 1024 / 1024) + 'MB');
console.log('📥 Total chunks:', chunks.length);

let chunkIndex = 0;
let streamedBytes = 0;

const stream = new ReadableStream({
async pull(controller) {
while (chunkIndex < chunks.length) {
try {
console.log('📥 Downloading chunk ' + (chunkIndex + 1) + '/' + chunks.length);
const chunkData = await loadSingleChunk(env, chunks[chunkIndex]);
const uint8Array = new Uint8Array(chunkData);

controller.enqueue(uint8Array);
streamedBytes += uint8Array.byteLength;
console.log('📥 Chunk ' + (chunkIndex + 1) + ' streamed: ' + Math.round(uint8Array.byteLength / 1024 / 1024) + 'MB');
console.log('📥 Progress: ' + Math.round((chunkIndex + 1) / chunks.length * 100) + '%');

chunkIndex++;

} catch (error) {
console.error('❌ Download chunk ' + (chunkIndex + 1) + ' failed:', error);
controller.error(error);
return;
}
}

console.log('📥 Download completed: ' + Math.round(streamedBytes / 1024 / 1024) + 'MB');
controller.close();
},

cancel(reason) {
console.log('📥 Download stream cancelled:', reason);
}
});

const headers = new Headers();
headers.set('Content-Type', mimeType);
headers.set('Content-Length', totalSize.toString());
headers.set('Content-Disposition', 'attachment; filename="' + filename + '"');
headers.set('Accept-Ranges', 'bytes');
headers.set('Access-Control-Allow-Origin', '*');
headers.set('Cache-Control', 'public, max-age=31536000');

return new Response(stream, { status: 200, headers });
}

async function loadSingleChunk(env, chunkInfo) {
const kvNamespace = env[chunkInfo.kvNamespace] || env.FILES_KV;
const chunkKey = chunkInfo.keyName || chunkInfo.chunkKey;

console.log('📥 Loading chunk:', chunkKey);

const metadataString = await kvNamespace.get(chunkKey);
if (!metadataString) {
throw new Error('Chunk metadata not found: ' + chunkKey);
}

const chunkMetadata = JSON.parse(metadataString);
chunkMetadata.telegramFileId = chunkMetadata.telegramFileId || chunkMetadata.fileIdCode;

if (chunkMetadata.directUrl) {
try {
console.log('🔄 Trying cached URL for chunk:', chunkKey);
const response = await fetchWithRetry(chunkMetadata.directUrl, {
signal: AbortSignal.timeout(30000)
});

if (response.ok) {
console.log('✅ Chunk loaded from cached URL:', chunkKey);
return response.arrayBuffer();
}

console.log('🔄 Cached URL expired for chunk:', chunkKey);
} catch (error) {
console.log('🔄 Cached URL failed for chunk:', chunkKey, error.message);
}
}

console.log('🔄 Refreshing URL for chunk:', chunkKey);

const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);

for (let botIndex = 0; botIndex < botTokens.length; botIndex++) {
const botToken = botTokens[botIndex];

try {
console.log('🤖 Refreshing with bot ' + (botIndex + 1) + '/' + botTokens.length + ' for chunk:', chunkKey);

const getFileResponse = await fetchWithRetry(
'https://api.telegram.org/bot' + botToken + '/getFile?file_id=' + encodeURIComponent(chunkMetadata.telegramFileId),
{ signal: AbortSignal.timeout(15000) }
);

const getFileData = await getFileResponse.json();

if (!getFileData.ok) {
console.error('🤖 Bot ' + (botIndex + 1) + ' API error for chunk ' + chunkKey + ':', getFileData.error_code, '-', getFileData.description);
continue;
}

if (!getFileData.result?.file_path) {
console.error('🤖 Bot ' + (botIndex + 1) + ' no file_path for chunk:', chunkKey);
continue;
}

const freshUrl = 'https://api.telegram.org/file/bot' + botToken + '/' + getFileData.result.file_path;

const response = await fetchWithRetry(freshUrl, {
signal: AbortSignal.timeout(30000)
});

if (response.ok) {
kvNamespace.put(chunkKey, JSON.stringify({
...chunkMetadata,
directUrl: freshUrl,
lastRefreshed: Date.now(),
refreshedBy: 'bot' + (botIndex + 1)
})).catch(function(error) {
console.warn('⚠️ Failed to update KV for chunk ' + chunkKey + ':', error.message);
});

console.log('✅ URL refreshed successfully for chunk:', chunkKey, 'using bot', (botIndex + 1));
return response.arrayBuffer();
}

console.error('📡 Fresh URL failed for chunk ' + chunkKey + ' with bot ' + (botIndex + 1) + ':', response.status);

} catch (botError) {
console.error('❌ Bot ' + (botIndex + 1) + ' failed for chunk ' + chunkKey + ':', botError.message);
continue;
}
}

throw new Error('All refresh attempts failed for chunk: ' + chunkKey);
}

async function fetchWithRetry(url, options, retries) {
if (!retries) retries = 5;

for (let attempt = 0; attempt < retries; attempt++) {
try {
const response = await fetch(url, options);

if (response.ok) {
return response;
}

if (response.status === 429) {
const retryAfter = parseInt(response.headers.get('Retry-After')) || 5;
console.warn('⏳ Rate limited, waiting ' + retryAfter + 's before retry ' + (attempt + 1) + '/' + retries);
await new Promise(function(resolve) { setTimeout(resolve, retryAfter * 1000); });
continue;
}

if (response.status >= 500) {
console.error('🔄 Server error ' + response.status + ' on attempt ' + (attempt + 1) + '/' + retries);
if (attempt < retries - 1) {
await new Promise(function(resolve) { setTimeout(resolve, Math.pow(2, attempt) * 1000); });
continue;
}
}

if (response.status >= 400 && response.status < 500) {
console.error('❌ Client error ' + response.status + ': ' + response.statusText);
return response;
}

console.error('❌ Attempt ' + (attempt + 1) + '/' + retries + ' failed: ' + response.status + ' ' + response.statusText);

} catch (error) {
console.error('❌ Attempt ' + (attempt + 1) + '/' + retries + ' error:', error.message);

if (attempt === retries - 1) {
throw error;
}
}

if (attempt < retries - 1) {
const delay = Math.min(Math.pow(2, attempt) * 1000, 10000);
console.log('⏳ Waiting ' + delay + 'ms before retry ' + (attempt + 2) + '/' + retries);
await new Promise(function(resolve) { setTimeout(resolve, delay); });
}
}

throw new Error('All ' + retries + ' fetch attempts failed for ' + url);
}

function createErrorResponse(message, status, additionalHeaders) {
const headers = new Headers({
'Content-Type': 'application/json',
'Access-Control-Allow-Origin': '*',
...additionalHeaders
});

const errorResponse = {
error: message,
status: status || 500,
timestamp: new Date().toISOString(),
service: 'BTF Storage - Advanced Streaming'
};

console.error('❌ Error response: ' + (status || 500) + ' - ' + message);

return new Response(JSON.stringify(errorResponse, null, 2), {
status: status || 500,
headers: headers
});
}