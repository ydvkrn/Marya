// functions/btfstorage/file/[id].js
// Advanced File Streaming Handler

const MIME_TYPES = {
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
'mp3': 'audio/mpeg',
'wav': 'audio/wav',
'aac': 'audio/mp4',
'm4a': 'audio/mp4',
'ogg': 'audio/ogg',
'flac': 'audio/flac',
'wma': 'audio/x-ms-wma',
'jpg': 'image/jpeg',
'jpeg': 'image/jpeg',
'png': 'image/png',
'gif': 'image/gif',
'webp': 'image/webp',
'svg': 'image/svg+xml',
'bmp': 'image/bmp',
'tiff': 'image/tiff',
'pdf': 'application/pdf',
'doc': 'application/msword',
'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
'txt': 'text/plain',
'zip': 'application/zip',
'rar': 'application/x-rar-compressed'
};

export async function onRequest(context) {
const { request, env, params } = context;
const fileId = params.id;

console.log('Streaming started:', fileId);

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
}

const metadataString = await env.FILES_KV.get(actualId);

if (!metadataString) {
return createErrorResponse('File not found', 404);
}

const metadata = JSON.parse(metadataString);

if (!metadata.filename || !metadata.size) {
return createErrorResponse('Invalid file metadata', 400);
}

metadata.telegramFileId = metadata.telegramFileId || metadata.fileIdCode;

if (!metadata.telegramFileId && (!metadata.chunks || metadata.chunks.length === 0)) {
return createErrorResponse('Missing file source data', 400);
}

const mimeType = metadata.contentType || MIME_TYPES[extension] || 'application/octet-stream';

if (metadata.telegramFileId && (!metadata.chunks || metadata.chunks.length === 0)) {
return await handleSingleFile(request, env, metadata, mimeType);
}

if (metadata.chunks && metadata.chunks.length > 0) {
return await handleChunkedFile(request, env, metadata, mimeType);
}

return createErrorResponse('Invalid file format', 400);

} catch (error) {
console.error('Critical error:', error);
return createErrorResponse('Streaming error: ' + error.message, 500);
}
}

async function handleSingleFile(request, env, metadata, mimeType) {
const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);

if (botTokens.length === 0) {
return createErrorResponse('Service configuration error', 503);
}

for (let botIndex = 0; botIndex < botTokens.length; botIndex++) {
const botToken = botTokens[botIndex];

try {
const getFileResponse = await fetch(
'https://api.telegram.org/bot' + botToken + '/getFile?file_id=' + encodeURIComponent(metadata.telegramFileId),
{ signal: AbortSignal.timeout(15000) }
);

const getFileData = await getFileResponse.json();

if (!getFileData.ok || !getFileData.result?.file_path) {
continue;
}

const directUrl = 'https://api.telegram.org/file/bot' + botToken + '/' + getFileData.result.file_path;

const requestHeaders = {};
const rangeHeader = request.headers.get('Range');

if (rangeHeader) {
requestHeaders['Range'] = rangeHeader;
}

const telegramResponse = await fetch(directUrl, {
headers: requestHeaders,
signal: AbortSignal.timeout(45000)
});

if (!telegramResponse.ok) {
continue;
}

const responseHeaders = new Headers();

['content-length', 'content-range', 'accept-ranges'].forEach(function(header) {
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
responseHeaders.set('Content-Disposition', 'attachment; filename="' + metadata.filename + '"');
} else {
responseHeaders.set('Content-Disposition', 'inline');
}

return new Response(telegramResponse.body, {
status: telegramResponse.status,
headers: responseHeaders
});

} catch (botError) {
console.error('Bot failed:', botError);
continue;
}
}

return createErrorResponse('All bots failed', 503);
}

async function handleChunkedFile(request, env, metadata, mimeType) {
const chunks = metadata.chunks;
const totalSize = metadata.size;
const chunkSize = metadata.chunkSize || 20971520;

const rangeHeader = request.headers.get('Range');
const url = new URL(request.url);
const isDownload = url.searchParams.has('dl') || url.searchParams.has('download');

if (rangeHeader) {
return await handleRangeRequest(request, env, metadata, rangeHeader, mimeType, chunkSize, isDownload);
}

if (isDownload) {
return await handleDownload(request, env, metadata, mimeType);
}

return await handleStream(request, env, metadata, mimeType, totalSize);
}

async function handleStream(request, env, metadata, mimeType, totalSize) {
const chunks = metadata.chunks;

try {
const maxInitialChunks = Math.min(3, chunks.length);

let loadedBytes = 0;
let chunkIndex = 0;

const stream = new ReadableStream({
async pull(controller) {
while (chunkIndex < maxInitialChunks) {
try {
const chunkData = await loadChunk(env, chunks[chunkIndex]);
const uint8Array = new Uint8Array(chunkData);

controller.enqueue(uint8Array);
loadedBytes += uint8Array.byteLength;
chunkIndex++;

} catch (error) {
console.error('Chunk failed:', error);
controller.error(error);
return;
}
}

controller.close();
},

cancel(reason) {
console.log('Stream cancelled:', reason);
}
});

const headers = new Headers();
headers.set('Content-Type', mimeType);
headers.set('Content-Length', Math.min(loadedBytes, totalSize).toString());
headers.set('Content-Range', 'bytes 0-' + (Math.min(loadedBytes, totalSize) - 1) + '/' + totalSize);
headers.set('Accept-Ranges', 'bytes');
headers.set('Access-Control-Allow-Origin', '*');
headers.set('Content-Disposition', 'inline');
headers.set('Cache-Control', 'public, max-age=31536000');

return new Response(stream, { status: 206, headers });

} catch (error) {
return createErrorResponse('Stream failed: ' + error.message, 500);
}
}

async function handleRangeRequest(request, env, metadata, rangeHeader, mimeType, chunkSize, isDownload) {
const totalSize = metadata.size;
const chunks = metadata.chunks;

const rangeMatch = rangeHeader.match(/bytes=(d+)-(d*)/);
if (!rangeMatch) {
return createErrorResponse('Invalid range', 416, {
'Content-Range': 'bytes */' + totalSize
});
}

const start = parseInt(rangeMatch[1], 10);
let end = rangeMatch[2] ? parseInt(rangeMatch[2], 10) : totalSize - 1;

if (end >= totalSize) end = totalSize - 1;
if (start >= totalSize || start > end) {
return createErrorResponse('Range error', 416, {
'Content-Range': 'bytes */' + totalSize
});
}

const requestedSize = end - start + 1;

const startChunk = Math.floor(start / chunkSize);
const endChunk = Math.floor(end / chunkSize);
const neededChunks = chunks.slice(startChunk, endChunk + 1);

let currentPosition = startChunk * chunkSize;

const stream = new ReadableStream({
async pull(controller) {
for (let i = 0; i < neededChunks.length; i++) {
const chunkInfo = neededChunks[i];

try {
const chunkData = await loadChunk(env, chunkInfo);
const uint8Array = new Uint8Array(chunkData);

const chunkStart = Math.max(start - currentPosition, 0);
const chunkEnd = Math.min(uint8Array.length, end - currentPosition + 1);

if (chunkStart < chunkEnd) {
const chunkSlice = uint8Array.slice(chunkStart, chunkEnd);
controller.enqueue(chunkSlice);
}

currentPosition += chunkSize;
if (currentPosition > end) break;

} catch (error) {
console.error('Range chunk failed:', error);
controller.error(error);
return;
}
}

controller.close();
},

cancel(reason) {
console.log('Range cancelled:', reason);
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

async function handleDownload(request, env, metadata, mimeType) {
const chunks = metadata.chunks;
const filename = metadata.filename;
const totalSize = metadata.size;

let chunkIndex = 0;

const stream = new ReadableStream({
async pull(controller) {
while (chunkIndex < chunks.length) {
try {
const chunkData = await loadChunk(env, chunks[chunkIndex]);
const uint8Array = new Uint8Array(chunkData);

controller.enqueue(uint8Array);
chunkIndex++;

} catch (error) {
console.error('Download chunk failed:', error);
controller.error(error);
return;
}
}

controller.close();
},

cancel(reason) {
console.log('Download cancelled:', reason);
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

async function loadChunk(env, chunkInfo) {
const kvNamespace = env[chunkInfo.kvNamespace] || env.FILES_KV;
const chunkKey = chunkInfo.keyName || chunkInfo.chunkKey;

const metadataString = await kvNamespace.get(chunkKey);
if (!metadataString) {
throw new Error('Chunk not found: ' + chunkKey);
}

const chunkMetadata = JSON.parse(metadataString);
chunkMetadata.telegramFileId = chunkMetadata.telegramFileId || chunkMetadata.fileIdCode;

if (chunkMetadata.directUrl) {
try {
const response = await fetch(chunkMetadata.directUrl, {
signal: AbortSignal.timeout(30000)
});

if (response.ok) {
return response.arrayBuffer();
}
} catch (error) {
console.log('Cache expired');
}
}

const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);

for (let botIndex = 0; botIndex < botTokens.length; botIndex++) {
const botToken = botTokens[botIndex];

try {
const getFileResponse = await fetch(
'https://api.telegram.org/bot' + botToken + '/getFile?file_id=' + encodeURIComponent(chunkMetadata.telegramFileId),
{ signal: AbortSignal.timeout(15000) }
);

const getFileData = await getFileResponse.json();

if (!getFileData.ok || !getFileData.result?.file_path) {
continue;
}

const freshUrl = 'https://api.telegram.org/file/bot' + botToken + '/' + getFileData.result.file_path;

const response = await fetch(freshUrl, {
signal: AbortSignal.timeout(30000)
});

if (response.ok) {
kvNamespace.put(chunkKey, JSON.stringify({
...chunkMetadata,
directUrl: freshUrl,
lastRefreshed: Date.now()
})).catch(function() {});

return response.arrayBuffer();
}

} catch (botError) {
continue;
}
}

throw new Error('All bots failed');
}

function createErrorResponse(message, status, additionalHeaders) {
const headers = new Headers({
'Content-Type': 'application/json',
'Access-Control-Allow-Origin', '*',
...additionalHeaders
});

const errorResponse = {
error: message,
status: status || 500,
timestamp: new Date().toISOString()
};

return new Response(JSON.stringify(errorResponse, null, 2), {
status: status || 500,
headers: headers
});
}