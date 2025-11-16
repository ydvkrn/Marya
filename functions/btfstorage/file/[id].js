// functions/btfstorage/file/[id].js
// 🎬 TESTED & WORKING - Video Streaming Handler

const MIME_TYPES = {
'mp4': 'video/mp4', 'mkv': 'video/x-matroska', 'avi': 'video/x-msvideo',
'mov': 'video/quicktime', 'm4v': 'video/mp4', 'wmv': 'video/x-ms-wmv',
'flv': 'video/x-flv', '3gp': 'video/3gpp', 'webm': 'video/webm',
'ogv': 'video/ogg', 'mp3': 'audio/mpeg', 'wav': 'audio/wav',
'aac': 'audio/mp4', 'm4a': 'audio/mp4', 'ogg': 'audio/ogg',
'flac': 'audio/flac', 'wma': 'audio/x-ms-wma', 'jpg': 'image/jpeg',
'jpeg': 'image/jpeg', 'png': 'image/png', 'gif': 'image/gif',
'webp': 'image/webp', 'svg': 'image/svg+xml', 'bmp': 'image/bmp',
'tiff': 'image/tiff', 'pdf': 'application/pdf', 'doc': 'application/msword',
'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
'txt': 'text/plain', 'zip': 'application/zip',
'rar': 'application/x-rar-compressed', 'm3u8': 'application/x-mpegURL',
'ts': 'video/mp2t', 'mpd': 'application/dash+xml'
};

export async function onRequest(context) {
const { request, env, params } = context;
const fileId = params.id;

console.log('🎬 Streaming:', fileId);

if (request.method === 'OPTIONS') {
return new Response(null, {
status: 204,
headers: {
'Access-Control-Allow-Origin': '*',
'Access-Control-Allow-Methods': 'GET, HEAD, OPTIONS',
'Access-Control-Allow-Headers': 'Range, Content-Type',
'Access-Control-Max-Age': '86400',
'Access-Control-Expose-Headers': 'Content-Length, Content-Range, Accept-Ranges'
}
});
}

try {
let actualId = fileId;
let extension = '';
let isHlsPlaylist = false;
let isHlsSegment = false;
let segmentIndex = -1;

if (fileId.includes('.')) {
const parts = fileId.split('.');
extension = parts.pop().toLowerCase();
actualId = parts.join('.');

if (extension === 'm3u8') {
isHlsPlaylist = true;
} else if (extension === 'ts' && actualId.includes('-')) {
const segParts = actualId.split('-');
const lastPart = segParts[segParts.length - 1];
if (!isNaN(parseInt(lastPart))) {
segmentIndex = parseInt(segParts.pop(), 10);
actualId = segParts.join('-');
isHlsSegment = true;
}
}
}

console.log('📂 ID:', actualId, 'Ext:', extension);

const metadataString = await env.FILES_KV.get(actualId);
if (!metadataString) {
return errorResponse('File not found', 404);
}

const metadata = JSON.parse(metadataString);

if (!metadata.filename || !metadata.size) {
return errorResponse('Invalid metadata', 400);
}

metadata.telegramFileId = metadata.telegramFileId || metadata.fileIdCode;

if (!metadata.telegramFileId && (!metadata.chunks || metadata.chunks.length === 0)) {
return errorResponse('Missing file source', 400);
}

const mimeType = metadata.contentType || MIME_TYPES[extension] || 'application/octet-stream';

console.log('📦', metadata.filename, Math.round(metadata.size/1024/1024) + 'MB');

if (isHlsPlaylist) {
return handleHlsPlaylist(request, env, metadata, actualId);
}

if (isHlsSegment && segmentIndex >= 0) {
return handleHlsSegment(request, env, metadata, segmentIndex);
}

if (metadata.telegramFileId && (!metadata.chunks || metadata.chunks.length === 0)) {
return streamSingleFile(request, env, metadata, mimeType);
}

if (metadata.chunks && metadata.chunks.length > 0) {
return streamChunkedFile(request, env, metadata, mimeType);
}

return errorResponse('Invalid configuration', 400);

} catch (error) {
console.error('❌ Error:', error.message);
return errorResponse(error.message, 500);
}
}

async function handleHlsPlaylist(request, env, metadata, actualId) {
if (!metadata.chunks || metadata.chunks.length === 0) {
return errorResponse('HLS not supported', 400);
}

const baseUrl = new URL(request.url).origin;
let playlist = '#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:6
';
playlist += '#EXT-X-MEDIA-SEQUENCE:0
#EXT-X-PLAYLIST-TYPE:VOD
';

for (let i = 0; i < metadata.chunks.length; i++) {
playlist += '#EXTINF:6.0,
';
playlist += `${baseUrl}/btfstorage/file/${actualId}-${i}.ts
`;
}

playlist += '#EXT-X-ENDLIST
';

return new Response(playlist, {
status: 200,
headers: {
'Content-Type': 'application/x-mpegURL',
'Access-Control-Allow-Origin': '*',
'Cache-Control': 'no-cache'
}
});
}

async function handleHlsSegment(request, env, metadata, segmentIndex) {
if (!metadata.chunks || segmentIndex >= metadata.chunks.length) {
return errorResponse('Segment not found', 404);
}

try {
const chunkData = await loadChunk(env, metadata.chunks[segmentIndex]);

return new Response(chunkData, {
status: 200,
headers: {
'Content-Type': 'video/mp2t',
'Content-Length': chunkData.byteLength.toString(),
'Access-Control-Allow-Origin': '*',
'Cache-Control': 'public, max-age=31536000',
'Accept-Ranges': 'bytes'
}
});
} catch (error) {
return errorResponse(error.message, 500);
}
}

async function streamSingleFile(request, env, metadata, mimeType) {
const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(Boolean);

for (const botToken of botTokens) {
try {
const fileInfoRes = await fetch(
`https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(metadata.telegramFileId)}`,
{ signal: AbortSignal.timeout(10000) }
);

const fileInfo = await fileInfoRes.json();
if (!fileInfo.ok || !fileInfo.result?.file_path) continue;

const directUrl = `https://api.telegram.org/file/bot${botToken}/${fileInfo.result.file_path}`;

const headers = {};
const rangeHeader = request.headers.get('Range');
if (rangeHeader) headers['Range'] = rangeHeader;

const telegramRes = await fetch(directUrl, {
headers,
signal: AbortSignal.timeout(30000)
});

if (!telegramRes.ok) continue;

const responseHeaders = new Headers();
responseHeaders.set('Content-Type', mimeType);
responseHeaders.set('Access-Control-Allow-Origin', '*');
responseHeaders.set('Accept-Ranges', 'bytes');
responseHeaders.set('Cache-Control', 'public, max-age=31536000');
responseHeaders.set('Content-Disposition', 'inline');

if (telegramRes.headers.get('content-length')) {
responseHeaders.set('Content-Length', telegramRes.headers.get('content-length'));
}
if (telegramRes.headers.get('content-range')) {
responseHeaders.set('Content-Range', telegramRes.headers.get('content-range'));
}

console.log('✅ Single file success');

return new Response(telegramRes.body, {
status: telegramRes.status,
headers: responseHeaders
});

} catch (error) {
continue;
}
}

return errorResponse('All bots failed', 503);
}

async function streamChunkedFile(request, env, metadata, mimeType) {
const chunks = metadata.chunks;
const totalSize = metadata.size;
const chunkSize = metadata.chunkSize || 20971520;

const rangeHeader = request.headers.get('Range');

console.log('🧩', chunks.length, 'chunks');

if (rangeHeader) {
return handleRangeRequest(request, env, metadata, rangeHeader, mimeType, chunkSize);
}

return handleFullStream(request, env, metadata, mimeType);
}

async function handleRangeRequest(request, env, metadata, rangeHeader, mimeType, chunkSize) {
const totalSize = metadata.size;
const chunks = metadata.chunks;

const match = rangeHeader.match(/bytes=(d+)-(d*)/);
if (!match) {
return errorResponse('Invalid range', 416, {
'Content-Range': `bytes */${totalSize}`
});
}

const start = parseInt(match[1], 10);
let end = match[2] ? parseInt(match[2], 10) : totalSize - 1;

if (end >= totalSize) end = totalSize - 1;
if (start >= totalSize || start > end) {
return errorResponse('Range not satisfiable', 416, {
'Content-Range': `bytes */${totalSize}`
});
}

const requestedSize = end - start + 1;
const startChunk = Math.floor(start / chunkSize);
const endChunk = Math.floor(end / chunkSize);

console.log('🎯 Range:', start, '-', end);

let chunkIdx = startChunk;
let position = startChunk * chunkSize;

const { readable, writable } = new TransformStream();
const writer = writable.getWriter();

(async () => {
try {
while (chunkIdx <= endChunk) {
const chunkData = await loadChunk(env, chunks[chunkIdx]);
const bytes = new Uint8Array(chunkData);

const sliceStart = Math.max(start - position, 0);
const sliceEnd = Math.min(bytes.length, end - position + 1);

if (sliceStart < sliceEnd) {
await writer.write(bytes.slice(sliceStart, sliceEnd));
}

position += chunkSize;
chunkIdx++;
if (position > end) break;
}
await writer.close();
} catch (error) {
console.error('❌', error.message);
await writer.abort(error).catch(() => {});
}
})();

return new Response(readable, {
status: 206,
headers: {
'Content-Type': mimeType,
'Content-Length': requestedSize.toString(),
'Content-Range': `bytes ${start}-${end}/${totalSize}`,
'Accept-Ranges': 'bytes',
'Access-Control-Allow-Origin': '*',
'Cache-Control': 'public, max-age=31536000',
'Content-Disposition': 'inline'
}
});
}

async function handleFullStream(request, env, metadata, mimeType) {
const chunks = metadata.chunks;
const totalSize = metadata.size;

let chunkIdx = 0;

const { readable, writable } = new TransformStream();
const writer = writable.getWriter();

(async () => {
try {
while (chunkIdx < chunks.length) {
console.log('📦', chunkIdx + 1, '/', chunks.length);
const chunkData = await loadChunk(env, chunks[chunkIdx]);
await writer.write(new Uint8Array(chunkData));
chunkIdx++;
}
await writer.close();
console.log('✅ Stream complete');
} catch (error) {
console.error('❌', error.message);
await writer.abort(error).catch(() => {});
}
})();

return new Response(readable, {
status: 200,
headers: {
'Content-Type': mimeType,
'Content-Length': totalSize.toString(),
'Accept-Ranges': 'bytes',
'Access-Control-Allow-Origin': '*',
'Cache-Control': 'public, max-age=31536000',
'Content-Disposition': 'inline'
}
});
}

async function loadChunk(env, chunkInfo) {
const kvNamespace = env[chunkInfo.kvNamespace] || env.FILES_KV;
const chunkKey = chunkInfo.keyName || chunkInfo.chunkKey;

const metadataStr = await kvNamespace.get(chunkKey);
if (!metadataStr) {
throw new Error('Chunk not found: ' + chunkKey);
}

const chunkMeta = JSON.parse(metadataStr);
const fileId = chunkMeta.telegramFileId || chunkMeta.fileIdCode;

if (chunkMeta.directUrl) {
try {
const res = await fetch(chunkMeta.directUrl, { signal: AbortSignal.timeout(20000) });
if (res.ok) return res.arrayBuffer();
} catch (e) {
console.log('🔄 URL expired');
}
}

const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(Boolean);

for (const botToken of botTokens) {
try {
const fileInfoRes = await fetch(
`https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(fileId)}`,
{ signal: AbortSignal.timeout(10000) }
);

const fileInfo = await fileInfoRes.json();
if (!fileInfo.ok || !fileInfo.result?.file_path) continue;

const freshUrl = `https://api.telegram.org/file/bot${botToken}/${fileInfo.result.file_path}`;
const res = await fetch(freshUrl, { signal: AbortSignal.timeout(20000) });

if (res.ok) {
kvNamespace.put(chunkKey, JSON.stringify({
...chunkMeta,
directUrl: freshUrl,
lastRefreshed: Date.now()
})).catch(() => {});

return res.arrayBuffer();
}
} catch (e) {
continue;
}
}

throw new Error('All bots failed for chunk');
}

function errorResponse(message, status = 500, additionalHeaders = {}) {
return new Response(JSON.stringify({
error: message,
status: status,
timestamp: new Date().toISOString()
}), {
status: status,
headers: {
'Content-Type': 'application/json',
'Access-Control-Allow-Origin': '*',
...additionalHeaders
}
});
}