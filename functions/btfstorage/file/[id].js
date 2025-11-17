// functions/btfstorage/files/[id].js
// Working Streaming Handler

const MIME_TYPES = {
'mp4': 'video/mp4', 'mkv': 'video/x-matroska', 'avi': 'video/x-msvideo',
'mov': 'video/quicktime', 'webm': 'video/webm', 'mp3': 'audio/mpeg',
'wav': 'audio/wav', 'm4a': 'audio/mp4', 'jpg': 'image/jpeg', 'png': 'image/png',
'pdf': 'application/pdf', 'zip': 'application/zip'
};

export async function onRequest(context) {
const { request, env, params } = context;
const fileId = params.id;

if (request.method === 'OPTIONS') {
const h = new Headers();
h.set('Access-Control-Allow-Origin', '*');
h.set('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
h.set('Access-Control-Allow-Headers', 'Range, Content-Type');
return new Response(null, { status: 204, headers: h });
}

try {
let actualId = fileId;
let extension = '';

if (fileId.includes('.')) {
const idx = fileId.lastIndexOf('.');
actualId = fileId.substring(0, idx);
extension = fileId.substring(idx + 1).toLowerCase();
}

const metaStr = await env.FILES_KV.get(actualId);
if (!metaStr) {
return errRes('File not found', 404);
}

const meta = JSON.parse(metaStr);
if (!meta.filename || !meta.size) {
return errRes('Invalid metadata', 400);
}

meta.telegramFileId = meta.telegramFileId || meta.fileIdCode;

if (!meta.telegramFileId && (!meta.chunks || meta.chunks.length === 0)) {
return errRes('Missing source', 400);
}

const mime = meta.contentType || MIME_TYPES[extension] || 'application/octet-stream';

if (meta.telegramFileId && (!meta.chunks || meta.chunks.length === 0)) {
return handleSingle(request, env, meta, mime);
}

if (meta.chunks && meta.chunks.length > 0) {
return handleChunked(request, env, meta, mime);
}

return errRes('Invalid file', 400);

} catch (e) {
return errRes('Error: ' + e.message, 500);
}
}

async function handleSingle(req, env, meta, mime) {
const bots = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);

if (bots.length === 0) {
return errRes('No bots', 503);
}

for (let i = 0; i < bots.length; i++) {
const bot = bots[i];

try {
const fileRes = await fetch(
'https://api.telegram.org/bot' + bot + '/getFile?file_id=' + encodeURIComponent(meta.telegramFileId),
{ signal: AbortSignal.timeout(15000) }
);

const fileData = await fileRes.json();

if (!fileData.ok || !fileData.result?.file_path) {
continue;
}

const url = 'https://api.telegram.org/file/bot' + bot + '/' + fileData.result.file_path;

const headers = {};
const range = req.headers.get('Range');
if (range) {
headers.Range = range;
}

const tgRes = await fetch(url, {
headers: headers,
signal: AbortSignal.timeout(45000)
});

if (!tgRes.ok) {
continue;
}

const h = new Headers();

const cl = tgRes.headers.get('content-length');
const cr = tgRes.headers.get('content-range');
const ar = tgRes.headers.get('accept-ranges');

if (cl) h.set('Content-Length', cl);
if (cr) h.set('Content-Range', cr);
if (ar) h.set('Accept-Ranges', ar);

if (!ar) h.set('Accept-Ranges', 'bytes');

h.set('Content-Type', mime);
h.set('Access-Control-Allow-Origin', '*');
h.set('Cache-Control', 'public, max-age=31536000');

const u = new URL(req.url);
if (u.searchParams.has('dl') || u.searchParams.has('download')) {
h.set('Content-Disposition', 'attachment; filename="' + meta.filename + '"');
} else {
h.set('Content-Disposition', 'inline');
}

return new Response(tgRes.body, {
status: tgRes.status,
headers: h
});

} catch (e) {
continue;
}
}

return errRes('All bots failed', 503);
}

async function handleChunked(req, env, meta, mime) {
const chunks = meta.chunks;
const total = meta.size;
const chunkSize = meta.chunkSize || 20971520;

const range = req.headers.get('Range');
const u = new URL(req.url);
const isDl = u.searchParams.has('dl') || u.searchParams.has('download');

if (range) {
return handleRange(env, meta, range, mime, chunkSize, isDl, total);
}

if (isDl) {
return handleDL(env, meta, mime, total);
}

return handlePlay(env, meta, mime, total);
}

async function handlePlay(env, meta, mime, total) {
const chunks = meta.chunks;

try {
const firstChunk = await loadChunk(env, chunks[0]);
const arr = new Uint8Array(firstChunk);

const h = new Headers();
h.set('Content-Type', mime);
h.set('Content-Length', total.toString());
h.set('Accept-Ranges', 'bytes');
h.set('Access-Control-Allow-Origin', '*');
h.set('Cache-Control', 'public, max-age=31536000');
h.set('Content-Disposition', 'inline');

let idx = 1;

const stream = new ReadableStream({
start(ctrl) {
ctrl.enqueue(arr);
},
async pull(ctrl) {
if (idx >= chunks.length) {
ctrl.close();
return;
}

try {
const data = await loadChunk(env, chunks[idx]);
ctrl.enqueue(new Uint8Array(data));
idx++;
} catch (e) {
ctrl.error(e);
}
}
});

return new Response(stream, { status: 200, headers: h });

} catch (e) {
return errRes('Play failed: ' + e.message, 500);
}
}

async function handleRange(env, meta, rangeHdr, mime, chunkSize, isDl, total) {
const chunks = meta.chunks;

const match = rangeHdr.match(/bytes=(d+)-(d*)/);
if (!match) {
return errRes('Invalid range', 416, {
'Content-Range': 'bytes */' + total
});
}

const start = parseInt(match[1], 10);
let end = match[2] ? parseInt(match[2], 10) : total - 1;

if (end >= total) end = total - 1;
if (start >= total || start > end) {
return errRes('Range error', 416, {
'Content-Range': 'bytes */' + total
});
}

const size = end - start + 1;

const startChunk = Math.floor(start / chunkSize);
const endChunk = Math.floor(end / chunkSize);
const needed = chunks.slice(startChunk, endChunk + 1);

try {
let pos = startChunk * chunkSize;
const parts = [];

for (let i = 0; i < needed.length; i++) {
const data = await loadChunk(env, needed[i]);
const arr = new Uint8Array(data);

const cStart = Math.max(start - pos, 0);
const cEnd = Math.min(arr.length, end - pos + 1);

if (cStart < cEnd) {
parts.push(arr.slice(cStart, cEnd));
}

pos += chunkSize;
if (pos > end) break;
}

const len = parts.reduce(function(sum, p) { return sum + p.length; }, 0);
const combined = new Uint8Array(len);
let offset = 0;

for (let i = 0; i < parts.length; i++) {
combined.set(parts[i], offset);
offset += parts[i].length;
}

const h = new Headers();
h.set('Content-Type', mime);
h.set('Content-Length', size.toString());
h.set('Content-Range', 'bytes ' + start + '-' + end + '/' + total);
h.set('Accept-Ranges', 'bytes');
h.set('Access-Control-Allow-Origin', '*');
h.set('Content-Disposition', isDl ? ('attachment; filename="' + meta.filename + '"') : 'inline');
h.set('Cache-Control', 'public, max-age=31536000');

return new Response(combined, { status: 206, headers: h });

} catch (e) {
return errRes('Range failed: ' + e.message, 500);
}
}

async function handleDL(env, meta, mime, total) {
const chunks = meta.chunks;
const name = meta.filename;

let idx = 0;

const stream = new ReadableStream({
async pull(ctrl) {
if (idx >= chunks.length) {
ctrl.close();
return;
}

try {
const data = await loadChunk(env, chunks[idx]);
ctrl.enqueue(new Uint8Array(data));
idx++;
} catch (e) {
ctrl.error(e);
}
}
});

const h = new Headers();
h.set('Content-Type', mime);
h.set('Content-Length', total.toString());
h.set('Content-Disposition', 'attachment; filename="' + name + '"');
h.set('Access-Control-Allow-Origin', '*');
h.set('Cache-Control', 'public, max-age=31536000');

return new Response(stream, { status: 200, headers: h });
}

async function loadChunk(env, info) {
const kv = env[info.kvNamespace] || env.FILES_KV;
const key = info.keyName || info.chunkKey;

const metaStr = await kv.get(key);
if (!metaStr) {
throw new Error('Chunk not found: ' + key);
}

const meta = JSON.parse(metaStr);
meta.telegramFileId = meta.telegramFileId || meta.fileIdCode;

if (meta.directUrl) {
try {
const res = await fetch(meta.directUrl, {
signal: AbortSignal.timeout(30000)
});

if (res.ok) {
return res.arrayBuffer();
}
} catch (e) {
console.log('Cache expired');
}
}

const bots = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);

for (let i = 0; i < bots.length; i++) {
const bot = bots[i];

try {
const fileRes = await fetch(
'https://api.telegram.org/bot' + bot + '/getFile?file_id=' + encodeURIComponent(meta.telegramFileId),
{ signal: AbortSignal.timeout(15000) }
);

const fileData = await fileRes.json();

if (!fileData.ok || !fileData.result?.file_path) {
continue;
}

const url = 'https://api.telegram.org/file/bot' + bot + '/' + fileData.result.file_path;

const res = await fetch(url, {
signal: AbortSignal.timeout(30000)
});

if (res.ok) {
kv.put(key, JSON.stringify({
...meta,
directUrl: url,
refreshed: Date.now()
})).catch(function() {});

return res.arrayBuffer();
}

} catch (e) {
continue;
}
}

throw new Error('All bots failed');
}

function errRes(msg, status, extra) {
const h = new Headers({
'Content-Type': 'application/json',
'Access-Control-Allow-Origin': '*',
...extra
});

const err = {
error: msg,
status: status || 500,
time: new Date().toISOString()
};

return new Response(JSON.stringify(err, null, 2), {
status: status || 500,
headers: h
});
}