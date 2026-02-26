// btfstorage-client.js
// ⚡ Client-side uploader — splits files into 45MB chunks, uploads in parallel
// Works in browser. Import or include via <script> tag.
// Usage: const uploader = new BTFUploader({ endpoint: '/btfstorage/upload' })
//        await uploader.upload(file, { onProgress, onComplete, onError })

const CHUNK_SIZE    = 45 * 1024 * 1024; // 45MB — well under Telegram's 50MB limit
const MAX_PARALLEL  = 3;                // Upload 3 chunks at a time (fast but not spammy)
const MAX_RETRIES   = 4;
const BASE_DELAY_MS = 1500;

export class BTFUploader {
  constructor({ endpoint = '/btfstorage/upload' } = {}) {
    this.endpoint = endpoint;
  }

  // ── Main upload method ────────────────────────────────────────────────────
  async upload(file, { onProgress, onComplete, onError } = {}) {
    try {
      const fileId      = generateId();
      const totalChunks = Math.ceil(file.size / CHUNK_SIZE);
      const results     = new Array(totalChunks);
      let   uploaded    = 0;

      // Semaphore-based parallel upload
      const queue = Array.from({ length: totalChunks }, (_, i) => i);

      const workers = Array.from({ length: Math.min(MAX_PARALLEL, totalChunks) }, async () => {
        while (queue.length) {
          const i = queue.shift();
          if (i === undefined) break;

          const start    = i * CHUNK_SIZE;
          const end      = Math.min(start + CHUNK_SIZE, file.size);
          const chunk    = file.slice(start, end);

          results[i] = await this._uploadChunkWithRetry(
            chunk, i, totalChunks, fileId, file, (bytesDone) => {
              onProgress?.({
                chunkIndex:   i,
                totalChunks,
                chunkBytes:   bytesDone,
                chunkTotal:   chunk.size,
                // overall approximate
                overallBytes: uploaded * CHUNK_SIZE + bytesDone,
                overallTotal: file.size,
                pct:          Math.round(((uploaded * CHUNK_SIZE + bytesDone) / file.size) * 100),
              });
            }
          );

          uploaded++;
          onProgress?.({
            chunkIndex:   i,
            totalChunks,
            chunkBytes:   chunk.size,
            chunkTotal:   chunk.size,
            overallBytes: uploaded * CHUNK_SIZE,
            overallTotal: file.size,
            pct:          Math.round((uploaded / totalChunks) * 100),
          });
        }
      });

      await Promise.all(workers);

      // Last chunk response has the final file info
      const finalResult = results[totalChunks - 1];
      onComplete?.(finalResult);
      return finalResult;

    } catch (err) {
      onError?.(err);
      throw err;
    }
  }

  // ── Upload single chunk with retry ────────────────────────────────────────
  async _uploadChunkWithRetry(chunk, index, totalChunks, fileId, file, onChunkProgress) {
    let lastErr;
    for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
      try {
        return await this._uploadChunk(chunk, index, totalChunks, fileId, file, onChunkProgress);
      } catch (e) {
        lastErr = e;
        if (attempt < MAX_RETRIES - 1) {
          await sleep(BASE_DELAY_MS * Math.pow(2, attempt));
        }
      }
    }
    throw new Error(`Chunk ${index} failed after ${MAX_RETRIES} attempts: ${lastErr?.message}`);
  }

  // ── Upload single chunk via XHR (for progress events) ────────────────────
  _uploadChunk(chunk, index, totalChunks, fileId, file, onChunkProgress) {
    return new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      xhr.open('POST', this.endpoint, true);

      // Headers tell server about this chunk
      xhr.setRequestHeader('X-File-Id',      fileId);
      xhr.setRequestHeader('X-Chunk-Index',  String(index));
      xhr.setRequestHeader('X-Total-Chunks', String(totalChunks));
      xhr.setRequestHeader('X-File-Name',    encodeURIComponent(file.name));
      xhr.setRequestHeader('X-File-Size',    String(file.size));
      xhr.setRequestHeader('X-File-Type',    file.type || 'application/octet-stream');

      xhr.upload.onprogress = (e) => {
        if (e.lengthComputable) onChunkProgress?.(e.loaded);
      };

      xhr.onload = () => {
        if (xhr.status >= 200 && xhr.status < 300) {
          try {
            resolve(JSON.parse(xhr.responseText));
          } catch {
            reject(new Error('Invalid JSON response'));
          }
        } else {
          reject(new Error(`Server returned ${xhr.status}: ${xhr.responseText}`));
        }
      };

      xhr.onerror   = () => reject(new Error('Network error'));
      xhr.ontimeout = () => reject(new Error('Request timed out'));
      xhr.timeout   = 300_000; // 5 min per chunk max

      xhr.send(chunk);
    });
  }
}

// ── Standalone upload function (simpler API) ───────────────────────────────
export async function uploadFile(file, options = {}) {
  const uploader = new BTFUploader({ endpoint: options.endpoint });
  return uploader.upload(file, options);
}

function generateId() {
  return `id${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}`;
}

const sleep = ms => new Promise(r => setTimeout(r, ms));
