<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<meta name="referrer" content="no-referrer-when-downgrade">
<title>Marya Vault - Professional File Storage</title>

<!-- Preconnect for speed -->
<link rel="preconnect" href="https://api.telegram.org">
<link rel="dns-prefetch" href="//api.telegram.org">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://cdnjs.cloudflare.com">

<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&display=swap" rel="stylesheet">
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" crossorigin="anonymous" referrerpolicy="no-referrer" />

<style>
  :root {
    --red:        #dc2626;
    --red-dark:   #b91c1c;
    --red-light:  #fef2f2;
    --white:      #ffffff;
    --g50:        #f9fafb;
    --g100:       #f3f4f6;
    --g200:       #e5e7eb;
    --g300:       #d1d5db;
    --g600:       #4b5563;
    --g700:       #374151;
    --g900:       #111827;
    --success:    #059669;
    --success-lt: #d1fae5;
    --error-lt:   #fee2e2;
    --sh-sm:      0 1px 2px 0 rgb(0 0 0/.05);
    --sh-md:      0 4px 6px -1px rgb(0 0 0/.1);
    --sh-lg:      0 10px 15px -3px rgb(0 0 0/.1);
    --sh-xl:      0 20px 25px -5px rgb(0 0 0/.1);
  }

  *, *::before, *::after { margin:0; padding:0; box-sizing:border-box; }

  body {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    background: linear-gradient(135deg, var(--white) 0%, var(--g50) 100%);
    min-height: 100vh;
    color: var(--g900);
    -webkit-font-smoothing: antialiased;
  }

  .container { max-width: 1100px; margin: 0 auto; padding: 2rem; }

  /* ── Header ── */
  .header { text-align:center; margin-bottom:3rem; padding:2rem 0; }
  .logo {
    width:100px; height:100px;
    background: linear-gradient(135deg, var(--red), var(--red-dark));
    border-radius:24px; margin:0 auto 2rem;
    display:flex; align-items:center; justify-content:center;
    box-shadow: var(--sh-xl); position:relative; overflow:hidden;
  }
  .logo::after {
    content:''; position:absolute; inset:0;
    background: linear-gradient(45deg,transparent 30%,rgba(255,255,255,.1) 50%,transparent 70%);
    animation: shimmer 3s infinite;
  }
  @keyframes shimmer { 0%{transform:translateX(-100%)} 100%{transform:translateX(100%)} }

  .title {
    font-size:3.5rem; font-weight:900;
    background:linear-gradient(135deg,var(--red),var(--red-dark));
    -webkit-background-clip:text; -webkit-text-fill-color:transparent; background-clip:text;
    letter-spacing:-0.02em; margin-bottom:1rem;
  }
  .subtitle { font-size:1.125rem; color:var(--g600); font-weight:500; max-width:600px; margin:0 auto; }

  /* ── Upload Card ── */
  .upload-section {
    background:var(--white); border-radius:24px; box-shadow:var(--sh-xl);
    padding:3rem; margin-bottom:3rem; border:1px solid var(--g100);
    position:relative; overflow:hidden;
  }
  .upload-section::before {
    content:''; position:absolute; top:0; left:0; right:0; height:4px;
    background:linear-gradient(90deg,var(--red),var(--red-dark));
  }

  .dropzone {
    border:2px dashed var(--g200); border-radius:16px; padding:4rem 2rem;
    text-align:center; cursor:pointer; background:var(--g50);
    transition:all .3s cubic-bezier(.4,0,.2,1);
    min-height:280px; display:flex; flex-direction:column;
    align-items:center; justify-content:center;
  }
  .dropzone:hover, .dropzone.dragover {
    border-color:var(--red); background:var(--red-light);
    transform:translateY(-2px); box-shadow:var(--sh-lg);
  }
  .upload-icon { font-size:4rem; margin-bottom:1.5rem; color:var(--red); animation:float 6s ease-in-out infinite; }
  @keyframes float { 0%,100%{transform:translateY(0)} 50%{transform:translateY(-10px)} }

  .upload-title { font-size:1.75rem; font-weight:700; margin-bottom:.5rem; }
  .upload-desc  { font-size:1rem; color:var(--g600); margin-bottom:2rem; }

  .badges { display:flex; justify-content:center; gap:.75rem; flex-wrap:wrap; margin-top:1.5rem; }
  .badge {
    background:var(--red); color:var(--white);
    padding:.5rem 1.25rem; border-radius:50px; font-size:.8rem;
    font-weight:600; text-transform:uppercase; letter-spacing:.025em;
  }
  .badge i { margin-right:.4rem; }

  .file-input { display:none; }

  /* ── Results ── */
  .results { display:none; }
  .results.show { display:block; animation:fadeIn .4s ease; }
  @keyframes fadeIn { from{opacity:0;transform:translateY(16px)} to{opacity:1;transform:translateY(0)} }

  .results-header { display:flex; align-items:center; gap:1rem; margin-bottom:2rem; }
  .results-title  { font-size:1.75rem; font-weight:700; }
  .results-count  {
    background:var(--red-light); color:var(--red);
    padding:.4rem 1rem; border-radius:50px; font-size:.85rem; font-weight:600;
  }

  /* ── File Item ── */
  .file-item {
    background:var(--white); border-radius:16px; padding:2rem;
    margin-bottom:1.5rem; box-shadow:var(--sh-md); border:1px solid var(--g100);
    transition:all .3s ease; position:relative; overflow:hidden;
  }
  .file-item::before {
    content:''; position:absolute; left:0; top:0; bottom:0; width:4px;
    background:var(--red);
  }
  .file-item:hover { transform:translateY(-2px); box-shadow:var(--sh-lg); }

  .file-header { display:flex; align-items:flex-start; gap:1.5rem; margin-bottom:1.5rem; }
  .file-icon-wrap {
    width:60px; height:60px; flex-shrink:0;
    background:linear-gradient(135deg,var(--red),var(--red-dark));
    border-radius:12px; display:flex; align-items:center; justify-content:center;
    color:var(--white); font-size:1.75rem;
  }
  .file-name { font-size:1.1rem; font-weight:600; margin-bottom:.4rem; word-break:break-all; }
  .file-meta { color:var(--g600); font-size:.85rem; }
  .file-status { font-weight:600; }
  .file-status.uploading { color:var(--red); }
  .file-status.done      { color:var(--success); }
  .file-status.failed    { color:var(--red); }

  /* ── Progress ── */
  .progress-wrap  { margin:1.25rem 0; }
  .progress-bar   { width:100%; height:8px; background:var(--g200); border-radius:50px; overflow:hidden; }
  .progress-fill  {
    height:100%; width:0%; border-radius:50px;
    background:linear-gradient(90deg,var(--red),var(--red-dark));
    transition:width .3s ease; position:relative;
  }
  .progress-fill::after {
    content:''; position:absolute; inset:0;
    background:linear-gradient(90deg,transparent,rgba(255,255,255,.3),transparent);
    animation:progress-shimmer 2s infinite;
  }
  @keyframes progress-shimmer { 0%{transform:translateX(-100%)} 100%{transform:translateX(100%)} }
  .progress-text { text-align:center; margin-top:.4rem; font-size:.8rem; font-weight:600; color:var(--red); }

  /* ── URL Box (new!) ── */
  .url-box {
    background:var(--g50); border:1px solid var(--g200); border-radius:10px;
    padding:.75rem 1rem; margin-bottom:.75rem;
    display:flex; align-items:center; gap:.75rem;
  }
  .url-label { font-size:.75rem; font-weight:700; color:var(--g600); min-width:72px; text-transform:uppercase; }
  .url-text  {
    flex:1; font-size:.8rem; color:var(--g700); font-family:monospace;
    white-space:nowrap; overflow:hidden; text-overflow:ellipsis;
  }
  .url-copy {
    background:none; border:none; cursor:pointer;
    color:var(--red); font-size:.9rem; padding:.25rem .5rem;
    border-radius:6px; transition:background .2s;
  }
  .url-copy:hover { background:var(--red-light); }

  /* ── Action Buttons ── */
  .file-actions { display:flex; gap:.75rem; flex-wrap:wrap; margin-top:1rem; }
  .btn {
    display:inline-flex; align-items:center; gap:.5rem;
    padding:.75rem 1.25rem; border:none; border-radius:10px;
    font-size:.85rem; font-weight:600; text-decoration:none;
    cursor:pointer; transition:all .2s; min-width:110px; justify-content:center;
  }
  .btn-primary  { background:var(--red); color:var(--white); }
  .btn-primary:hover  { background:var(--red-dark); transform:translateY(-1px); box-shadow:var(--sh-md); }
  .btn-success  { background:var(--success); color:var(--white); }
  .btn-success:hover  { background:#047857; transform:translateY(-1px); }
  .btn-purple   { background:#7c3aed; color:var(--white); }
  .btn-purple:hover   { background:#6d28d9; transform:translateY(-1px); }
  .btn-outline  { background:var(--white); color:var(--g700); border:1px solid var(--g300); }
  .btn-outline:hover  { background:var(--g50); transform:translateY(-1px); }

  /* ── Toast ── */
  .toast {
    position:fixed; top:2rem; right:2rem;
    background:var(--white); border-radius:12px; padding:1.25rem 1.5rem;
    box-shadow:var(--sh-xl); border:1px solid var(--g200); z-index:1000;
    transform:translateX(420px); transition:all .3s cubic-bezier(.4,0,.2,1);
    max-width:380px; display:flex; align-items:center; gap:1rem;
  }
  .toast.show { transform:translateX(0); }
  .toast.success { border-left:4px solid var(--success); }
  .toast.error   { border-left:4px solid var(--red); }
  .toast-icon { width:28px; height:28px; border-radius:50%; display:flex; align-items:center; justify-content:center; font-size:.875rem; font-weight:700; flex-shrink:0; }
  .toast.success .toast-icon { background:var(--success-lt); color:var(--success); }
  .toast.error   .toast-icon { background:var(--error-lt);   color:var(--red); }
  .toast-title   { font-weight:700; font-size:.9rem; margin-bottom:.15rem; }
  .toast-msg     { font-size:.8rem; color:var(--g600); }

  /* ── Responsive ── */
  @media (max-width:768px) {
    .container   { padding:1rem; }
    .title       { font-size:2.25rem; }
    .upload-section { padding:1.5rem; }
    .dropzone    { padding:2.5rem 1rem; min-height:220px; }
    .file-header { flex-direction:column; gap:1rem; }
    .file-actions{ flex-direction:column; }
    .btn         { width:100%; }
    .toast { top:1rem; right:1rem; left:1rem; max-width:none; transform:translateY(-120px); }
    .toast.show  { transform:translateY(0); }
  }
</style>
</head>
<body>
<div class="container">

  <!-- Header -->
  <header class="header">
    <div class="logo">
      <i class="fas fa-cloud" style="font-size:2.5rem;color:#fff"></i>
    </div>
    <h1 class="title">Marya Vault</h1>
    <p class="subtitle">Lightning-fast file storage & streaming — powered by Cloudflare + Telegram.</p>
  </header>

  <!-- Upload -->
  <section class="upload-section">
    <label class="dropzone" id="dropzone" for="fileInput">
      <div class="upload-icon"><i class="fas fa-folder-open"></i></div>
      <h2 class="upload-title">Drop your files here</h2>
      <p class="upload-desc">or click to browse from your device</p>
      <div class="badges">
        <span class="badge"><i class="fas fa-image"></i>Images</span>
        <span class="badge"><i class="fas fa-video"></i>Videos</span>
        <span class="badge"><i class="fas fa-file"></i>Docs</span>
        <span class="badge"><i class="fas fa-music"></i>Audio</span>
        <span class="badge"><i class="fas fa-file-archive"></i>Archives</span>
      </div>
    </label>
    <input type="file" id="fileInput" class="file-input" multiple accept="*/*">
  </section>

  <!-- Results -->
  <section class="results" id="results">
    <div class="results-header">
      <h2 class="results-title">Your Files</h2>
      <div class="results-count" id="filesCount">0 files</div>
    </div>
    <div id="filesList"></div>
  </section>
</div>

<div id="toast" class="toast"></div>

<script>
'use strict';
let fileCounter = 0;

document.addEventListener('DOMContentLoaded', () => {
  const dropzone  = document.getElementById('dropzone');
  const fileInput = document.getElementById('fileInput');
  const results   = document.getElementById('results');
  const filesList = document.getElementById('filesList');
  const filesCount= document.getElementById('filesCount');

  // ── Drag & Drop ────────────────────────────────────────────────────────────
  const prevent = e => { e.preventDefault(); e.stopPropagation(); };
  ['dragenter','dragover','dragleave','drop'].forEach(ev => {
    dropzone.addEventListener(ev, prevent);
    document.body.addEventListener(ev, prevent);
  });
  ['dragenter','dragover'].forEach(ev => dropzone.addEventListener(ev, () => dropzone.classList.add('dragover')));
  ['dragleave','drop']    .forEach(ev => dropzone.addEventListener(ev, () => dropzone.classList.remove('dragover')));

  dropzone.addEventListener('drop', e => {
    const files = e.dataTransfer.files;
    if (files.length) handleFiles(Array.from(files));
  });

  fileInput.addEventListener('change', e => {
    if (e.target.files.length) {
      handleFiles(Array.from(e.target.files));
      fileInput.value = '';
    }
  });

  // ── Handle Files ───────────────────────────────────────────────────────────
  function handleFiles(files) {
    results.classList.add('show');
    fileCounter += files.length;
    filesCount.textContent = `${fileCounter} file${fileCounter !== 1 ? 's' : ''}`;
    files.forEach(uploadFile);
  }

  // ── Upload ─────────────────────────────────────────────────────────────────
  function uploadFile(file) {
    const MAX = 140 * 1024 * 1024; // 140MB — matches upload.js (7 KV × 20MB)
    if (file.size > MAX) {
      showToast(`"${file.name}" too large (max 140MB)`, 'error');
      fileCounter--;
      filesCount.textContent = `${fileCounter} file${fileCounter !== 1 ? 's' : ''}`;
      return;
    }

    const item     = createFileItem(file);
    filesList.appendChild(item);

    const fillEl   = item.querySelector('.progress-fill');
    const textEl   = item.querySelector('.progress-text');
    const statusEl = item.querySelector('.file-status');
    const actionsEl= item.querySelector('.file-actions');
    const urlsEl   = item.querySelector('.url-boxes');

    const fd  = new FormData();
    fd.append('file', file);

    const xhr = new XMLHttpRequest();

    xhr.upload.addEventListener('progress', e => {
      if (e.lengthComputable) {
        const pct = Math.round((e.loaded / e.total) * 100);
        fillEl.style.width  = pct + '%';
        textEl.textContent  = `${pct}% uploaded`;
        statusEl.textContent= 'Uploading…';
      }
    });

    xhr.addEventListener('load', () => {
      if (xhr.status === 200) {
        try {
          const res = JSON.parse(xhr.responseText);

          if (!res.success) throw new Error(res.error || 'Upload failed');

          // ✅ NEW response structure: res.urls.stream / res.urls.download / res.urls.hls
          const streamUrl   = res.urls?.stream;
          const downloadUrl = res.urls?.download;
          const hlsUrl      = res.urls?.hls;

          if (!streamUrl) throw new Error('Server returned no URL');

          fillEl.style.width  = '100%';
          textEl.textContent  = '100% complete';
          statusEl.textContent= '✓ Upload Complete';
          statusEl.className  = 'file-status done';

          // ── URL boxes ──────────────────────────────────────────────────────
          urlsEl.innerHTML = buildUrlBox('Stream', streamUrl)
            + (downloadUrl ? buildUrlBox('Download', downloadUrl) : '')
            + (hlsUrl      ? buildUrlBox('HLS', hlsUrl)           : '');

          // ── Action buttons ─────────────────────────────────────────────────
          actionsEl.innerHTML =
            `<a href="${streamUrl}" target="_blank" class="btn btn-primary">
               <i class="fas fa-play"></i> Stream
             </a>
             <a href="${downloadUrl}" class="btn btn-success">
               <i class="fas fa-download"></i> Download
             </a>`
            + (hlsUrl
              ? `<a href="${hlsUrl}" target="_blank" class="btn btn-purple">
                   <i class="fas fa-film"></i> HLS
                 </a>`
              : '')
            + `<button onclick="copyText('${streamUrl}')" class="btn btn-outline">
                 <i class="fas fa-copy"></i> Copy Link
               </button>`;

          showToast(`${file.name} uploaded!`, 'success');

        } catch (e) {
          setFailed(statusEl, fillEl, e.message);
          showToast(`Failed: ${file.name}`, 'error');
        }
      } else {
        setFailed(statusEl, fillEl, `HTTP ${xhr.status}`);
        showToast(`Upload failed: ${file.name}`, 'error');
      }
    });

    xhr.addEventListener('error', () => {
      setFailed(statusEl, fillEl, 'Network error');
      showToast(`Network error: ${file.name}`, 'error');
    });

    // ── Send to /btfstorage/upload  (change path to match your pages function)
    xhr.open('POST', '/btfstorage/upload');
    xhr.send(fd);
  }

  // ── DOM helpers ────────────────────────────────────────────────────────────
  function createFileItem(file) {
    const el = document.createElement('div');
    el.className = 'file-item';
    el.innerHTML = `
      <div class="file-header">
        <div class="file-icon-wrap">${getIcon(file.name)}</div>
        <div style="flex:1;min-width:0">
          <div class="file-name">${escHtml(file.name)}</div>
          <div class="file-meta">${fmtBytes(file.size)} · <span class="file-status uploading">Preparing…</span></div>
        </div>
      </div>
      <div class="progress-wrap">
        <div class="progress-bar"><div class="progress-fill"></div></div>
        <div class="progress-text">0% uploaded</div>
      </div>
      <div class="url-boxes" style="margin-bottom:.5rem"></div>
      <div class="file-actions"></div>`;
    return el;
  }

  function buildUrlBox(label, url) {
    const safe = escHtml(url);
    return `
      <div class="url-box">
        <span class="url-label">${label}</span>
        <span class="url-text" title="${safe}">${safe}</span>
        <button class="url-copy" onclick="copyText('${safe}')" title="Copy">
          <i class="fas fa-copy"></i>
        </button>
      </div>`;
  }

  function setFailed(statusEl, fillEl, msg) {
    statusEl.textContent = `✗ ${msg}`;
    statusEl.className   = 'file-status failed';
    fillEl.style.background = 'var(--red)';
  }

  function getIcon(name) {
    const ext = name.split('.').pop()?.toLowerCase() || '';
    const map = {
      jpg:'fa-image', jpeg:'fa-image', png:'fa-image', gif:'fa-image', webp:'fa-image', svg:'fa-palette',
      mp4:'fa-video', mov:'fa-video',  avi:'fa-video', mkv:'fa-video', webm:'fa-film',
      mp3:'fa-music', wav:'fa-music',  flac:'fa-music', aac:'fa-music', m4a:'fa-headphones',
      pdf:'fa-file-pdf', doc:'fa-file-word', docx:'fa-file-word', txt:'fa-file-alt',
      zip:'fa-file-archive', rar:'fa-file-archive', '7z':'fa-file-archive',
      js:'fa-code', html:'fa-globe', css:'fa-palette', py:'fa-code',
    };
    return `<i class="fas ${map[ext] || 'fa-file'}"></i>`;
  }

  function fmtBytes(b) {
    if (!b) return '0 B';
    const k = 1024, sizes = ['B','KB','MB','GB'];
    const i = Math.floor(Math.log(b) / Math.log(k));
    return (b / Math.pow(k, i)).toFixed(2) + ' ' + sizes[i];
  }

  function escHtml(str) {
    return str.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  }

  // ── Toast ──────────────────────────────────────────────────────────────────
  let toastTimer;
  function showToast(msg, type = 'success') {
    const t    = document.getElementById('toast');
    const icon = type === 'success' ? 'fa-check' : 'fa-times';
    t.innerHTML = `
      <div class="toast-icon"><i class="fas ${icon}"></i></div>
      <div><div class="toast-title">${type === 'success' ? 'Success' : 'Error'}</div>
           <div class="toast-msg">${escHtml(msg)}</div></div>`;
    t.className = `toast ${type} show`;
    clearTimeout(toastTimer);
    toastTimer  = setTimeout(() => t.classList.remove('show'), 5000);
  }
});

// ── Global copy helper ─────────────────────────────────────────────────────────
window.copyText = function(text) {
  (navigator.clipboard?.writeText(text) ?? Promise.reject())
    .then(() => { /* toast from inside */ })
    .catch(() => {
      const ta = Object.assign(document.createElement('textarea'),
        { value: text, style: 'position:fixed;left:-9999px;top:-9999px' });
      document.body.appendChild(ta);
      ta.select();
      try { document.execCommand('copy'); } catch(_) {}
      document.body.removeChild(ta);
    })
    .finally(() => {
      // show toast — defined inside DOMContentLoaded but we need it globally
      const t = document.getElementById('toast');
      t.innerHTML = `<div class="toast-icon"><i class="fas fa-check"></i></div>
        <div><div class="toast-title">Copied!</div><div class="toast-msg">Link copied to clipboard</div></div>`;
      t.className = 'toast success show';
      setTimeout(() => t.classList.remove('show'), 3000);
    });
};
</script>
</body>
</html>
