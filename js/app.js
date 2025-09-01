const $ = s=>document.querySelector(s);
const zone=$('#zone'), pick=$('#pick'), url=$('#u'), uBtn=$('#uBtn'),
      load=$('#L'), out=$('#O'), alert=$('#alert');

zone.onclick = ()=>pick.click();
pick.onchange = e => [...e.target.files].forEach(sendFile);
['dragover','drop'].forEach(ev=>document.addEventListener(ev,e=>e.preventDefault()));
document.addEventListener('drop',e=>[...e.dataTransfer.files].forEach(sendFile));
uBtn.onclick = ()=> url.value.trim() && sendURL(url.value.trim());

const showLoad = s => load.classList.toggle('show',s);
const fmt = b => {const u=['B','KB','MB'];let i=0;while(b>1024&&i<2){b/=1024;i++;}return b.toFixed(1)+' '+u[i];};
const err = t => {alert.textContent=t;alert.style.display='block';setTimeout(()=>alert.style.display='none',4000);};

function addCard(d){
  out.insertAdjacentHTML('afterbegin',`<div class="res">
  <b>${d.filename}</b> • ${fmt(d.size)}<br>
  <a href="${d.view_url}" target="_blank">View</a> | <a href="${d.download_url}">Download</a>
  </div>`);
}

/* ---------- upload helpers ---------- */
async function sendFile(file){
  if(file.size>25*1024*1024) return err('Max 25 MB');
  const fd=new FormData();fd.append('file',file); showLoad(true);
  try{
    const r = await fetch('/upload',{method:'POST',body:fd});
    const j = await r.json(); j.success ? addCard(j) : err(j.error);
  }catch(e){err(e.message);} showLoad(false);
}
async function sendURL(s){
  uBtn.disabled=true;showLoad(true);
  try{
    const r = await fetch(`/upload?src=${encodeURIComponent(s)}`);
    const j = await r.json(); j.success ? addCard(j) : err(j.error);
  }catch(e){err(e.message);} showLoad(false); uBtn.disabled=false; url.value='';
}
