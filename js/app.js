const $ = s=>document.querySelector(s);
const zone=$('#zone'), pick=$('#pick'), url=$('#u'), uBtn=$('#uBtn'), load=$('#L'), prog=$('#p'), out=$('#O'), alert=$('#alert');
zone.onclick = ()=>pick.click();
pick.onchange=e=>[...e.target.files].forEach(send);
['dragover','drop'].forEach(ev=>document.addEventListener(ev,e=>e.preventDefault()));
document.addEventListener('drop',e=>[...e.dataTransfer.files].forEach(send));
uBtn.onclick=()=>{ if(url.value.trim()) sendURL(url.value.trim()); };

function fmt(b){const u=['B','KB','MB'];let i=0;while(b>1024&&i<2){b/=1024;i++;}return b.toFixed(1)+' '+u[i];}
function showErr(t){alert.textContent=t;alert.style.display='block';setTimeout(()=>alert.style.display='none',4e3);}
function card(d){out.insertAdjacentHTML('afterbegin',`<div class="res"><b>${d.filename}</b> • ${fmt(d.size)}<br><a href="${d.view_url}" target="_blank">View</a> | <a href="${d.download_url}">Download</a></div>`);}
const showLoad=s=>load.classList.toggle('show',s);
async function send(f){
  if(f.size>26214400) return showErr('Max 25 MB');
  const fd=new FormData();fd.append('file',f); showLoad(true);
  try{const r=await fetch('/upload',{method:'POST',body:fd});const j=await r.json();j.success?card(j):showErr(j.error);}
  catch(e){showErr(e.message);} showLoad(false);
}
async function sendURL(u){
  uBtn.disabled=true;showLoad(true);
  try{const r=await fetch(`/upload?src=${encodeURIComponent(u)}`);const j=await r.json();j.success?card(j):showErr(j.error);}
  catch(e){showErr(e.message);} showLoad(false);uBtn.disabled=false;url.value='';
}
