const pick  = document.getElementById('pick');
const zone  = document.getElementById('zone');
const urlIn = document.getElementById('u');
const urlBtn= document.getElementById('uBtn');
const load  = document.getElementById('L');
const out   = document.getElementById('O');
const alert = document.getElementById('alert');

zone.onclick = ()=>pick.click();
pick.onchange = e=>[...e.target.files].forEach(uploadFile);
['dragover','drop'].forEach(ev=>document.addEventListener(ev,e=>e.preventDefault()));
document.addEventListener('drop',e=>[...e.dataTransfer.files].forEach(uploadFile));
urlBtn.onclick = ()=> urlIn.value.trim() && uploadURL(urlIn.value.trim());

const showLoad = s=>load.classList.toggle('show',s);
const fmt = b=>{const u=['B','KB','MB','GB'];let i=0;while(b>1024&&i<3){b/=1024;i++;}return b.toFixed(1)+' '+u[i];};
const error = t=>{alert.textContent=t;alert.style.display='block';setTimeout(()=>alert.style.display='none',4e3);};
const card  = d=>{
  out.insertAdjacentHTML('afterbegin',`<div class="res">
  <b>${d.filename}</b> • ${fmt(d.size)}<br>
  <a href="${d.view_url}" target="_blank">View</a> | <a href="${d.download_url}">Download</a>
  </div>`);
};

async function uploadFile(file){
  if(file.size>25*1024*1024) return error('Max 25 MB');
  const fd=new FormData();fd.append('file',file); showLoad(true);
  try{
    const r=await fetch('/upload',{method:'POST',body:fd});
    const j=await r.json(); j.success?card(j):error(j.error);
  }catch(e){error(e.message);} showLoad(false);
}
async function uploadURL(u){
  urlBtn.disabled=true;showLoad(true);
  try{
    const r=await fetch(`/upload?src=${encodeURIComponent(u)}`);
    const j=await r.json(); j.success?card(j):error(j.error);
  }catch(e){error(e.message);} showLoad(false);urlBtn.disabled=false;urlIn.value='';
}
