const zone = document.getElementById('zone');
const picker = document.getElementById('picker');
const urlIn = document.getElementById('url');
const urlBtn = document.getElementById('urlBtn');
const loadBox = document.getElementById('load');
const prog   = document.getElementById('prog');
const out    = document.getElementById('out');

zone.onclick = () => picker.click();
picker.onchange = e => [...e.target.files].forEach(sendFile);

['dragover','drop'].forEach(ev=>document.addEventListener(ev,e=>e.preventDefault()));
document.addEventListener('drop',e=>[...e.dataTransfer.files].forEach(sendFile));

urlBtn.onclick = () => {
  const val=urlIn.value.trim();
  if(!val) return;
  toggleLoad(true);
  fetch(`/upload?src=${encodeURIComponent(val)}`)
   .then(r=>r.json()).then(show).catch(er=>show({success:false,error:er.message}))
   .finally(()=>{toggleLoad(false);urlIn.value='';});
};

function sendFile(f){
  if(f.size>25*1024*1024) return show({success:false,error:'Max 25 MB'});
  const fd=new FormData();fd.append('file',f);
  toggleLoad(true);
  fetch('/upload',{method:'POST',body:fd})
   .then(r=>r.json()).then(show).catch(er=>show({success:false,error:er.message}))
   .finally(()=>toggleLoad(false));
}

function toggleLoad(s){loadBox.classList.toggle('show',s);prog.textContent='0 %';}
function fmt(x){const u=['B','KB','MB'];let i=0;while(x>1024&&i<2){x/=1024;i++;}return x.toFixed(1)+' '+u[i];}

function show(d){
  const now=new Date().toLocaleString();
  if(!d.success){
    out.insertAdjacentHTML('afterbegin',`<div class="result error"><h3><span class="material-icons">error</span>Error</h3><p>${d.error}</p><small>${now}</small></div>`);
    return;
  }
  out.insertAdjacentHTML('afterbegin',`<div class="result"><h3><span class="material-icons">check_circle</span>Uploaded</h3>
   <p><b>${d.filename}</b> • ${fmt(d.size)}</p>
   <p><a href="${d.view_url}" target="_blank">View</a> &nbsp;|&nbsp; <a href="${d.download_url}">Download</a></p>
   <small>${now}</small></div>`);
}
