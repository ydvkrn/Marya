import { CACHE_SECS } from '../../_config.js';

export async function onRequest({params,request,env}) {
  const slug=params.slug; const kv=env.FILES_KV;
  const tgURL = await kv.get(slug);
  if(!tgURL) return new Response('404',{status:404});

  const range = request.headers.get('Range');
  const r = await fetch(tgURL,{headers: range?{Range:range}:{}});

  /* prepare headers */
  const h=new Headers(r.headers);
  h.set('Access-Control-Allow-Origin','*');
  h.set('Cache-Control',`public,max-age=${CACHE_SECS},immutable`);
  h.set('Expires',new Date(Date.now()+CACHE_SECS*1000).toUTCString());
  if(request.url.includes('dl=1')) h.set('Content-Disposition','attachment');
  if(!h.has('Accept-Ranges')) h.set('Accept-Ranges','bytes');

  return new Response(r.body,{status:r.status,headers:h});
}
