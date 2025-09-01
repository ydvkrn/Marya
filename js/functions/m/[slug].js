import { CACHE_YRS } from '../../_config.js';

const SECS = CACHE_YRS*31536000;

export async function onRequest({params,env,request}) {
  const kv  = env.FILES_KV, slug = params.slug;
  let link  = await kv.get(slug);          // value holds Telegram CDN URL

  if(!link){                               // never stored?
    const meta = await kv.getWithMetadata(slug,'text');
    if(!meta.value) return new Response('404',{status:404});
    link = meta.value;                     // Telegram URL already saved
  }

  // proxy with range support
  const range = request.headers.get('Range');
  const r = await fetch(link,{headers: range? {Range:range}:{}});
  if(!r.ok) return new Response('TG fetch fail',{status:502});

  const h = new Headers(r.headers);
  h.set('Cache-Control',`public, max-age=${SECS}, immutable`);
  h.set('Expires',new Date(Date.now()+SECS*1000).toUTCString());
  h.set('Access-Control-Allow-Origin','*');
  if(request.url.includes('dl=1'))
      h.set('Content-Disposition','attachment');

  return new Response(r.body,{status:r.status,headers:h});
}
