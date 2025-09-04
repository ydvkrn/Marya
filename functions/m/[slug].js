import { CACHE_SECS } from '../_config.js';

export async function onRequest({ params, request, env }) {
  try {
    const slug = params.slug;
    const directUrl = await env.FILES_KV.get(slug);

    if (!directUrl) {
      return new Response('File not found', { status: 404 });
    }

    const response = await fetch(directUrl);
    
    if (!response.ok) {
      return new Response('File not accessible', { status: 404 });
    }

    const headers = new Headers(response.headers);
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Cache-Control', `public, max-age=${CACHE_SECS}`);
    
    if (request.url.includes('dl=1')) {
      headers.set('Content-Disposition', 'attachment');
    }

    return new Response(response.body, {
      status: response.status,
      headers
    });

  } catch (error) {
    console.error('Serve error:', error);
    return new Response('Server error', { status: 500 });
  }
}
