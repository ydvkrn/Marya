export async function onRequest({ params, request, env }) {
  try {
    const slug = params.slug;
    const metadata = await env.FILES_KV.get(slug, { type: 'json' });
    const fileIdCode = metadata?.metadata?.fileIdCode || 'MSMfile0/old-fmt';
    
    const baseUrl = new URL(request.url).origin;
    const newUrl = `${baseUrl}/btf/${slug}/${fileIdCode}${request.url.includes('dl=1') ? '?dl=1' : ''}`;
    
    return Response.redirect(newUrl, 301);
  } catch (error) {
    return new Response(`Error: ${error.message}`, { status: 500 });
  }
}
