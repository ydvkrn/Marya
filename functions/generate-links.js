// STYLE LINK GENERATOR
// Creates both streaming and download links

export async function onRequest(context) {
  const { request, env } = context;
  
  if (request.method !== 'POST') {
    return new Response('Method not allowed', { status: 405 });
  }

  try {
    const { fileId, filename, telegramFileId } = await request.json();
    
    if (!fileId || !telegramFileId) {
      return new Response('Missing required data', { status: 400 });
    }

    // Create hash (safone.co style)
    const hashData = `${telegramFileId}|${filename || 'file'}`;
    const hash = Buffer.from(hashData).toString('base64')
      .replace(/+/g, '-')
      .replace(///g, '_')
      .replace(/=/g, '');

    const baseUrl = new URL(request.url).origin;
    
    // Generate links (exactly like safone.co)
    const streamLink = `${baseUrl}/stream/${hash}`;
    const downloadLink = `${baseUrl}/download/${fileId}?hash=${hash}`;

    console.log('🔗 Generated links:', { streamLink, downloadLink });

    return new Response(JSON.stringify({
      success: true,
      file: {
        id: fileId,
        filename: filename,
        hash: hash
      },
      links: {
        stream: streamLink,
        download: downloadLink
      }
    }), {
      headers: { 'Content-Type': 'application/json' }
    });

  } catch (error) {
    console.error('❌ Link generation error:', error);
    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' }
    });
  }
}