import { BOT_TOKEN, CHANNEL_ID } from './config.js';

export async function onRequest(context) {
  const { request, env } = context;
  
  // CORS headers
  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type'
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  if (request.method !== 'POST') {
    return new Response(JSON.stringify({ success: false, error: 'Method not allowed' }), {
      status: 405,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }

  try {
    const formData = await request.formData();
    const file = formData.get('file');

    if (!file || file.size === 0) {
      return new Response(JSON.stringify({ success: false, error: 'No file provided' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    // Size limit: 2GB
    if (file.size > 2147483648) {
      return new Response(JSON.stringify({ success: false, error: 'File too large (max 2GB)' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    // Upload to Telegram
    const telegramForm = new FormData();
    telegramForm.append('chat_id', CHANNEL_ID);
    telegramForm.append('document', file, file.name);

    const telegramRes = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/sendDocument`, {
      method: 'POST',
      body: telegramForm
    });

    const telegramData = await telegramRes.json();

    if (!telegramData.ok) {
      return new Response(JSON.stringify({ success: false, error: telegramData.description }), {
        status: 500,
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    // Get file URL
    const fileId = telegramData.result.document.file_id;
    const fileRes = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/getFile?file_id=${fileId}`);
    const fileData = await fileRes.json();

    if (!fileData.ok) {
      return new Response(JSON.stringify({ success: false, error: 'Failed to get file URL' }), {
        status: 500,
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    const telegramURL = `https://api.telegram.org/file/bot${BOT_TOKEN}/${fileData.result.file_path}`;

    // Generate unique slug
    const slug = Date.now().toString(36) + Math.random().toString(36).substr(2);
    const extension = file.name.includes('.') ? file.name.split('.').pop() : '';
    const finalSlug = extension ? `${slug}.${extension}` : slug;

    // Store in KV
    await env.VAULT_KV.put(finalSlug, telegramURL, {
      metadata: {
        filename: file.name,
        size: file.size,
        type: file.type,
        uploaded: Date.now()
      }
    });

    // Generate URLs
    const baseURL = new URL(request.url).origin;
    const fileURL = `${baseURL}/f/${finalSlug}`;
    const downloadURL = `${baseURL}/f/${finalSlug}?dl=1`;

    return new Response(JSON.stringify({
      success: true,
      filename: file.name,
      size: file.size,
      type: file.type,
      url: fileURL,
      download: downloadURL,
      slug: finalSlug
    }), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (error) {
    return new Response(JSON.stringify({ success: false, error: error.message }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}
