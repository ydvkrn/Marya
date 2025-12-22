// 🔥 functions/api/upload.js - EXACT MATCH YOUR id.js
export async function onRequestPost({ request, env }) {
    const formData = await request.formData();
    const file = formData.get('file');
    const filename = formData.get('filename');
    
    if (file.size > 500 * 1024 * 1024) {
        return Response.json({ error: 'Max 500MB' }, { status: 413 });
    }

    const fileId = `marya_${Date.now()}_${crypto.randomUUID().slice(0,8)}`;
    const chunks = await create20MBChunks(file, env.FILES_KV, filename);
    const telegramFileId = await uploadTelegram(file, env.BOT_TOKEN, env.CHANNEL_ID);
    
    const metadata = {
        filename, size: file.size, contentType: getMimeType(filename),
        telegramFileId, chunks, chunkSize: 20*1024*1024,
        uploadedAt: new Date().toISOString()
    };
    
    await env.FILES_KV.put(fileId, JSON.stringify(metadata));
    
    return Response.json({
        success: true,
        fileId,
        totalChunks: chunks.length,
        telegramFileId,
        botsUsed: 1
    });
}
