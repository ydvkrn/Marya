export async function onRequest(context) {
  const { request, env } = context;
  
  console.log('=== CHANNEL MONITOR WEBHOOK ===');
  
  if (request.method === 'POST') {
    try {
      const update = await request.json();
      console.log('Webhook update:', update);
      
      // ✅ Check if message is from your channel
      const MONITOR_CHANNEL_ID = env.MONITOR_CHANNEL_ID; // Your channel ID
      
      if (update.channel_post) {
        const message = update.channel_post;
        const chatId = message.chat.id;
        
        console.log('Channel post detected:', chatId);
        
        // ✅ Process only your channel
        if (chatId.toString() === MONITOR_CHANNEL_ID) {
          return await processChannelFile(message, env);
        }
      }
      
      return new Response('OK', { status: 200 });
      
    } catch (error) {
      console.error('Webhook error:', error);
      return new Response('Error', { status: 500 });
    }
  }
  
  return new Response('Method not allowed', { status: 405 });
}

// ✅ Process forwarded file from channel
async function processChannelFile(message, env) {
  const BOT_TOKEN = env.BOT_TOKEN;
  
  try {
    // ✅ Extract file information
    let fileInfo = null;
    let fileName = 'file';
    
    if (message.document) {
      fileInfo = message.document;
      fileName = fileInfo.file_name || 'document';
    } else if (message.video) {
      fileInfo = message.video;
      fileName = `video_${fileInfo.duration}s.mp4`;
    } else if (message.photo) {
      fileInfo = message.photo[message.photo.length - 1]; // Highest resolution
      fileName = `photo_${fileInfo.file_id.slice(0, 8)}.jpg`;
    } else if (message.audio) {
      fileInfo = message.audio;
      fileName = fileInfo.file_name || `audio_${fileInfo.duration}s.mp3`;
    }
    
    if (!fileInfo || !fileInfo.file_id) {
      console.log('No valid file found in message');
      return new Response('No file', { status: 200 });
    }
    
    console.log(`Processing file: ${fileName} (${fileInfo.file_size} bytes)`);
    
    // ✅ Check file size limit
    if (fileInfo.file_size > 175 * 1024 * 1024) { // 175MB limit
      console.log('File too large for KV system');
      return await sendChannelMessage(
        env,
        message.chat.id,
        `❌ File "${fileName}" is too large (${Math.round(fileInfo.file_size / 1024 / 1024)}MB). Maximum size is 175MB.`,
        message.message_id
      );
    }
    
    // ✅ Download file from Telegram
    const downloadResult = await downloadTelegramFile(fileInfo.file_id, fileName, env);
    
    if (!downloadResult.success) {
      return await sendChannelMessage(
        env,
        message.chat.id,
        `❌ Failed to process: ${downloadResult.error}`,
        message.message_id
      );
    }
    
    // ✅ Upload to Multi-KV system
    const uploadResult = await uploadToMultiKV(downloadResult.fileData, fileName, env);
    
    if (uploadResult.success) {
      // ✅ Send Cloudflare link back to channel
      const replyText = `✅ **File Processed Successfully!**\n\n` +
                       `📁 **Name:** ${fileName}\n` +
                       `💾 **Size:** ${Math.round(fileInfo.file_size / 1024 / 1024 * 100) / 100}MB\n` +
                       `🔗 **Cloudflare Link:** ${uploadResult.url}\n` +
                       `⬇️ **Direct Download:** ${uploadResult.download}\n\n` +
                       `🚀 **Powered by Marya Vault**`;
      
      return await sendChannelMessage(
        env,
        message.chat.id,
        replyText,
        message.message_id
      );
    } else {
      return await sendChannelMessage(
        env,
        message.chat.id,
        `❌ Upload failed: ${uploadResult.error}`,
        message.message_id
      );
    }
    
  } catch (error) {
    console.error('Process file error:', error);
    return await sendChannelMessage(
      env,
      message.chat.id,
      `❌ Processing error: ${error.message}`,
      message.message_id
    );
  }
}

// ✅ Download file from Telegram
async function downloadTelegramFile(fileId, fileName, env) {
  try {
    const BOT_TOKEN = env.BOT_TOKEN;
    
    // Get file info
    const getFileResponse = await fetch(
      `https://api.telegram.org/bot${BOT_TOKEN}/getFile?file_id=${encodeURIComponent(fileId)}`
    );
    
    if (!getFileResponse.ok) {
      throw new Error('Failed to get file info from Telegram');
    }
    
    const getFileData = await getFileResponse.json();
    
    if (!getFileData.ok || !getFileData.result?.file_path) {
      throw new Error('Invalid file info response');
    }
    
    // Download file
    const fileUrl = `https://api.telegram.org/file/bot${BOT_TOKEN}/${getFileData.result.file_path}`;
    const downloadResponse = await fetch(fileUrl);
    
    if (!downloadResponse.ok) {
      throw new Error('Failed to download file from Telegram');
    }
    
    const fileBuffer = await downloadResponse.arrayBuffer();
    const fileData = new File([fileBuffer], fileName, { 
      type: getContentType(fileName) 
    });
    
    return {
      success: true,
      fileData: fileData
    };
    
  } catch (error) {
    return {
      success: false,
      error: error.message
    };
  }
}

// ✅ Upload to Multi-KV system (reuse your existing function)
async function uploadToMultiKV(file, fileName, env) {
  try {
    // ✅ All KV namespaces
    const kvNamespaces = [
      { kv: env.FILES_KV, name: 'FILES_KV' },
      { kv: env.FILES_KV2, name: 'FILES_KV2' },
      { kv: env.FILES_KV3, name: 'FILES_KV3' },
      { kv: env.FILES_KV4, name: 'FILES_KV4' },
      { kv: env.FILES_KV5, name: 'FILES_KV5' },
      { kv: env.FILES_KV6, name: 'FILES_KV6' },
      { kv: env.FILES_KV7, name: 'FILES_KV7' }
    ].filter(item => item.kv);
    
    // Generate unique ID
    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).slice(2, 8);
    const fileId = `ch${timestamp}${random}`; // 'ch' prefix for channel files
    const extension = fileName.includes('.') ? fileName.slice(fileName.lastIndexOf('.')) : '';
    
    // ✅ Chunking logic (same as your upload function)
    const CHUNK_SIZE = 20 * 1024 * 1024;
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);
    
    if (totalChunks === 1) {
      // Single file upload
      await kvNamespaces[0].kv.put(fileId, 'direct_file', {
        metadata: {
          filename: fileName,
          size: file.size,
          contentType: file.type,
          extension: extension,
          uploadedAt: Date.now(),
          source: 'channel_forward',
          type: 'single'
        }
      });
    } else {
      // Multi-chunk upload logic here...
    }
    
    const baseUrl = 'https://marya-hosting.pages.dev'; // Your domain
    const customUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}`;
    const downloadUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}?dl=1`;
    
    return {
      success: true,
      url: customUrl,
      download: downloadUrl,
      id: fileId
    };
    
  } catch (error) {
    return {
      success: false,
      error: error.message
    };
  }
}

// ✅ Send message to channel
async function sendChannelMessage(env, chatId, text, replyToMessageId) {
  try {
    const BOT_TOKEN = env.BOT_TOKEN;
    
    const response = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        chat_id: chatId,
        text: text,
        reply_to_message_id: replyToMessageId,
        parse_mode: 'Markdown'
      })
    });
    
    return new Response('Message sent', { status: 200 });
    
  } catch (error) {
    console.error('Send message error:', error);
    return new Response('Send error', { status: 500 });
  }
}

// ✅ Get content type from filename
function getContentType(fileName) {
  const ext = fileName.split('.').pop()?.toLowerCase();
  const mimeMap = {
    'mp4': 'video/mp4',
    'mov': 'video/quicktime', 
    'avi': 'video/x-msvideo',
    'jpg': 'image/jpeg',
    'png': 'image/png',
    'pdf': 'application/pdf',
    'mp3': 'audio/mpeg'
  };
  return mimeMap[ext] || 'application/octet-stream';
}
