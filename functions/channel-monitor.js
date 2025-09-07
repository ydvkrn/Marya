export async function onRequest(context) {
  const { request, env } = context;
  
  console.log('=== TELEGRAM WEBHOOK ===');
  console.log('Method:', request.method);

  if (request.method !== 'POST') {
    return new Response('OK', { status: 200 });
  }

  try {
    const update = await request.json();
    console.log('Update received:', JSON.stringify(update, null, 2));

    const BOT_TOKEN = env.BOT_TOKEN;
    const CHANNEL_ID = env.MONITOR_CHANNEL_ID;

    // Check for channel post
    if (update.channel_post) {
      const message = update.channel_post;
      const chatId = message.chat.id;
      
      console.log('Channel post detected in chat:', chatId);
      console.log('Monitor channel ID:', CHANNEL_ID);
      
      if (chatId.toString() === CHANNEL_ID.toString()) {
        console.log('✅ Message from monitored channel');
        
        // Send simple reply
        await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: chatId,
            text: `✅ Bot is working! Message detected: ${message.text || 'File received'}`,
            reply_to_message_id: message.message_id
          })
        });
      }
    }

    return new Response('OK', { status: 200 });

  } catch (error) {
    console.error('Error:', error);
    return new Response('OK', { status: 200 });
  }
}
