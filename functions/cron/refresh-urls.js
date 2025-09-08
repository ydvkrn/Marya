export async function onRequest(context) {
  const { env } = context;

  console.log('=== BACKGROUND URL REFRESH CRON ===');

  try {
    // ✅ Refresh URLs that are about to expire (proactive refresh)
    const kvNamespaces = {
      FILES_KV: env.FILES_KV,
      FILES_KV2: env.FILES_KV2,
      FILES_KV3: env.FILES_KV3,
      FILES_KV4: env.FILES_KV4,
      FILES_KV5: env.FILES_KV5,
      FILES_KV6: env.FILES_KV6,
      FILES_KV7: env.FILES_KV7
    };

    let refreshed = 0;

    for (const [kvName, kvNamespace] of Object.entries(kvNamespaces)) {
      if (!kvNamespace) continue;

      try {
        // ✅ List all keys
        const listResult = await kvNamespace.list();
        
        for (const key of listResult.keys) {
          try {
            // ✅ Check if URL needs refresh (older than 20 hours)
            const metadata = key.metadata;
            if (metadata && metadata.lastRefreshed) {
              const hoursSinceRefresh = (Date.now() - metadata.lastRefreshed) / (1000 * 60 * 60);
              
              if (hoursSinceRefresh > 20) { // Refresh before 24 hour expiry
                console.log(`🔄 Proactive refresh needed for ${key.name}`);
                
                // ✅ Background refresh
                if (metadata.telegramFileId) {
                  await proactiveRefreshUrl(kvNamespace, key.name, metadata, env.BOT_TOKEN);
                  refreshed++;
                }
              }
            }
          } catch (keyError) {
            console.log(`Skip key ${key.name}:`, keyError.message);
          }
        }
      } catch (kvError) {
        console.log(`Skip KV ${kvName}:`, kvError.message);
      }
    }

    console.log(`✅ Proactive refresh completed: ${refreshed} URLs refreshed`);

    return new Response(JSON.stringify({
      success: true,
      refreshed: refreshed,
      timestamp: Date.now()
    }), {
      headers: { 'Content-Type': 'application/json' }
    });

  } catch (error) {
    console.error('❌ Cron refresh error:', error);
    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), { status: 500 });
  }
}

// ✅ Proactive URL refresh
async function proactiveRefreshUrl(kvNamespace, key, metadata, botToken) {
  try {
    if (!botToken || !metadata.telegramFileId) return;

    const getFileResponse = await fetch(
      `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(metadata.telegramFileId)}`
    );

    if (!getFileResponse.ok) return;

    const getFileData = await getFileResponse.json();
    if (!getFileData.ok || !getFileData.result?.file_path) return;

    const freshUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

    // ✅ Update with fresh URL
    const updatedMetadata = {
      ...metadata,
      directUrl: freshUrl,
      lastRefreshed: Date.now(),
      refreshCount: (metadata.refreshCount || 0) + 1
    };

    if (key.includes('_chunk_')) {
      await kvNamespace.put(key, JSON.stringify(updatedMetadata));
    } else {
      await kvNamespace.put(key, freshUrl, { metadata: updatedMetadata });
    }

    console.log(`✅ Proactively refreshed: ${key}`);

  } catch (error) {
    console.error(`❌ Proactive refresh failed for ${key}:`, error);
  }
}
