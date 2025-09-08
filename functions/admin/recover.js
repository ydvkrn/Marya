export async function onRequest(context) {
  const { env } = context;
  
  try {
    const FILES_KV = env.FILES_KV;
    
    // List all existing files
    const listResult = await FILES_KV.list();
    const recoveredFiles = [];
    
    for (const key of listResult.keys) {
      try {
        const value = await FILES_KV.get(key.name);
        if (value) {
          // Check if it's old format or new format
          let metadata;
          try {
            metadata = JSON.parse(value);
          } catch {
            // Old format - direct URL
            metadata = {
              filename: key.name,
              directUrl: value,
              type: 'legacy',
              recovered: true
            };
          }
          
          recoveredFiles.push({
            id: key.name,
            metadata: metadata,
            working: true
          });
        }
      } catch (error) {
        recoveredFiles.push({
          id: key.name,
          error: error.message,
          working: false
        });
      }
    }
    
    return new Response(JSON.stringify({
      success: true,
      total: recoveredFiles.length,
      working: recoveredFiles.filter(f => f.working).length,
      broken: recoveredFiles.filter(f => !f.working).length,
      files: recoveredFiles
    }), {
      headers: { 'Content-Type': 'application/json' }
    });
    
  } catch (error) {
    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), { status: 500 });
  }
}
