/**
 * Cleanup Script v2 - IMPROVED
 * Remove Old Episodes from Firestore
 * 
 * This version checks multiple factors, not just status!
 */

const admin = require('firebase-admin');

// Initialize Firebase
const serviceAccount = {
  projectId: process.env.FIREBASE_PROJECT_ID,
  privateKey: process.env.FIREBASE_PRIVATE_KEY.replace(/\\n/g, '\n'),
  clientEmail: process.env.FIREBASE_CLIENT_EMAIL,
};

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

const db = admin.firestore();

// Only keep episodes from the last 7 days
const RECENCY_WINDOW_DAYS = 7;

// List of known OLD anime IDs that should be deleted
const OLD_ANIME_IDS = [
  22729,  // Aldnoah.Zero (2014)
  32281,  // Kimi no Na wa. (2016)
  235,    // Meitantei Conan (1996)
  433,    // Kumo no Mukou, Yakusoku no Basho (2004)
  9760,   // Hoshi wo Ou Kodomo (2011)
  60666,  // Aldnoah.Zero (Re+)
  59843,  // Aldnoah.Zero: Ame no Danshou
  54863,  // Trigun Stargaze (old)
];

/**
 * Check if an anime should be kept
 */
function shouldKeepAnime(data, animeId) {
  const now = new Date();
  const cutoffDate = new Date(now.getTime() - (RECENCY_WINDOW_DAYS * 24 * 60 * 60 * 1000));
  
  // ❌ DELETE if in the blacklist of old anime
  if (OLD_ANIME_IDS.includes(parseInt(animeId))) {
    return false;
  }
  
  // ✅ KEEP if episode aired very recently (last 7 days)
  if (data.episodeAiredDate) {
    const epDate = new Date(data.episodeAiredDate);
    if (epDate >= cutoffDate) {
      return true;
    }
  }
  
  // ✅ KEEP if added to database recently
  if (data.episodeAddedAt) {
    const addedDate = new Date(data.episodeAddedAt);
    if (addedDate >= cutoffDate) {
      return true;
    }
  }
  
  // ✅ KEEP if anime started airing in last 30 days (new series)
  if (data.airedDate) {
    const animeStartDate = new Date(data.airedDate);
    const newSeriesCutoff = new Date(now.getTime() - (30 * 24 * 60 * 60 * 1000));
    if (animeStartDate >= newSeriesCutoff) {
      return true;
    }
  }
  
  // ✅ KEEP if status explicitly says "Currently Airing"
  const status = data.status?.toLowerCase() || '';
  if (status === 'currently airing') {
    return true;
  }
  
  // ❌ DELETE if status is "Finished Airing"
  if (status === 'finished airing') {
    return false;
  }
  
  // ✅ KEEP if last updated very recently (in last 2 days)
  if (data.lastUpdated) {
    const updatedDate = new Date(data.lastUpdated);
    const recentUpdateWindow = new Date(now.getTime() - (2 * 24 * 60 * 60 * 1000));
    if (updatedDate >= recentUpdateWindow) {
      return true;
    }
  }
  
  // ❌ Default: DELETE if none of the above conditions are met
  return false;
}

/**
 * Clean up old episodes
 */
async function cleanupOldEpisodes() {
  console.log('🧹 Starting IMPROVED cleanup of old episodes...');
  console.log(`📅 Cutoff: Episodes older than ${RECENCY_WINDOW_DAYS} days`);
  console.log(`🚫 Blacklist: ${OLD_ANIME_IDS.length} known old anime IDs\n`);
  
  try {
    const episodesRef = db.collection('episodes');
    const snapshot = await episodesRef.get();
    
    console.log(`📊 Total episodes in database: ${snapshot.size}\n`);
    
    const batch = db.batch();
    let deleteCount = 0;
    let keepCount = 0;
    
    snapshot.forEach((doc) => {
      const data = doc.data();
      const animeId = doc.id;
      const title = data.title || 'Unknown';
      const status = data.status || 'Unknown';
      
      if (shouldKeepAnime(data, animeId)) {
        console.log(`  ✅ KEEP: ${title} (ID: ${animeId}, status: ${status})`);
        keepCount++;
      } else {
        console.log(`  🗑️  DELETE: ${title} (ID: ${animeId}, status: ${status})`);
        batch.delete(doc.ref);
        deleteCount++;
      }
    });
    
    if (deleteCount > 0) {
      console.log(`\n🔥 Committing deletion of ${deleteCount} old episodes...`);
      await batch.commit();
      console.log('✨ Cleanup completed!');
    } else {
      console.log('\n✨ No old episodes to delete!');
    }
    
    console.log(`\n📊 Summary:`);
    console.log(`   Kept: ${keepCount} episodes`);
    console.log(`   Deleted: ${deleteCount} episodes`);
    
  } catch (error) {
    console.error('❌ Error during cleanup:', error.message);
    throw error;
  }
}

/**
 * Main execution
 */
async function main() {
  console.log('🚀 Starting Episodes Cleanup Script v2 (IMPROVED)...');
  console.log(`⏰ Time: ${new Date().toISOString()}\n`);
  
  try {
    await cleanupOldEpisodes();
    console.log('\n✅ Cleanup script completed successfully!');
    process.exit(0);
  } catch (error) {
    console.error('💥 Fatal error:', error.message);
    process.exit(1);
  }
}

// Run the script
main();
