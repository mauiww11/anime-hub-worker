/**
 * Cleanup Script - Remove Old Episodes from Firestore
 * 
 * This script removes old/finished anime from the episodes collection.
 * Run this ONCE before deploying the fixed fetch.js
 * 
 * Requirements:
 * - Firebase Secrets: FIREBASE_PROJECT_ID, FIREBASE_PRIVATE_KEY, FIREBASE_CLIENT_EMAIL
 * - npm packages: firebase-admin
 */

const admin = require('firebase-admin');

// Initialize Firebase with service account from GitHub Secrets
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

/**
 * Check if an anime should be kept based on its data
 */
function shouldKeepAnime(data) {
  const now = new Date();
  const cutoffDate = new Date(now.getTime() - (RECENCY_WINDOW_DAYS * 24 * 60 * 60 * 1000));
  
  // Keep if currently airing
  const status = data.status?.toLowerCase() || '';
  if (status === 'currently airing' || status === 'airing') {
    return true;
  }
  
  // Keep if episode aired recently
  if (data.episodeAiredDate) {
    const epDate = new Date(data.episodeAiredDate);
    if (epDate >= cutoffDate) {
      return true;
    }
  }
  
  // Keep if anime started airing recently
  if (data.airedDate) {
    const animeStartDate = new Date(data.airedDate);
    if (animeStartDate >= cutoffDate) {
      return true;
    }
  }
  
  // Keep if added to database recently
  if (data.episodeAddedAt) {
    const addedDate = new Date(data.episodeAddedAt);
    if (addedDate >= cutoffDate) {
      return true;
    }
  }
  
  // Keep if last updated recently (in case of metadata refresh)
  if (data.lastUpdated) {
    const updatedDate = new Date(data.lastUpdated);
    const recentUpdateWindow = new Date(now.getTime() - (2 * 24 * 60 * 60 * 1000)); // 2 days
    if (updatedDate >= recentUpdateWindow) {
      return true;
    }
  }
  
  return false;
}

/**
 * Clean up old episodes from Firestore
 */
async function cleanupOldEpisodes() {
  console.log('🧹 Starting cleanup of old episodes...');
  console.log(`📅 Cutoff: Episodes older than ${RECENCY_WINDOW_DAYS} days`);
  
  try {
    const episodesRef = db.collection('episodes');
    const snapshot = await episodesRef.get();
    
    console.log(`📊 Total episodes in database: ${snapshot.size}`);
    
    const batch = db.batch();
    let deleteCount = 0;
    let keepCount = 0;
    
    snapshot.forEach((doc) => {
      const data = doc.data();
      const animeId = doc.id;
      const title = data.title || 'Unknown';
      
      if (shouldKeepAnime(data)) {
        console.log(`  ✅ KEEP: ${title} (status: ${data.status})`);
        keepCount++;
      } else {
        console.log(`  🗑️  DELETE: ${title} (old/finished)`);
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
  console.log('🚀 Starting Episodes Cleanup Script...');
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
