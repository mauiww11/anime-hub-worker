/**
 * Anime Hub Worker - fetch.js (FIXED VERSION)
 * 
 * This script runs every 10 minutes via GitHub Actions.
 * It fetches new episodes from Jikan API, filters for RECENT episodes only,
 * and updates Firestore with complete information.
 * 
 * Requirements:
 * - Firebase Secrets: FIREBASE_PROJECT_ID, FIREBASE_PRIVATE_KEY, FIREBASE_CLIENT_EMAIL
 * - npm packages: firebase-admin, axios
 */

const admin = require('firebase-admin');
const axios = require('axios');

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
const JIKAN_API = 'https://api.jikan.moe/v4';

// ✅ FIX: Only show episodes from the last 7 days
const RECENCY_WINDOW_DAYS = 7;

// Rate limiting: Jikan allows 60 requests/minute
// We'll make requests with a 100ms delay between them
async function delay(ms = 100) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Fetch latest episodes from /watch/episodes endpoint
 */
async function fetchLatestEpisodes() {
  try {
    console.log('📺 Fetching latest episodes from /watch/episodes...');
    
    const response = await axios.get(`${JIKAN_API}/watch/episodes`, {
      params: {
        page: 1, // First page of latest episodes
      },
    });

    return response.data.data || [];
  } catch (error) {
    console.error('❌ Error fetching episodes:', error.message);
    return [];
  }
}

/**
 * Fetch FULL anime data from /anime/{id}/full endpoint
 */
async function fetchFullAnimeData(animeId) {
  try {
    console.log(`  📖 Fetching full data for anime ${animeId}...`);
    await delay(100); // Rate limiting
    
    const response = await axios.get(`${JIKAN_API}/anime/${animeId}/full`);
    return response.data.data || null;
  } catch (error) {
    console.warn(`  ⚠️  Could not fetch full data for anime ${animeId}: ${error.message}`);
    return null;
  }
}

/**
 * ✅ NEW: Check if an episode entry is recent enough to display
 */
function isRecentEpisode(entry, fullAnimeData) {
  const now = new Date();
  const cutoffDate = new Date(now.getTime() - (RECENCY_WINDOW_DAYS * 24 * 60 * 60 * 1000));
  
  // Strategy 1: Check if the episode itself has a recent aired date
  if (entry.episodes && entry.episodes.length > 0) {
    const latestEp = entry.episodes[0];
    if (latestEp.aired) {
      const epAiredDate = new Date(latestEp.aired);
      if (epAiredDate >= cutoffDate) {
        console.log(`  ✅ Recent episode (aired: ${latestEp.aired})`);
        return true;
      }
    }
  }
  
  // Strategy 2: Check if anime is currently airing
  if (fullAnimeData) {
    const status = fullAnimeData.status?.toLowerCase() || '';
    const airing = fullAnimeData.airing || false;
    
    if (status === 'currently airing' || airing === true) {
      console.log(`  ✅ Currently airing anime`);
      return true;
    }
    
    // Strategy 3: Check if anime aired recently (for new series)
    if (fullAnimeData.aired?.from) {
      const animeStartDate = new Date(fullAnimeData.aired.from);
      if (animeStartDate >= cutoffDate) {
        console.log(`  ✅ Recently started airing (from: ${fullAnimeData.aired.from})`);
        return true;
      }
    }
  }
  
  console.log(`  ⏭️  Skipping old/finished anime`);
  return false;
}

/**
 * Process episode entry + full anime data
 */
async function processEpisodeEntry(entry) {
  try {
    if (!entry || !entry.entry) return null;

    const animeData = entry.entry;
    const animeId = animeData.mal_id;
    
    // Get basic data from episode entry
    let imageUrl = '';
    if (animeData.images?.webp?.image_url) {
      imageUrl = animeData.images.webp.image_url;
    } else if (animeData.images?.jpg?.image_url) {
      imageUrl = animeData.images.jpg.image_url;
    }

    let latestEpisodeNum = 0;
    if (entry.episodes && entry.episodes.length > 0) {
      latestEpisodeNum = entry.episodes[0]?.mal_id || 0;
    }

    // ✨ Fetch FULL anime data from Jikan API
    const fullAnimeData = await fetchFullAnimeData(animeId);
    
    // ✅ FIX: Filter out old/finished anime
    if (!isRecentEpisode(entry, fullAnimeData)) {
      return null; // Skip this anime
    }

    // Merge basic + full data (prioritize full data)
    const mergedData = {
      animeId,
      title: fullAnimeData?.title || animeData.title || 'Unknown',
      imageUrl: fullAnimeData?.images?.webp?.large_image_url || imageUrl,
      synopsis: fullAnimeData?.synopsis || '',
      genres: fullAnimeData?.genres?.map(g => g.name) || [],
      studios: fullAnimeData?.studios?.map(s => s.name) || [],
      rating: fullAnimeData?.score || 0,
      episodes: fullAnimeData?.episodes || 0,
      status: fullAnimeData?.status || animeData.status || 'Airing',
      seasonYear: fullAnimeData?.year || null,
      season: fullAnimeData?.season || '',
      type: fullAnimeData?.type || '',
      
      // Episode-specific data
      latestEpisode: latestEpisodeNum,
      latestEpisodeTitle: entry.episodes && entry.episodes.length > 0 ? entry.episodes[0].title : '',
      mal_url: fullAnimeData?.url || animeData.url || `https://myanimelist.net/anime/${animeId}`,
      regionLocked: entry.region_locked || false,
      // keep an explicit aired date (if available) for filtering
      airedDate: fullAnimeData?.aired?.from || animeData.aired?.from || null,
      episodeAiredDate: entry.episodes && entry.episodes.length > 0 ? entry.episodes[0].aired : null,

      // Metadata
      lastUpdated: new Date().toISOString(),
    };

    return mergedData;
  } catch (error) {
    console.error(`❌ Error processing episode entry:`, error.message);
    return null;
  }
}

/**
 * Update Firestore with new episodes (now with filtering)
 */
async function updateFirestore(episodesList) {
  const batch = db.batch();
  let updateCount = 0;
  let errorCount = 0;
  let skippedOldCount = 0;

  console.log(`\n📝 Processing ${episodesList.length} episodes...`);

  for (const entry of episodesList) {
    const animeData = await processEpisodeEntry(entry);
    if (!animeData) {
      skippedOldCount++;
      continue; // Skip old/finished anime
    }

    try {
      const docRef = db.collection('episodes').doc(String(animeData.animeId));
      const existing = await docRef.get();

      const prev = existing.exists ? (existing.data() || {}) : null;
      const prevLatest = prev ? Number(prev.latestEpisode || 0) : 0;
      const newLatest = Number(animeData.latestEpisode || 0);

      // Always add episodeAddedAt for new recent episodes
      const nowIso = new Date().toISOString();
      let needsWrite = false;

      if (!existing.exists) {
        // New document - always write with timestamp
        animeData.episodeAddedAt = nowIso;
        needsWrite = true;
        console.log(`  ✅ NEW anime: ${animeData.title} (ep ${newLatest})`);
      } else if (newLatest > prevLatest) {
        // Episode number increased - update timestamp
        animeData.episodeAddedAt = nowIso;
        needsWrite = true;
        console.log(`  ✅ UPDATED: ${animeData.title} (${prevLatest} → ${newLatest})`);
      } else if (!prev.episodeAddedAt) {
        // Existing doc but missing timestamp - backfill
        animeData.episodeAddedAt = nowIso;
        needsWrite = true;
        console.log(`  🛠️  BACKFILL timestamp: ${animeData.title}`);
      } else {
        // Same episode, already has timestamp - refresh metadata only
        needsWrite = true;
        console.log(`  🔄 REFRESH metadata: ${animeData.title}`);
      }

      if (needsWrite) {
        batch.set(docRef, animeData, { merge: true });
        updateCount++;
      }
    } catch (error) {
      errorCount++;
      console.error(`  ❌ Error processing anime ${animeData.animeId}:`, error.message);
    }
  }

  try {
    await batch.commit();
    console.log(`\n✨ Batch committed!`);
    console.log(`✅ Updated ${updateCount} episodes in Firestore`);
    console.log(`⏭️  Skipped ${skippedOldCount} old/finished anime`);
    if (errorCount > 0) {
      console.log(`⚠️  Failed to process ${errorCount} entries`);
    }
  } catch (error) {
    console.error('❌ Error updating Firestore:', error.message);
  }
}

/**
 * Main execution
 */
async function main() {
  console.log('🚀 Starting Anime Hub Worker (FIXED VERSION)...');
  console.log(`⏰ Time: ${new Date().toISOString()}`);
  console.log(`📡 Fetching from: ${JIKAN_API}/watch/episodes`);
  console.log(`📚 Then enriching with: ${JIKAN_API}/anime/{id}/full`);
  console.log(`🎯 Filtering: Only episodes from last ${RECENCY_WINDOW_DAYS} days`);

  try {
    // Fetch latest episodes
    const episodesList = await fetchLatestEpisodes();
    console.log(`📊 Found ${episodesList.length} episode entries from API`);

    if (episodesList.length > 0) {
      // Process and update Firestore (with filtering for recent only)
      await updateFirestore(episodesList);
    } else {
      console.log('⚠️  No episodes found from API');
    }

    console.log('✨ Worker completed successfully!');
    process.exit(0);
  } catch (error) {
    console.error('💥 Fatal error:', error.message);
    process.exit(1);
  }
}

// Run the script
main();
