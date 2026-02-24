/**
 * Anime Hub Worker - fetch.js (UPDATED)
 * 
 * This script runs every 10 minutes via GitHub Actions.
 * It fetches new episodes from Jikan API, then fetches FULL anime data,
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

    // ✨ NEW: Fetch FULL anime data from Jikan API
    const fullAnimeData = await fetchFullAnimeData(animeId);

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
 * Update Firestore with new episodes (now with full data)
 */
async function updateFirestore(episodesList) {
  const batch = db.batch();
  let updateCount = 0;
  let errorCount = 0;

  console.log(`\n📝 Processing ${episodesList.length} episodes...`);

  for (const entry of episodesList) {
    const animeData = await processEpisodeEntry(entry);
    if (!animeData) {
      errorCount++;
      continue;
    }

    try {
      const docRef = db.collection('episodes').doc(String(animeData.animeId));
      const existing = await docRef.get();

      const prev = existing.exists ? (existing.data() || {}) : null;
      const prevLatest = prev ? Number(prev.latestEpisode || 0) : 0;
      const newLatest = Number(animeData.latestEpisode || 0);

      // Determine whether we should write the document. We avoid rewriting
      // the doc if the latest episode number hasn't changed to prevent
      // bumping timestamps for unchanged shows.
      let needsWrite = !existing.exists || newLatest !== prevLatest;

      // Build object to write. We normally write metadata fields, but only
      // set `episodeAddedAt` when the latest episode is newly detected OR
      // when creating a new doc whose `airedDate` is within the recent window.
      const toWrite = Object.assign({}, animeData);
      const nowIso = new Date().toISOString();

      // If the latest episode increased, mark it as newly added right now.
      if (newLatest > prevLatest) {
        toWrite.episodeAddedAt = nowIso;
        needsWrite = true; // ensure we write the update
      } else if (!existing.exists) {
        // New document: only mark as recent if airedDate is within last 48h.
        if (toWrite.airedDate) {
          const aired = new Date(toWrite.airedDate);
          const cutoff = new Date(Date.now() - 48 * 60 * 60 * 1000);
          if (aired >= cutoff) {
            toWrite.episodeAddedAt = nowIso;
            needsWrite = true;
          }
        }
      } else {
        // Existing doc with same latest episode – check if it's missing the
        // timestamp and should be backfilled (e.g. we previously skipped it).
        if (!prev || !prev.episodeAddedAt) {
          if (toWrite.airedDate) {
            const aired = new Date(toWrite.airedDate);
            const cutoff = new Date(Date.now() - 48 * 60 * 60 * 1000);
            if (aired >= cutoff) {
              toWrite.episodeAddedAt = nowIso;
              needsWrite = true;
              console.log(`  🛠️  Backfilling timestamp for ${animeData.title}`);
            }
          }
        }
      }

      if (needsWrite) {
        batch.set(docRef, toWrite, { merge: true });
        updateCount++;
        console.log(`  ✅ Queued write: ${animeData.title} (latest ${newLatest})`);
      } else {
        console.log(`  ➖ Skipping unchanged: ${animeData.title}`);
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
  console.log('🚀 Starting Anime Hub Worker...');
  console.log(`⏰ Time: ${new Date().toISOString()}`);
  console.log(`📡 Fetching from: ${JIKAN_API}/watch/episodes`);
  console.log(`📚 Then enriching with: ${JIKAN_API}/anime/{id}/full`);

  try {
    // Fetch latest episodes
    const episodesList = await fetchLatestEpisodes();
    console.log(`📊 Found ${episodesList.length} new episode entries`);

    if (episodesList.length > 0) {
      // Process and update Firestore (with full anime data for each)
      await updateFirestore(episodesList);
    } else {
      console.log('⚠️  No new episodes found');
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
