/**
 * Anime Hub Worker - fetch.js
 * 
 * This script runs every 3 minutes via GitHub Actions.
 * It fetches new episodes from Jikan API and updates Firestore.
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
 * Process episode entry and extract anime data
 */
function processEpisodeEntry(entry) {
  try {
    if (!entry || !entry.entry) return null;

    const animeData = entry.entry;
    const animeId = animeData.mal_id;
    
    // Get image URL (prioritize webp, fallback to jpg)
    let imageUrl = '';
    if (animeData.images?.webp?.image_url) {
      imageUrl = animeData.images.webp.image_url;
    } else if (animeData.images?.jpg?.image_url) {
      imageUrl = animeData.images.jpg.image_url;
    }

    // Get latest episode number from episodes array
    let latestEpisodeNum = 0;
    if (entry.episodes && entry.episodes.length > 0) {
      latestEpisodeNum = entry.episodes[0]?.mal_id || 0;
    }

    return {
      animeId,
      title: animeData.title || 'Unknown',
      image: imageUrl,
      episodes: animeData.episodes || 0,
      airedDate: animeData.aired?.from || new Date().toISOString(),
      status: animeData.status || 'Airing',
      score: animeData.score || 0,
      synopsis: animeData.synopsis || '',
      genres: animeData.genres?.map(g => g.name) || [],
      studios: animeData.studios?.map(s => s.name) || [],
      latestEpisode: latestEpisodeNum,
      url: animeData.url || `https://myanimelist.net/anime/${animeId}`,
      regionLocked: entry.region_locked || false,
      lastUpdated: new Date().toISOString(),
    };
  } catch (error) {
    console.error(`❌ Error processing episode entry:`, error.message);
    return null;
  }
}

/**
 * Update Firestore with new episodes
 */
async function updateFirestore(episodesList) {
  const batch = db.batch();
  let updateCount = 0;
  const errorCount = 0;

  for (const entry of episodesList) {
    const animeData = processEpisodeEntry(entry);
    
    if (animeData) {
      try {
        const docRef = db.collection('episodes').doc(String(animeData.animeId));
        batch.set(docRef, animeData, { merge: true });
        updateCount++;
      } catch (error) {
        console.error(`❌ Error processing anime ${animeData.animeId}:`, error.message);
      }
    }
  }

  try {
    await batch.commit();
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

  try {
    // Fetch latest episodes
    const episodesList = await fetchLatestEpisodes();
    console.log(`📊 Found ${episodesList.length} new episode entries`);

    if (episodesList.length > 0) {
      // Process and update Firestore
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
