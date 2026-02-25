/**
 * Anime Hub Worker - fetch.js (AniList Version)
 * 
 * Uses AniList GraphQL API for accurate, real-time data!
 * Endpoint: https://graphql.anilist.co
 * 
 * Advantages:
 * - Accurate latest episode numbers
 * - Real-time updates
 * - Better rate limits (90 requests/minute)
 * - More reliable data
 */

const admin = require('firebase-admin');
const axios = require('axios');

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
const ANILIST_API = 'https://graphql.anilist.co';

// Rate limiting: AniList allows 90 requests/minute
async function delay(ms = 700) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * GraphQL Query to fetch currently airing anime with latest episodes
 */
const AIRING_ANIME_QUERY = `
query ($page: Int, $perPage: Int) {
  Page(page: $page, perPage: $perPage) {
    pageInfo {
      total
      currentPage
      lastPage
      hasNextPage
    }
    airingSchedules(notYetAired: false, sort: TIME_DESC) {
      id
      episode
      airingAt
      media {
        id
        idMal
        title {
          romaji
          english
          native
        }
        coverImage {
          extraLarge
          large
        }
        bannerImage
        description
        status
        episodes
        season
        seasonYear
        averageScore
        genres
        studios(isMain: true) {
          nodes {
            name
          }
        }
        siteUrl
        nextAiringEpisode {
          episode
          airingAt
        }
      }
    }
  }
}
`;

/**
 * Fetch recently aired episodes from AniList
 */
async function fetchRecentlyAiredEpisodes() {
  try {
    console.log('📺 Fetching recently aired episodes from AniList...');
    
    const response = await axios.post(ANILIST_API, {
      query: AIRING_ANIME_QUERY,
      variables: {
        page: 1,
        perPage: 50, // Get 50 most recent episodes
      },
    });

    const schedules = response.data?.data?.Page?.airingSchedules || [];
    console.log(`📊 Found ${schedules.length} recently aired episodes`);
    
    return schedules;
  } catch (error) {
    console.error('❌ Error fetching from AniList:', error.message);
    if (error.response) {
      console.error('Response:', error.response.data);
    }
    return [];
  }
}

/**
 * Filter and deduplicate anime (keep only latest episode per anime)
 */
function filterLatestEpisodes(schedules) {
  const animeMap = new Map();
  const now = Date.now() / 1000; // Current time in seconds
  const sevenDaysAgo = now - (7 * 24 * 60 * 60);
  
  console.log('\n📝 Processing and filtering episodes...');
  
  for (const schedule of schedules) {
    const media = schedule.media;
    if (!media || !media.idMal) continue;
    
    const animeId = media.idMal;
    const airingTime = schedule.airingAt;
    const episode = schedule.episode;
    
    // ✅ Filter: Only episodes from last 7 days
    if (airingTime < sevenDaysAgo) {
      console.log(`  ⏭️  Too old: ${media.title.romaji} ep ${episode} (${new Date(airingTime * 1000).toISOString()})`);
      continue;
    }
    
    // ✅ Filter: Only currently airing anime
    if (media.status !== 'RELEASING') {
      console.log(`  ⏭️  Not airing: ${media.title.romaji} (${media.status})`);
      continue;
    }
    
    // Keep only the latest episode per anime
    if (!animeMap.has(animeId) || animeMap.get(animeId).episode < episode) {
      animeMap.set(animeId, { media, episode, airingTime });
      console.log(`  ✅ KEEP: ${media.title.romaji} ep ${episode}`);
    } else {
      console.log(`  ⏭️  Duplicate (older): ${media.title.romaji} ep ${episode}`);
    }
  }
  
  return Array.from(animeMap.values());
}

/**
 * Convert AniList data to Firestore format
 */
function convertToFirestoreFormat(data) {
  const { media, episode, airingTime } = data;
  
  try {
    const animeId = media.idMal;
    const title = media.title.english || media.title.romaji || 'Unknown';
    
    // Get best quality image
    const imageUrl = media.coverImage?.extraLarge || 
                     media.coverImage?.large || 
                     media.bannerImage || '';
    
    // Clean HTML from description
    const synopsis = media.description 
      ? media.description.replace(/<[^>]*>/g, '').substring(0, 500)
      : '';
    
    const firestoreData = {
      animeId,
      title,
      imageUrl,
      synopsis,
      genres: media.genres || [],
      studios: media.studios?.nodes?.map(s => s.name) || [],
      rating: media.averageScore ? media.averageScore / 10 : 0, // Convert 0-100 to 0-10
      episodes: media.episodes || 0,
      status: 'Currently Airing', // AniList RELEASING = Currently Airing
      seasonYear: media.seasonYear || null,
      season: media.season || '',
      type: 'TV', // AniList mostly has TV anime
      
      // Episode-specific data
      latestEpisode: episode,
      latestEpisodeTitle: `Episode ${episode}`,
      mal_url: `https://myanimelist.net/anime/${animeId}`,
      anilist_url: media.siteUrl || '',
      regionLocked: false,
      
      // Timestamps
      episodeAiredDate: new Date(airingTime * 1000).toISOString(),
      lastUpdated: new Date().toISOString(),
      episodeAddedAt: new Date().toISOString(),
    };
    
    // Add next episode info if available
    if (media.nextAiringEpisode) {
      firestoreData.nextEpisode = media.nextAiringEpisode.episode;
      firestoreData.nextEpisodeAiringAt = new Date(media.nextAiringEpisode.airingAt * 1000).toISOString();
    }
    
    return firestoreData;
  } catch (error) {
    console.error(`❌ Error converting data:`, error.message);
    return null;
  }
}

/**
 * Update Firestore with new episodes
 */
async function updateFirestore(episodesList) {
  const batch = db.batch();
  let updateCount = 0;
  let newCount = 0;
  let refreshCount = 0;

  console.log(`\n📝 Updating Firestore with ${episodesList.length} anime...`);

  for (const data of episodesList) {
    const animeData = convertToFirestoreFormat(data);
    if (!animeData) continue;

    try {
      const docRef = db.collection('episodes').doc(String(animeData.animeId));
      const existing = await docRef.get();

      const prev = existing.exists ? existing.data() : null;
      const prevLatest = prev ? Number(prev.latestEpisode || 0) : 0;
      const newLatest = Number(animeData.latestEpisode || 0);

      if (!existing.exists) {
        // New anime
        batch.set(docRef, animeData);
        updateCount++;
        newCount++;
        console.log(`  ✅ NEW: ${animeData.title} (ep ${newLatest})`);
      } else if (newLatest > prevLatest) {
        // Episode number increased
        batch.set(docRef, animeData, { merge: true });
        updateCount++;
        console.log(`  ✅ UPDATED: ${animeData.title} (${prevLatest} → ${newLatest})`);
      } else {
        // Same episode - refresh metadata only
        animeData.episodeAddedAt = prev.episodeAddedAt || animeData.episodeAddedAt;
        batch.set(docRef, animeData, { merge: true });
        updateCount++;
        refreshCount++;
        console.log(`  🔄 REFRESH: ${animeData.title} (ep ${newLatest})`);
      }
    } catch (error) {
      console.error(`  ❌ Error processing anime ${animeData.animeId}:`, error.message);
    }
  }

  try {
    await batch.commit();
    console.log(`\n✨ Batch committed!`);
    console.log(`✅ Total updated: ${updateCount} anime`);
    console.log(`   - New: ${newCount}`);
    console.log(`   - Updated episodes: ${updateCount - newCount - refreshCount}`);
    console.log(`   - Refreshed: ${refreshCount}`);
  } catch (error) {
    console.error('❌ Error updating Firestore:', error.message);
  }
}

/**
 * Main execution
 */
async function main() {
  console.log('🚀 Starting Anime Hub Worker (AniList Version)...');
  console.log(`⏰ Time: ${new Date().toISOString()}`);
  console.log(`📡 Using AniList GraphQL API: ${ANILIST_API}`);
  console.log(`🎯 Filtering: Episodes from last 7 days + Currently airing\n`);

  try {
    // Fetch recently aired episodes
    const schedules = await fetchRecentlyAiredEpisodes();
    
    if (schedules.length === 0) {
      console.log('⚠️  No episodes found');
      process.exit(0);
    }

    // Filter and deduplicate
    const latestEpisodes = filterLatestEpisodes(schedules);
    console.log(`\n📊 After filtering: ${latestEpisodes.length} unique anime with recent episodes`);

    if (latestEpisodes.length > 0) {
      // Update Firestore
      await updateFirestore(latestEpisodes);
    }

    console.log('\n✨ Worker completed successfully!');
    process.exit(0);
  } catch (error) {
    console.error('💥 Fatal error:', error.message);
    console.error(error.stack);
    process.exit(1);
  }
}

// Run the script
main();
