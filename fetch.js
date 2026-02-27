/**
 * Anime Hub Worker - fetch.js (Enhanced AniList Version)
 * 
 * Uses AniList GraphQL API for accurate, real-time data!
 * Endpoint: https://graphql.anilist.co
 * 
 * Enhancements:
 * - Comprehensive logging for debugging
 * - All available AniList fields extracted
 * - Better error handling and recovery
 * - Performance metrics
 * - Retry logic for failed requests
 */

const admin = require('firebase-admin');
const axios = require('axios');

// ============================================
// CONFIGURATION
// ============================================

const CONFIG = {
  ANILIST_API: 'https://graphql.anilist.co',
  RECENCY_DAYS: 7,
  EPISODES_PER_PAGE: 50,
  MAX_RETRIES: 3,
  RETRY_DELAY: 2000, // 2 seconds
  RATE_LIMIT_DELAY: 700, // milliseconds between requests
};

// ============================================
// FIREBASE INITIALIZATION
// ============================================

const serviceAccount = {
  projectId: process.env.FIREBASE_PROJECT_ID,
  privateKey: process.env.FIREBASE_PRIVATE_KEY.replace(/\\n/g, '\n'),
  clientEmail: process.env.FIREBASE_CLIENT_EMAIL,
};

console.log('🔧 Initializing Firebase...');
console.log(`   Project ID: ${serviceAccount.projectId}`);

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

const db = admin.firestore();
console.log('✅ Firebase initialized successfully\n');

// ============================================
// UTILITY FUNCTIONS
// ============================================

async function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function formatTimestamp(unixTimestamp) {
  return new Date(unixTimestamp * 1000).toISOString();
}

function cleanHtmlTags(html) {
  if (!html) return '';
  return html
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<[^>]*>/g, '')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .trim();
}

// ============================================
// GRAPHQL QUERY (ENHANCED - ALL FIELDS)
// ============================================

const AIRING_ANIME_QUERY = `
query ($page: Int, $perPage: Int) {
  Page(page: $page, perPage: $perPage) {
    pageInfo {
      total
      currentPage
      lastPage
      hasNextPage
      perPage
    }
    airingSchedules(notYetAired: false, sort: TIME_DESC) {
      id
      episode
      airingAt
      timeUntilAiring
      media {
        id
        idMal
        title {
          romaji
          english
          native
          userPreferred
        }
        coverImage {
          extraLarge
          large
          medium
          color
        }
        bannerImage
        startDate {
          year
          month
          day
        }
        endDate {
          year
          month
          day
        }
        description
        season
        seasonYear
        seasonInt
        episodes
        duration
        countryOfOrigin
        isLicensed
        source
        hashtag
        trailer {
          id
          site
        }
        updatedAt
        coverImage {
          extraLarge
          large
          medium
          color
        }
        bannerImage
        genres
        synonyms
        averageScore
        meanScore
        popularity
        isLocked
        trending
        favourites
        tags {
          id
          name
          description
          category
          rank
          isGeneralSpoiler
          isMediaSpoiler
          isAdult
        }
        relations {
          edges {
            id
            relationType
            node {
              id
              idMal
              title {
                romaji
                english
              }
              type
            }
          }
        }
        characters {
          edges {
            id
            role
            name
            node {
              id
              name {
                first
                middle
                last
                full
                native
              }
            }
          }
        }
        staff {
          edges {
            id
            role
            node {
              id
              name {
                first
                middle
                last
                full
                native
              }
            }
          }
        }
        studios(isMain: true) {
          edges {
            isMain
            node {
              id
              name
              isAnimationStudio
              siteUrl
            }
          }
        }
        isFavourite
        isAdult
        nextAiringEpisode {
          id
          airingAt
          timeUntilAiring
          episode
          mediaId
        }
        airingSchedule {
          edges {
            node {
              id
              airingAt
              timeUntilAiring
              episode
            }
          }
        }
        externalLinks {
          id
          url
          site
          type
          language
          color
          icon
        }
        streamingEpisodes {
          title
          thumbnail
          url
          site
        }
        rankings {
          id
          rank
          type
          format
          year
          season
          allTime
          context
        }
        mediaListEntry {
          id
          status
          score
          progress
          repeat
        }
        stats {
          scoreDistribution {
            score
            amount
          }
          statusDistribution {
            status
            amount
          }
        }
        siteUrl
        autoCreateForumThread
        isRecommendationBlocked
        isReviewBlocked
        modNotes
        type
        format
        status
      }
    }
  }
}
`;

// ============================================
// API FUNCTIONS
// ============================================

/**
 * Fetch a single page with retry logic
 */
async function fetchPage(page) {
  let retries = 0;
  let lastError = null;

  while (retries < CONFIG.MAX_RETRIES) {
    try {
      const startTime = Date.now();

      const response = await axios.post(CONFIG.ANILIST_API, {
        query: AIRING_ANIME_QUERY,
        variables: {
          page,
          perPage: CONFIG.EPISODES_PER_PAGE,
        },
      }, {
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'application/json',
        },
        timeout: 30000,
      });

      const requestTime = Date.now() - startTime;

      // Check for GraphQL errors
      if (response.data.errors) {
        console.error('⚠️  GraphQL Errors detected:');
        response.data.errors.forEach((err, i) => {
          console.error(`   Error ${i + 1}: ${err.message}`);
          if (err.locations) {
            console.error(`   Location: Line ${err.locations[0].line}, Column ${err.locations[0].column}`);
          }
        });
      }

      const schedules = response.data?.data?.Page?.airingSchedules || [];
      const pageInfo = response.data?.data?.Page?.pageInfo || {};

      console.log(`✅ Page ${page} fetched successfully`);
      console.log(`   Response time: ${requestTime}ms`);
      console.log(`   Episodes in page: ${schedules.length}`);
      console.log(`   hasNextPage: ${pageInfo.hasNextPage} | lastPage: ${pageInfo.lastPage}`);
      console.log(`   Rate limit remaining: ${response.headers['x-ratelimit-remaining'] || 'N/A'}`);

      return { schedules, pageInfo };
    } catch (error) {
      retries++;
      lastError = error;

      console.error(`❌ Page ${page} failed (Attempt ${retries}/${CONFIG.MAX_RETRIES})`);
      console.error(`   Error: ${error.message}`);

      if (error.response) {
        console.error(`   Status: ${error.response.status}`);
        console.error(`   Data:`, JSON.stringify(error.response.data, null, 2));
      } else if (error.request) {
        console.error(`   No response received from server`);
      }

      if (retries < CONFIG.MAX_RETRIES) {
        const waitTime = CONFIG.RETRY_DELAY * retries;
        console.log(`   ⏳ Retrying in ${waitTime / 1000} seconds...\n`);
        await delay(waitTime);
      }
    }
  }

  console.error(`💥 All retry attempts failed for page ${page}!`);
  console.error(`   Last error: ${lastError?.message}`);
  return null;
}

/**
 * Fetch ALL episodes with full pagination support
 */
async function fetchRecentlyAiredEpisodes() {
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log('📺 FETCHING EPISODES FROM ANILIST (ALL PAGES)');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log(`🔗 API Endpoint: ${CONFIG.ANILIST_API}`);
  console.log(`📄 Episodes per page: ${CONFIG.EPISODES_PER_PAGE}`);
  console.log(`⏱️  Request time: ${new Date().toISOString()}\n`);

  const allSchedules = [];
  let currentPage = 1;
  let hasNextPage = true;
  const now = Date.now() / 1000;
  const cutoffDate = now - (CONFIG.RECENCY_DAYS * 24 * 60 * 60);

  while (hasNextPage) {
    console.log(`\n📄 Fetching page ${currentPage}...`);

    const result = await fetchPage(currentPage);

    if (!result) {
      console.error(`⚠️  Failed to fetch page ${currentPage}, stopping pagination`);
      break;
    }

    const { schedules, pageInfo } = result;
    allSchedules.push(...schedules);

    // Early exit: if the last episode on this page is older than our cutoff,
    // no need to fetch more pages (API returns TIME_DESC order)
    if (schedules.length > 0) {
      const oldestOnPage = schedules[schedules.length - 1].airingAt;
      if (oldestOnPage < cutoffDate) {
        console.log(`\n⏹️  Oldest episode on page ${currentPage} is beyond ${CONFIG.RECENCY_DAYS}-day window — stopping early`);
        hasNextPage = false;
        break;
      }
    }

    hasNextPage = pageInfo.hasNextPage;

    if (hasNextPage) {
      currentPage++;
      console.log(`   ⏳ Waiting ${CONFIG.RATE_LIMIT_DELAY}ms before next page...`);
      await delay(CONFIG.RATE_LIMIT_DELAY);
    }
  }

  console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log(`✅ Pagination complete!`);
  console.log(`   Total pages fetched: ${currentPage}`);
  console.log(`   Total episodes fetched: ${allSchedules.length}`);
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');

  if (allSchedules.length === 0) {
    console.log('⚠️  Warning: No episodes returned from API');
  }

  return allSchedules;
}

// ============================================
// CONTENT FILTERING RULES
// ============================================

// Formats allowed — anime only, no cartoons or other media
const ALLOWED_FORMATS = ['TV', 'TV_SHORT', 'OVA', 'ONA', 'SPECIAL', 'MOVIE'];

// Genres that are strictly blocked (hentai / adult)
const BLOCKED_GENRES = ['Hentai', 'Ecchi'];

// Tags that indicate adult/explicit content — blocked
const BLOCKED_TAGS = [
  'Hentai', 'Ecchi', 'Nudity', 'Explicit Sexual Content',
  'Sex', 'Softcore', 'Pornography', 'BDSM',
  'Sexual Abuse', 'Rape', 'Incest',
];

// Country whitelist — only Japanese anime (JP) and Chinese donghua (CN) / Korean (KR)
// Blocks Western cartoons (US, GB, FR, CA, AU, etc.)
const ALLOWED_COUNTRIES = ['JP', 'CN', 'KR', 'TW'];

/**
 * Check if a media entry should be blocked due to adult/hentai content
 */
function isAdultContent(media) {
  // AniList's own isAdult flag
  if (media.isAdult === true) return { blocked: true, reason: 'isAdult flag' };

  // Block by genre
  const genres = media.genres || [];
  for (const genre of genres) {
    if (BLOCKED_GENRES.includes(genre)) {
      return { blocked: true, reason: `Blocked genre: ${genre}` };
    }
  }

  // Block by tag
  const tags = media.tags || [];
  for (const tag of tags) {
    if (BLOCKED_TAGS.includes(tag.name)) {
      return { blocked: true, reason: `Blocked tag: ${tag.name}` };
    }
  }

  return { blocked: false };
}

/**
 * Check if a media entry is a proper anime (not a cartoon)
 */
function isAnime(media) {
  // Must be type ANIME
  if (media.type !== 'ANIME') {
    return { allowed: false, reason: `Not anime type: ${media.type}` };
  }

  // Must be an allowed format
  if (!ALLOWED_FORMATS.includes(media.format)) {
    return { allowed: false, reason: `Blocked format: ${media.format}` };
  }

  // Must be from an allowed country (JP, CN, KR, TW)
  if (media.countryOfOrigin && !ALLOWED_COUNTRIES.includes(media.countryOfOrigin)) {
    return { allowed: false, reason: `Blocked country: ${media.countryOfOrigin}` };
  }

  return { allowed: true };
}

// ============================================
// FILTERING AND PROCESSING
// ============================================

/**
 * Filter and deduplicate episodes — strict content rules applied
 */
function filterLatestEpisodes(schedules) {
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log('🔍 FILTERING AND PROCESSING EPISODES');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  
  const animeMap = new Map();
  const now = Date.now() / 1000;
  const cutoffDate = now - (CONFIG.RECENCY_DAYS * 24 * 60 * 60);
  
  console.log(`📅 Current time: ${formatTimestamp(now)}`);
  console.log(`📅 Cutoff date (${CONFIG.RECENCY_DAYS} days ago): ${formatTimestamp(cutoffDate)}`);
  console.log(`📊 Total schedules to process: ${schedules.length}\n`);

  let skippedOld = 0;
  let skippedNotAiring = 0;
  let skippedNoMalId = 0;
  let skippedDuplicate = 0;
  let skippedAdult = 0;
  let skippedNotAnime = 0;
  let kept = 0;

  for (let i = 0; i < schedules.length; i++) {
    const schedule = schedules[i];
    const media = schedule.media;
    
    console.log(`\n[${i + 1}/${schedules.length}] Processing: ${media?.title?.romaji || 'Unknown'}`);
    
    // Validation checks
    if (!media) {
      console.log(`   ⚠️  No media data`);
      continue;
    }

    if (!media.idMal) {
      console.log(`   ⏭️  SKIP: No MAL ID (AniList ID: ${media.id})`);
      skippedNoMalId++;
      continue;
    }

    const animeId = media.idMal;
    const airingTime = schedule.airingAt;
    const episode = schedule.episode;

    console.log(`   MAL ID: ${animeId}`);
    console.log(`   AniList ID: ${media.id}`);
    console.log(`   Episode: ${episode}`);
    console.log(`   Aired at: ${formatTimestamp(airingTime)}`);
    console.log(`   Status: ${media.status}`);
    console.log(`   Format: ${media.format} | Country: ${media.countryOfOrigin} | isAdult: ${media.isAdult}`);
    console.log(`   Genres: ${(media.genres || []).join(', ') || 'N/A'}`);

    // ── STRICT FILTER 1: Adult / Hentai / Ecchi content ──
    const adultCheck = isAdultContent(media);
    if (adultCheck.blocked) {
      console.log(`   🚫 SKIP: Adult content — ${adultCheck.reason}`);
      skippedAdult++;
      continue;
    }

    // ── STRICT FILTER 2: Must be anime (not cartoon / other) ──
    const animeCheck = isAnime(media);
    if (!animeCheck.allowed) {
      console.log(`   🚫 SKIP: Not anime — ${animeCheck.reason}`);
      skippedNotAnime++;
      continue;
    }

    // Filter: Only episodes from last N days
    if (airingTime < cutoffDate) {
      const daysAgo = Math.floor((now - airingTime) / (24 * 60 * 60));
      console.log(`   ⏭️  SKIP: Too old (${daysAgo} days ago)`);
      skippedOld++;
      continue;
    }

    // Filter: Only currently airing anime
    if (media.status !== 'RELEASING') {
      console.log(`   ⏭️  SKIP: Not currently releasing (status: ${media.status})`);
      skippedNotAiring++;
      continue;
    }

    // Deduplicate: Keep only latest episode per anime
    if (animeMap.has(animeId)) {
      const existing = animeMap.get(animeId);
      if (existing.episode >= episode) {
        console.log(`   ⏭️  SKIP: Duplicate (already have ep ${existing.episode})`);
        skippedDuplicate++;
        continue;
      } else {
        console.log(`   🔄 REPLACE: Updating from ep ${existing.episode} to ep ${episode}`);
      }
    }

    animeMap.set(animeId, { media, episode, airingTime, scheduleId: schedule.id });
    console.log(`   ✅ KEPT`);
    kept++;
  }

  console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log('📊 FILTERING SUMMARY');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log(`✅ Kept: ${kept}`);
  console.log(`⏭️  Skipped - Too old: ${skippedOld}`);
  console.log(`⏭️  Skipped - Not airing: ${skippedNotAiring}`);
  console.log(`⏭️  Skipped - No MAL ID: ${skippedNoMalId}`);
  console.log(`⏭️  Skipped - Duplicate: ${skippedDuplicate}`);
  console.log(`🚫 Skipped - Adult/Hentai/Ecchi: ${skippedAdult}`);
  console.log(`🚫 Skipped - Not anime (cartoon/other): ${skippedNotAnime}`);
  console.log(`📈 Total processed: ${schedules.length}`);
  console.log('');

  return Array.from(animeMap.values());
}

// ============================================
// DATA CONVERSION
// ============================================

/**
 * Convert AniList data to Firestore format (ALL FIELDS)
 */
function convertToFirestoreFormat(data) {
  const { media, episode, airingTime, scheduleId } = data;
  
  try {
    console.log(`\n🔄 Converting: ${media.title.romaji}`);

    const animeId = media.idMal;
    const title = media.title.english || media.title.romaji || media.title.userPreferred || 'Unknown';
    
    // Get best quality image
    const imageUrl = media.coverImage?.extraLarge || 
                     media.coverImage?.large || 
                     media.coverImage?.medium ||
                     media.bannerImage || '';
    
    // Clean and truncate description
    const synopsis = cleanHtmlTags(media.description || '').substring(0, 1000);
    
    // Extract all available data
    const firestoreData = {
      // ============ Basic Info ============
      animeId,
      anilistId: media.id,
      title,
      titleRomaji: media.title.romaji || '',
      titleEnglish: media.title.english || '',
      titleNative: media.title.native || '',
      synonyms: media.synonyms || [],
      
      // ============ Images ============
      imageUrl,
      coverImageLarge: media.coverImage?.large || '',
      coverImageMedium: media.coverImage?.medium || '',
      coverImageColor: media.coverImage?.color || '',
      bannerImage: media.bannerImage || '',
      
      // ============ Description ============
      synopsis,
      
      // ============ Classification ============
      type: media.type || 'TV',
      format: media.format || '',
      status: 'Currently Airing',
      season: media.season || '',
      seasonYear: media.seasonYear || null,
      seasonInt: media.seasonInt || null,
      
      // ============ Episodes ============
      episodes: media.episodes || 0,
      duration: media.duration || 0,
      latestEpisode: episode,
      latestEpisodeTitle: `Episode ${episode}`,
      
      // ============ Metadata ============
      genres: media.genres || [],
      tags: (media.tags || []).map(tag => ({
        id: tag.id,
        name: tag.name,
        description: tag.description,
        category: tag.category,
        rank: tag.rank,
        isGeneralSpoiler: tag.isGeneralSpoiler || false,
        isMediaSpoiler: tag.isMediaSpoiler || false,
        isAdult: tag.isAdult || false,
      })),
      
      // ============ Studios ============
      studios: (media.studios?.edges || []).map(edge => ({
        id: edge.node.id,
        name: edge.node.name,
        isMain: edge.isMain || false,
        isAnimationStudio: edge.node.isAnimationStudio || false,
        siteUrl: edge.node.siteUrl || '',
      })),
      studiosNames: (media.studios?.edges || []).map(edge => edge.node.name),
      
      // ============ Scores & Rankings ============
      rating: media.averageScore ? media.averageScore / 10 : 0,
      averageScore: media.averageScore || 0,
      meanScore: media.meanScore || 0,
      popularity: media.popularity || 0,
      trending: media.trending || 0,
      favourites: media.favourites || 0,
      rankings: (media.ranDELAY}ms before next page...`);
      await delay(CONFIG.RATE_LIMIT_DELAY);
    }
  }

  console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log(`✅ Pagination complete!`);
  console.log(`   Total pages fetched: ${currentPage}`);
  console.log(`   Total episodes fetched: ${allSchedules.length}`);
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');

  if (allSchedules.length === 0) {
    console.log('⚠️  Warning: No episodes returned from API');
  }

  return allSchedules;
}

// ============================================
// CONTENT FILTERING RULES
// ============================================

// Formats allowed — anime only, no cartoons or other media
const ALLOWED_FORMATS = ['TV', 'TV_SHORT', 'OVA', 'ONA', 'SPECIAL', 'MOVIE'];

// Genres that are strictly blocked (hentai / adult)
const BLOCKED_GENRES = ['Hentai', 'Ecchi'];

// Tags that indicate adult/explicit content — blocked
const BLOCKED_TAGS = [
  'Hentai', 'Ecchi', 'Nudity', 'Explicit Sexual Content',
  'Sex', 'Softcore', 'Pornography', 'BDSM',
  'Sexual Abuse', 'Rape', 'Incest',
];

// Country whitelist — only Japanese anime (JP) and Chinese donghua (CN) / Korean (KR)
// Blocks Western cartoons (US, GB, FR, CA, AU, etc.)
const ALLOWED_COUNTRIES = ['JP', 'CN', 'KR', 'TW'];

/**
 * Check if a media entry should be blocked due to adult/hentai content
 */
function isAdultContent(media) {
  // AniList's own isAdult flag
  if (media.isAdult === true) return { blocked: true, reason: 'isAdult flag' };

  // Block by genre
  const genres = media.genres || [];
  for (const genre of genres) {
    if (BLOCKED_GENRES.includes(genre)) {
      return { blocked: true, reason: `Blocked genre: ${genre}` };
    }
  }

  // Block by tag
  const tags = media.tags || [];
  for (const tag of tags) {
    if (BLOCKED_TAGS.includes(tag.name)) {
      return { blocked: true, reason: `Blocked tag: ${tag.name}` };
    }
  }

  return { blocked: false };
}

/**
 * Check if a media entry is a proper anime (not a cartoon)
 */
function isAnime(media) {
  // Must be type ANIME
  if (media.type !== 'ANIME') {
    return { allowed: false, reason: `Not anime type: ${media.type}` };
  }

  // Must be an allowed format
  if (!ALLOWED_FORMATS.includes(media.format)) {
    return { allowed: false, reason: `Blocked format: ${media.format}` };
  }

  // Must be from an allowed country (JP, CN, KR, TW)
  if (media.countryOfOrigin && !ALLOWED_COUNTRIES.includes(media.countryOfOrigin)) {
    return { allowed: false, reason: `Blocked country: ${media.countryOfOrigin}` };
  }

  return { allowed: true };
}

// ============================================
// FILTERING AND PROCESSING
// ============================================

/**
 * Filter and deduplicate episodes — strict content rules applied
 */
function filterLatestEpisodes(schedules) {
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log('🔍 FILTERING AND PROCESSING EPISODES');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  
  const animeMap = new Map();
  const now = Date.now() / 1000;
  const cutoffDate = now - (CONFIG.RECENCY_DAYS * 24 * 60 * 60);
  
  console.log(`📅 Current time: ${formatTimestamp(now)}`);
  console.log(`📅 Cutoff date (${CONFIG.RECENCY_DAYS} days ago): ${formatTimestamp(cutoffDate)}`);
  console.log(`📊 Total schedules to process: ${schedules.length}\n`);

  let skippedOld = 0;
  let skippedNotAiring = 0;
  let skippedNoMalId = 0;
  let skippedDuplicate = 0;
  let skippedAdult = 0;
  let skippedNotAnime = 0;
  let kept = 0;

  for (let i = 0; i < schedules.length; i++) {
    const schedule = schedules[i];
    const media = schedule.media;
    
    console.log(`\n[${i + 1}/${schedules.length}] Processing: ${media?.title?.romaji || 'Unknown'}`);
    
    // Validation checks
    if (!media) {
      console.log(`   ⚠️  No media data`);
      continue;
    }

    if (!media.idMal) {
      console.log(`   ⏭️  SKIP: No MAL ID (AniList ID: ${media.id})`);
      skippedNoMalId++;
      continue;
    }

    const animeId = media.idMal;
    const airingTime = schedule.airingAt;
    const episode = schedule.episode;

    console.log(`   MAL ID: ${animeId}`);
    console.log(`   AniList ID: ${media.id}`);
    console.log(`   Episode: ${episode}`);
    console.log(`   Aired at: ${formatTimestamp(airingTime)}`);
    console.log(`   Status: ${media.status}`);
    console.log(`   Format: ${media.format} | Country: ${media.countryOfOrigin} | isAdult: ${media.isAdult}`);
    console.log(`   Genres: ${(media.genres || []).join(', ') || 'N/A'}`);

    // ── STRICT FILTER 1: Adult / Hentai / Ecchi content ──
    const adultCheck = isAdultContent(media);
    if (adultCheck.blocked) {
      console.log(`   🚫 SKIP: Adult content — ${adultCheck.reason}`);
      skippedAdult++;
      continue;
    }

    // ── STRICT FILTER 2: Must be anime (not cartoon / other) ──
    const animeCheck = isAnime(media);
    if (!animeCheck.allowed) {
      console.log(`   🚫 SKIP: Not anime — ${animeCheck.reason}`);
      skippedNotAnime++;
      continue;
    }

    // Filter: Only episodes from last N days
    if (airingTime < cutoffDate) {
      const daysAgo = Math.floor((now - airingTime) / (24 * 60 * 60));
      console.log(`   ⏭️  SKIP: Too old (${daysAgo} days ago)`);
      skippedOld++;
      continue;
    }

    // Filter: Only currently airing anime
    if (media.status !== 'RELEASING') {
      console.log(`   ⏭️  SKIP: Not currently releasing (status: ${media.status})`);
      skippedNotAiring++;
      continue;
    }

    // Deduplicate: Keep only latest episode per anime
    if (animeMap.has(animeId)) {
      const existing = animeMap.get(animeId);
      if (existing.episode >= episode) {
        console.log(`   ⏭️  SKIP: Duplicate (already have ep ${existing.episode})`);
        skippedDuplicate++;
        continue;
      } else {
        console.log(`   🔄 REPLACE: Updating from ep ${existing.episode} to ep ${episode}`);
      }
    }

    animeMap.set(animeId, { media, episode, airingTime, scheduleId: schedule.id });
    console.log(`   ✅ KEPT`);
    kept++;
  }

  console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log('📊 FILTERING SUMMARY');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log(`✅ Kept: ${kept}`);
  console.log(`⏭️  Skipped - Too old: ${skippedOld}`);
  console.log(`⏭️  Skipped - Not airing: ${skippedNotAiring}`);
  console.log(`⏭️  Skipped - No MAL ID: ${skippedNoMalId}`);
  console.log(`⏭️  Skipped - Duplicate: ${skippedDuplicate}`);
  console.log(`🚫 Skipped - Adult/Hentai/Ecchi: ${skippedAdult}`);
  console.log(`🚫 Skipped - Not anime (cartoon/other): ${skippedNotAnime}`);
  console.log(`📈 Total processed: ${schedules.length}`);
  console.log('');

  return Array.from(animeMap.values());
}

// ============================================
// DATA CONVERSION
// ============================================

/**
 * Convert AniList data to Firestore format (ALL FIELDS)
 */
function convertToFirestoreFormat(data) {
  const { media, episode, airingTime, scheduleId } = data;
  
  try {
    console.log(`\n🔄 Converting: ${media.title.romaji}`);

    const animeId = media.idMal;
    const title = media.title.english || media.title.romaji || media.title.userPreferred || 'Unknown';
    
    // Get best quality image
    const imageUrl = media.coverImage?.extraLarge || 
                     media.coverImage?.large || 
                     media.coverImage?.medium ||
                     media.bannerImage || '';
    
    // Clean and truncate description
    const synopsis = cleanHtmlTags(media.description || '').substring(0, 1000);
    
    // Extract all available data
    const firestoreData = {
      // ============ Basic Info ============
      animeId,
      anilistId: media.id,
      title,
      titleRomaji: media.title.romaji || '',
      titleEnglish: media.title.english || '',
      titleNative: media.title.native || '',
      synonyms: media.synonyms || [],
      
      // ============ Images ============
      imageUrl,
      coverImageLarge: media.coverImage?.large || '',
      coverImageMedium: media.coverImage?.medium || '',
      coverImageColor: media.coverImage?.color || '',
      bannerImage: media.bannerImage || '',
      
      // ============ Description ============
      synopsis,
      
      // ============ Classification ============
      type: media.type || 'TV',
      format: media.format || '',
      status: 'Currently Airing',
      season: media.season || '',
      seasonYear: media.seasonYear || null,
      seasonInt: media.seasonInt || null,
      
      // ============ Episodes ============
      episodes: media.episodes || 0,
      duration: media.duration || 0,
      latestEpisode: episode,
      latestEpisodeTitle: `Episode ${episode}`,
      
      // ============ Metadata ============
      genres: media.genres || [],
      tags: (media.tags || []).map(tag => ({
        id: tag.id,
        name: tag.name,
        description: tag.description,
        category: tag.category,
        rank: tag.rank,
        isGeneralSpoiler: tag.isGeneralSpoiler || false,
        isMediaSpoiler: tag.isMediaSpoiler || false,
        isAdult: tag.isAdult || false,
      })),
      
      // ============ Studios ============
      studios: (media.studios?.edges || []).map(edge => ({
        id: edge.node.id,
        name: edge.node.name,
        isMain: edge.isMain || false,
        isAnimationStudio: edge.node.isAnimationStudio || false,
        siteUrl: edge.node.siteUrl || '',
      })),
      studiosNames: (media.studios?.edges || []).map(edge => edge.node.name),
      
      // ============ Scores & Rankings ============
      rating: media.averageScore ? media.averageScore / 10 : 0,
      averageScore: media.averageScore || 0,
      meanScore: media.meanScore || 0,
      popularity: media.popularity || 0,
      trending: media.trending || 0,
      favourites: media.favourites || 0,
      rankings: (media.rankings || []).map(rank => ({
        id: rank.id,
        rank: rank.rank,
        type: rank.type,
        format: rank.format,
        year: rank.year,
        season: rank.season,
        allTime: rank.allTime || false,
        context: rank.context,
      })),
      
      // ============ Additional Info ============
      source: media.source || '',
      countryOfOrigin: media.countryOfOrigin || '',
      isLicensed: media.isLicensed || false,
      isAdult: media.isAdult || false,
      hashtag: media.hashtag || '',
      
      // ============ Dates ============
      startDate: media.startDate ? 
        `${media.startDate.year}-${String(media.startDate.month || 1).padStart(2, '0')}-${String(media.startDate.day || 1).padStart(2, '0')}` : 
        null,
      endDate: media.endDate && media.endDate.year ? 
        `${media.endDate.year}-${String(media.endDate.month || 1).padStart(2, '0')}-${String(media.endDate.day || 1).padStart(2, '0')}` : 
        null,
      
      // ============ External Links ============
      mal_url: `https://myanimelist.net/anime/${animeId}`,
      anilist_url: media.siteUrl || '',
      externalLinks: (media.externalLinks || []).map(link => ({
        id: link.id,
        url: link.url,
        site: link.site,
        type: link.type,
        language: link.language,
        color: link.color,
        icon: link.icon,
      })),
      
      // ============ Streaming ============
      streamingEpisodes: (media.streamingEpisodes || []).map(ep => ({
        title: ep.title,
        thumbnail: ep.thumbnail,
        url: ep.url,
        site: ep.site,
      })),
      
      // ============ Trailer ============
      trailer: media.trailer ? {
        id: media.trailer.id,
        site: media.trailer.site,
        url: media.trailer.site === 'youtube' ? 
          `https://www.youtube.com/watch?v=${media.trailer.id}` : 
          null,
      } : null,
      
      // ============ Relations ============
      relations: (media.relations?.edges || []).slice(0, 10).map(edge => ({
        id: edge.node.id,
        idMal: edge.node.idMal,
        type: edge.relationType,
        title: edge.node.title?.english || edge.node.title?.romaji || '',
        format: edge.node.type,
      })),
      
      // ============ Characters (Top 10) ============
      characters: (media.characters?.edges || []).slice(0, 10).map(edge => ({
        id: edge.node.id,
        role: edge.role,
        name: edge.node.name?.full || edge.node.name?.native || '',
      })),
      
      // ============ Staff (Top 10) ============
      staff: (media.staff?.edges || []).slice(0, 10).map(edge => ({
        id: edge.node.id,
        role: edge.role,
        name: edge.node.name?.full || edge.node.name?.native || '',
      })),
      
      // ============ Next Episode ============
      nextEpisode: media.nextAiringEpisode?.episode || null,
      nextEpisodeAiringAt: media.nextAiringEpisode?.airingAt ? 
        formatTimestamp(media.nextAiringEpisode.airingAt) : 
        null,
      timeUntilNextEpisode: media.nextAiringEpisode?.timeUntilAiring || null,
      
      // ============ Stats ============
      scoreDistribution: (media.stats?.scoreDistribution || []).map(dist => ({
        score: dist.score,
        amount: dist.amount,
      })),
      statusDistribution: (media.stats?.statusDistribution || []).map(dist => ({
        status: dist.status,
        amount: dist.amount,
      })),
      
      // ============ Timestamps ============
      episodeAiredDate: formatTimestamp(airingTime),
      episodeAiredAt: airingTime, // Unix timestamp for sorting
      lastUpdated: new Date().toISOString(),
      episodeAddedAt: new Date().toISOString(),
      anilistUpdatedAt: media.updatedAt ? formatTimestamp(media.updatedAt) : null,
      
      // ============ Internal ============
      scheduleId: scheduleId,
      regionLocked: false,
      dataSource: 'anilist',
      apiVersion: '2.0',
    };

    console.log(`   ✅ Conversion successful`);
    console.log(`   Fields extracted: ${Object.keys(firestoreData).length}`);
    
    return firestoreData;
  } catch (error) {
    console.error(`   ❌ Conversion failed: ${error.message}`);
    console.error(`   Stack:`, error.stack);
    return null;
  }
}

// ============================================
// FIRESTORE UPDATE
// ============================================

/**
 * Update Firestore with comprehensive logging
 */
async function updateFirestore(episodesList) {
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log('💾 UPDATING FIRESTORE DATABASE');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log(`📊 Episodes to process: ${episodesList.length}\n`);

  const batch = db.batch();
  let updateCount = 0;
  let newCount = 0;
  let updatedEpisodeCount = 0;
  let refreshCount = 0;
  let errorCount = 0;
  const errors = [];

  const startTime = Date.now();

  for (let i = 0; i < episodesList.length; i++) {
    const data = episodesList[i];
    console.log(`\n[${i + 1}/${episodesList.length}] Processing for Firestore...`);
    
    const animeData = convertToFirestoreFormat(data);
    if (!animeData) {
      errorCount++;
      errors.push({ animeId: data.media?.idMal, error: 'Conversion failed' });
      continue;
    }

    try {
      const docRef = db.collection('episodes').doc(String(animeData.animeId));
      const existing = await docRef.get();

      const prev = existing.exists ? existing.data() : null;
      const prevLatest = prev ? Number(prev.latestEpisode || 0) : 0;
      const newLatest = Number(animeData.latestEpisode || 0);

      console.log(`   Document: episodes/${animeData.animeId}`);
      console.log(`   Exists: ${existing.exists}`);

      if (!existing.exists) {
        // New anime
        batch.set(docRef, animeData);
        updateCount++;
        newCount++;
        console.log(`   ✅ NEW anime will be added`);
        console.log(`      Title: ${animeData.title}`);
        console.log(`      Episode: ${newLatest}`);
      } else if (newLatest > prevLatest) {
        // Episode number increased
        batch.set(docRef, animeData, { merge: true });
        updateCount++;
        updatedEpisodeCount++;
        console.log(`   ✅ EPISODE UPDATE`);
        console.log(`      Title: ${animeData.title}`);
        console.log(`      Previous: Episode ${prevLatest}`);
        console.log(`      New: Episode ${newLatest}`);
      } else {
        // Same episode - refresh metadata only
        animeData.episodeAddedAt = prev.episodeAddedAt || animeData.episodeAddedAt;
        batch.set(docRef, animeData, { merge: true });
        updateCount++;
        refreshCount++;
        console.log(`   🔄 METADATA REFRESH`);
        console.log(`      Title: ${animeData.title}`);
        console.log(`      Episode: ${newLatest} (unchanged)`);
      }
    } catch (error) {
      errorCount++;
      errors.push({ 
        animeId: animeData.animeId, 
        title: animeData.title,
        error: error.message 
      });
      console.error(`   ❌ Error: ${error.message}`);
    }
  }

  // Commit batch
  try {
    console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    console.log('💾 COMMITTING BATCH TO FIRESTORE...');
    await batch.commit();
    const commitTime = Date.now() - startTime;
    
    console.log('✅ BATCH COMMITTED SUCCESSFULLY!');
    console.log(`⏱️  Total time: ${commitTime}ms`);
    console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    console.log('📊 FIRESTORE UPDATE SUMMARY');
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    console.log(`✅ Total operations: ${updateCount}`);
    console.log(`   🆕 New anime: ${newCount}`);
    console.log(`   📈 Episode updates: ${updatedEpisodeCount}`);
    console.log(`   🔄 Metadata refreshes: ${refreshCount}`);
    console.log(`   ❌ Errors: ${errorCount}`);
    
    if (errors.length > 0) {
      console.log('\n⚠️  ERRORS DETAILS:');
      errors.forEach((err, i) => {
        console.log(`   ${i + 1}. ${err.title || `Anime ${err.animeId}`}: ${err.error}`);
      });
    }
    
    console.log('');
  } catch (error) {
    console.error('\n💥 BATCH COMMIT FAILED!');
    console.error(`   Error: ${error.message}`);
    console.error(`   Stack:`, error.stack);
    throw error;
  }
}

// ============================================
// MAIN EXECUTION
// ============================================

async function main() {
  const scriptStartTime = Date.now();
  
  console.log('\n');
  console.log('═══════════════════════════════════════════');
  console.log('🚀 ANIME HUB WORKER - ENHANCED VERSION');
  console.log('═══════════════════════════════════════════');
  console.log(`⏰ Started at: ${new Date().toISOString()}`);
  console.log(`🌍 Timezone: ${Intl.DateTimeFormat().resolvedOptions().timeZone}`);
  console.log(`📡 API: ${CONFIG.ANILIST_API}`);
  console.log(`📅 Recency window: ${CONFIG.RECENCY_DAYS} days`);
  console.log(`🔢 Episodes per fetch: ${CONFIG.EPISODES_PER_PAGE}`);
  console.log(`🔄 Max retries: ${CONFIG.MAX_RETRIES}`);
  console.log(`⏱️  Rate limit delay: ${CONFIG.RATE_LIMIT_DELAY}ms`);
  console.log('');

  try {
    // Step 1: Fetch episodes from AniList
    const schedules = await fetchRecentlyAiredEpisodes();
    
    if (schedules.length === 0) {
      console.log('⚠️  No episodes found or API request failed');
      console.log('   Exiting without database updates');
      process.exit(1);
    }

    // Step 2: Filter and process episodes
    const latestEpisodes = filterLatestEpisodes(schedules);
    
    if (latestEpisodes.length === 0) {
      console.log('⚠️  No episodes passed filtering');
      console.log('   This might be normal if no new episodes aired recently');
      console.log('   Exiting without database updates');
      process.exit(0);
    }

    // Step 3: Update Firestore
    await updateFirestore(latestEpisodes);

    // Success summary
    const totalTime = Date.now() - scriptStartTime;
    console.log('═══════════════════════════════════════════');
    console.log('✅ WORKER COMPLETED SUCCESSFULLY');
    console.log('═══════════════════════════════════════════');
    console.log(`⏱️  Total execution time: ${(totalTime / 1000).toFixed(2)}s`);
    console.log(`⏰ Finished at: ${new Date().toISOString()}`);
    console.log('═══════════════════════════════════════════\n');
    
    process.exit(0);
  } catch (error) {
    const totalTime = Date.now() - scriptStartTime;
    console.error('\n═══════════════════════════════════════════');
    console.error('💥 FATAL ERROR - WORKER FAILED');
    console.error('═══════════════════════════════════════════');
    console.error(`❌ Error: ${error.message}`);
    console.error(`📍 Stack trace:`);
    console.error(error.stack);
    console.error(`⏱️  Failed after: ${(totalTime / 1000).toFixed(2)}s`);
    console.error(`⏰ Failed at: ${new Date().toISOString()}`);
    console.error('═══════════════════════════════════════════\n');
    
    process.exit(1);
  }
}

// Run the script
main();
