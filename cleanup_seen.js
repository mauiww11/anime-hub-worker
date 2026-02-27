/**
 * cleanup_seen.js
 *
 * Runs once daily at 00:00 UTC (before the main fetch worker).
 * Removes entries older than 7 days from seen_episodes.json
 * and commits the trimmed file back to the repo.
 */

const fs = require('fs');

const SEEN_FILE = 'seen_episodes.json';
const RECENCY_DAYS = 7;

console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
console.log('🧹 DAILY CLEANUP — seen_episodes.json');
console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
console.log(`⏰ Started at: ${new Date().toISOString()}`);

if (!fs.existsSync(SEEN_FILE)) {
  console.log(`⚠️  ${SEEN_FILE} not found — nothing to clean`);
  process.exit(0);
}

const raw = JSON.parse(fs.readFileSync(SEEN_FILE, 'utf8'));
const total = Object.keys(raw).length;

const now = Date.now() / 1000;
const cutoff = now - (RECENCY_DAYS * 24 * 60 * 60);

const trimmed = {};
let removed = 0;

for (const [key, airingAt] of Object.entries(raw)) {
  if (airingAt >= cutoff) {
    trimmed[key] = airingAt;
  } else {
    removed++;
    console.log(`   🗑️  Removed: ${key}`);
  }
}

fs.writeFileSync(SEEN_FILE, JSON.stringify(trimmed, null, 2), 'utf8');

console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
console.log('📊 CLEANUP SUMMARY');
console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
console.log(`📂 Total before: ${total}`);
console.log(`🗑️  Removed (>7 days): ${removed}`);
console.log(`✅ Remaining: ${Object.keys(trimmed).length}`);
console.log(`⏰ Finished at: ${new Date().toISOString()}`);
console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
