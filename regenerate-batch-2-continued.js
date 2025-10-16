// Script to continue batch 2 from where we left off

const BASE_URL = 'https://notion-calendar-feeds-production.up.railway.app';

// Starting from person 29 where we left off
const PERSON_IDS = [
  'c32e8758-a1e8-4f9e-be7a-e5aeba078f27',
  '1b42c721-c54a-4cc9-9c09-7da8cf0935c0',
  '18e3a82c-4d20-4f55-86cd-0ef5ee782f18',
  '1bf61721-41c8-4a5d-a700-d1a48f29981a',
  '82116a82-8d06-4aaa-8947-951950d0243f',
  '6b20e26d-c6c0-4af1-990d-9ee3e1418269',
];

const DELAY_BETWEEN_PEOPLE = 15000; // 15 seconds

async function regeneratePerson(personId, index, total) {
  try {
    console.log(`\n📅 [${index + 1}/${total}] ${personId}`);
    console.log(`   🔄 Regenerating...`);
    
    const response = await fetch(`${BASE_URL}/regenerate/${personId}`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json'
      }
    });
    
    const data = await response.json();
    
    if (data.success) {
      console.log(`   ✅ Success! ${data.personName}: ${data.eventCount} events`);
      return { success: true, personId, personName: data.personName, eventCount: data.eventCount };
    } else {
      // Better error categorization
      if (data.reason === 'no_events') {
        console.log(`   ⏭️  Skipped: No events in calendar`);
        return { success: false, personId, reason: 'no_events' };
      } else if (data.reason && data.reason.includes('JSON parse error')) {
        console.log(`   ⚠️  Skipped: Bad data in Notion (JSON error)`);
        return { success: false, personId, reason: 'bad_data' };
      } else {
        console.log(`   ❌ Failed: ${data.message || data.error}`);
        return { success: false, personId, error: data.message || data.error };
      }
    }
  } catch (error) {
    console.log(`   ❌ Error: ${error.message}`);
    return { success: false, personId, error: error.message };
  }
}

async function regenerateAll() {
  console.log('🚀 Continuing batch 2 calendar regeneration...');
  console.log(`📋 Processing remaining ${PERSON_IDS.length} people`);
  console.log(`⏱️  ${DELAY_BETWEEN_PEOPLE / 1000}s delay between each\n`);
  
  const results = [];
  let successCount = 0;
  let noEventsCount = 0;
  let badDataCount = 0;
  let errorCount = 0;
  
  const startTime = Date.now();
  
  for (let i = 0; i < PERSON_IDS.length; i++) {
    const personId = PERSON_IDS[i];
    
    const result = await regeneratePerson(personId, i, PERSON_IDS.length);
    results.push(result);
    
    if (result.success) {
      successCount++;
    } else if (result.reason === 'no_events') {
      noEventsCount++;
    } else if (result.reason === 'bad_data') {
      badDataCount++;
    } else {
      errorCount++;
    }
    
    // Show progress
    const elapsed = Math.round((Date.now() - startTime) / 1000);
    console.log(`   ⏱️  Progress: ${successCount} ✅ | ${noEventsCount} ⏭️ | ${badDataCount} ⚠️ | ${errorCount} ❌`);
    
    // Wait before processing next person (except for last one)
    if (i < PERSON_IDS.length - 1) {
      console.log(`   ⏳ Waiting ${DELAY_BETWEEN_PEOPLE / 1000}s...`);
      await new Promise(resolve => setTimeout(resolve, DELAY_BETWEEN_PEOPLE));
    }
  }
  
  const totalTime = Math.round((Date.now() - startTime) / 1000);
  
  console.log('\n\n✨ ========== COMPLETION SUMMARY ==========');
  console.log(`Total processed: ${PERSON_IDS.length}`);
  console.log(`✅ Cached successfully: ${successCount}`);
  console.log(`⏭️  Skipped (no events): ${noEventsCount}`);
  console.log(`⚠️  Skipped (bad data): ${badDataCount}`);
  console.log(`❌ Errors: ${errorCount}`);
  console.log(`⏱️  Total time: ${totalTime}s (${Math.round(totalTime / 60)} minutes)`);
  
  if (successCount > 0) {
    console.log('\n📊 Successfully cached:');
    results.filter(r => r.success).forEach(result => {
      console.log(`  ✅ ${result.personName}: ${result.eventCount} events`);
    });
  }
  
  console.log('\n✅ Done!');
  console.log('📊 Combined total (batch 1 + batch 2): ~' + (12 + 13 + successCount) + ' people cached');
  console.log('🔄 Background job will keep updating them every 30 minutes.');
}

// Run it
regenerateAll().catch(error => {
  console.error('Fatal error:', error);
  process.exit(1);
});

