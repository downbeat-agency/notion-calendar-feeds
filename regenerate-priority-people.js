// Script to regenerate specific priority calendars one person at a time

const BASE_URL = 'https://notion-calendar-feeds-production.up.railway.app';

// Priority person IDs (with proper UUID formatting)
const PERSON_IDS = [
  'ac294b7c-1907-4977-b5ba-191890a397a3',
  '29aaf64b-cf86-4d3c-b117-3a58cf6c76f2',
  '3cfd19e6-3a2e-4209-bd77-f523812c3f83',
  'f5a0225c-3f0d-4d93-b1ee-ae5ab564d678',
  'c35efa76-6cb1-4cac-abbf-97f4362a1fab',
  'a364001a-0ebf-439b-b114-9fb4f64c8b3a',
  '52151e7a-9823-47dd-8232-2817d8d70a2f',
  'fff39e4a-65a9-8147-9001-cb597f8b49e0',
  '51b050cc-4765-41aa-be46-2a8e3b0632ba',
  'c7ba522b-1f47-4fb2-8996-2cdcdda578a9',
  '4d58fe1a-0687-44d0-90e3-e5b7b84967bb',
  '330ae3dd-b0c3-47d5-a660-ce3b1c925b75',
  '345984c3-1f94-4476-a27c-1b98f51c56d8',
  '948e3520-8fd4-403f-8402-65d250161669'
];

const DELAY_BETWEEN_PEOPLE = 15000; // 15 seconds between each person

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
      console.log(`   ❌ Failed: ${data.message || data.error}`);
      return { success: false, personId, error: data.message || data.error };
    }
  } catch (error) {
    console.log(`   ❌ Error: ${error.message}`);
    return { success: false, personId, error: error.message };
  }
}

async function regenerateAll() {
  console.log('🚀 Starting priority calendar regeneration...');
  console.log(`📋 Processing ${PERSON_IDS.length} people`);
  console.log(`⏱️  ${DELAY_BETWEEN_PEOPLE / 1000}s delay between each`);
  console.log(`⏱️  Estimated time: ~${Math.round((PERSON_IDS.length * (DELAY_BETWEEN_PEOPLE + 10000)) / 60000)} minutes\n`);
  
  const results = [];
  let successCount = 0;
  let failCount = 0;
  
  const startTime = Date.now();
  
  for (let i = 0; i < PERSON_IDS.length; i++) {
    const personId = PERSON_IDS[i];
    
    const result = await regeneratePerson(personId, i, PERSON_IDS.length);
    results.push(result);
    
    if (result.success) {
      successCount++;
    } else {
      failCount++;
    }
    
    // Show progress
    const elapsed = Math.round((Date.now() - startTime) / 1000);
    const avgTime = elapsed / (i + 1);
    const remaining = Math.round(avgTime * (PERSON_IDS.length - i - 1));
    console.log(`   ⏱️  Progress: ${successCount} success, ${failCount} failed | Remaining: ~${remaining}s`);
    
    // Wait before processing next person (except for last one)
    if (i < PERSON_IDS.length - 1) {
      console.log(`   ⏳ Waiting ${DELAY_BETWEEN_PEOPLE / 1000}s...`);
      await new Promise(resolve => setTimeout(resolve, DELAY_BETWEEN_PEOPLE));
    }
  }
  
  const totalTime = Math.round((Date.now() - startTime) / 1000);
  
  console.log('\n\n✨ ========== SUMMARY ==========');
  console.log(`Total: ${PERSON_IDS.length}`);
  console.log(`✅ Success: ${successCount}`);
  console.log(`❌ Failed: ${failCount}`);
  console.log(`⏱️  Total time: ${totalTime}s (${Math.round(totalTime / 60)} minutes)`);
  
  if (successCount > 0) {
    console.log('\n📊 Successful generations:');
    results.filter(r => r.success).forEach(result => {
      console.log(`  ✅ ${result.personName}: ${result.eventCount} events`);
    });
  }
  
  if (failCount > 0) {
    console.log('\n⚠️  Failed generations:');
    results.filter(r => !r.success).forEach(result => {
      console.log(`  ❌ ${result.personId.substring(0, 8)}: ${result.error}`);
    });
  }
  
  console.log('\n✅ Done! All successful calendars are now cached and will load instantly.');
  console.log('🔄 Background job will keep them updated every 30 minutes.');
}

// Run it
regenerateAll().catch(error => {
  console.error('Fatal error:', error);
  process.exit(1);
});

