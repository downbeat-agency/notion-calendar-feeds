// Script to regenerate calendars one person at a time
// This is more fault-tolerant than the bulk endpoint

const BASE_URL = 'https://notion-calendar-feeds-production.up.railway.app';
// const BASE_URL = 'http://localhost:8080'; // For local testing

// List of person IDs to regenerate (add more as needed)
const PERSON_IDS = [
  'f5a0225c-3f0d-4d93-b1ee-ae5ab564d678', // Hubert
  '330ae3dd-b0c3-47d5-a660-ce3b1c925b75', // Gabriel
  '345984c3-1f94-4476-a27c-1b98f51c56d8', // Andrew
  '948e3520-8fd4-403f-8402-65d250161669', // Diego
  // Add more person IDs here as needed
];

const DELAY_BETWEEN_PEOPLE = 15000; // 15 seconds between each person

async function regeneratePerson(personId) {
  try {
    console.log(`\n🔄 Regenerating calendar for ${personId}...`);
    
    const response = await fetch(`${BASE_URL}/regenerate/${personId}`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json'
      }
    });
    
    const data = await response.json();
    
    if (data.success) {
      console.log(`✅ Success! Generated ${data.eventCount} events for ${data.personName}`);
      return { success: true, personId, personName: data.personName, eventCount: data.eventCount };
    } else {
      console.log(`❌ Failed: ${data.message || data.error}`);
      return { success: false, personId, error: data.message || data.error };
    }
  } catch (error) {
    console.log(`❌ Error: ${error.message}`);
    return { success: false, personId, error: error.message };
  }
}

async function regenerateAll() {
  console.log(`🚀 Starting individual calendar regeneration for ${PERSON_IDS.length} people...`);
  console.log(`⏱️  Delay between each: ${DELAY_BETWEEN_PEOPLE / 1000} seconds\n`);
  
  const results = [];
  let successCount = 0;
  let failCount = 0;
  
  for (let i = 0; i < PERSON_IDS.length; i++) {
    const personId = PERSON_IDS[i];
    console.log(`\n📅 Processing ${i + 1}/${PERSON_IDS.length}`);
    
    const result = await regeneratePerson(personId);
    results.push(result);
    
    if (result.success) {
      successCount++;
    } else {
      failCount++;
    }
    
    // Wait before processing next person (except for last one)
    if (i < PERSON_IDS.length - 1) {
      console.log(`⏳ Waiting ${DELAY_BETWEEN_PEOPLE / 1000} seconds before next person...`);
      await new Promise(resolve => setTimeout(resolve, DELAY_BETWEEN_PEOPLE));
    }
  }
  
  console.log('\n\n✨ ========== SUMMARY ==========');
  console.log(`Total: ${PERSON_IDS.length}`);
  console.log(`✅ Success: ${successCount}`);
  console.log(`❌ Failed: ${failCount}`);
  
  console.log('\n📊 Details:');
  results.forEach(result => {
    if (result.success) {
      console.log(`  ✅ ${result.personName || result.personId}: ${result.eventCount} events`);
    } else {
      console.log(`  ❌ ${result.personId}: ${result.error}`);
    }
  });
  
  console.log('\n✅ Done!');
}

// Run it
regenerateAll().catch(error => {
  console.error('Fatal error:', error);
  process.exit(1);
});

