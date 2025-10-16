// Script to regenerate batch 2 calendars (skipping already completed ones)

const BASE_URL = 'https://notion-calendar-feeds-production.up.railway.app';

// Batch 2 person IDs (excluding ones already done in batch 1)
const PERSON_IDS = [
  // 'ac294b7c-1907-4977-b5ba-191890a397a3', // SKIP - already done
  '20339e4a-65a9-8163-a33b-d5b359fbf7c0',
  '1ec39e4a-65a9-819a-a157-e498c7a8be90',
  '9ceddff1-ca4e-429f-952d-5110d5819613',
  '19139e4a-65a9-803d-a097-d26b61d25feb',
  '19139e4a-65a9-8069-a3c6-e5124edc5b99',
  '19139e4a-65a9-80a3-b4f6-dd5f16d5460a',
  '17339e4a-65a9-802e-a8f2-cd7f6dc59c1f',
  'cce05de8-3b00-497d-8cab-2f3aad4e843c',
  // '29aaf64b-cf86-4d3c-b117-3a58cf6c76f2', // SKIP - already done
  // '3cfd19e6-3a2e-4209-bd77-f523812c3f83', // SKIP - already done
  '10b39e4a-65a9-8156-88a6-d3bf9a15855c',
  '10a39e4a-65a9-815d-896d-db7e38a6d96a',
  'c20ff1e6-c399-4d04-8981-90fd6a857b33',
  // 'f5a0225c-3f0d-4d93-b1ee-ae5ab564d678', // SKIP - already done
  // 'c35efa76-6cb1-4cac-abbf-97f4362a1fab', // SKIP - already done
  'c13e1f17-d797-4626-ba8a-a9935bb0e154',
  '97192f16-33cd-45e6-a9a3-75f106284b54',
  '6b02aa2e-9cd4-4e67-a0ec-1d9c25ffe4fc',
  '426643e3-dd75-4e10-ba7b-117ff820bd52',
  'e1799ad5-57fd-465a-bb22-69540548cd7c',
  'c9144be1-0f8d-4881-9c54-6373d879fac9',
  '4bb84d1b-a2b3-4177-aaed-85c88633bf60',
  '192ee879-2892-4368-8550-9a36a338d4db',
  '88d00f44-30c2-4e18-8a8f-67d0cc7088a3',
  'c890fd84-2dcf-4ee7-941f-4c946833af0c',
  '9ad631ad-2077-4233-9e38-c0e24e34110e',
  '550b8ac9-8e03-4a03-b985-61c0a05aff05',
  'db72170f-9d3c-46cc-a1f1-3dcf303cad54',
  '89fba2d8-faaa-4ce9-bc73-4b616d5a5707',
  'd535a4ad-592c-43ba-a7e1-f8a090851b18',
  '0d1f11cc-0ea7-4297-ad5b-0ba5d426e07f',
  '26036abf-7d5a-492c-98e0-06ab4bce14e7',
  'c32e8758-a1e8-4f9e-be7a-e5aeba078f27',
  '1b42c721-c54a-4cc9-9c09-7da8cf0935c0',
  '18e3a82c-4d20-4f55-86cd-0ef5ee782f18',
  '1bf61721-41c8-4a5d-a700-d1a48f29981a',
  '82116a82-8d06-4aaa-8947-951950d0243f',
  // '4d58fe1a-0687-44d0-90e3-e5b7b84967bb', // SKIP - already done
  // '330ae3dd-b0c3-47d5-a660-ce3b1c925b75', // SKIP - already done
  // '345984c3-1f94-4476-a27c-1b98f51c56d8', // SKIP - already done
  '6b20e26d-c6c0-4af1-990d-9ee3e1418269',
  // '948e3520-8fd4-403f-8402-65d250161669', // SKIP - already done
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
  console.log('🚀 Starting batch 2 calendar regeneration...');
  console.log(`📋 Processing ${PERSON_IDS.length} NEW people (9 skipped from batch 1)`);
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
  
  console.log('\n\n✨ ========== BATCH 2 SUMMARY ==========');
  console.log(`Total processed: ${PERSON_IDS.length}`);
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
  console.log(`📊 Total cached so far: ${12 + successCount} people (batch 1: 12, batch 2: ${successCount})`);
}

// Run it
regenerateAll().catch(error => {
  console.error('Fatal error:', error);
  process.exit(1);
});

