// Test the 11 people who had JSON errors to see if they're fixed

const BASE_URL = 'https://notion-calendar-feeds-production.up.railway.app';

// The 11 people who had JSON parse errors
const FIXED_IDS = [
  'cce05de8-3b00-497d-8cab-2f3aad4e843c',
  '10b39e4a-65a9-8156-88a6-d3bf9a15855c',
  '97192f16-33cd-45e6-a9a3-75f106284b54',
  'e1799ad5-57fd-465a-bb22-69540548cd7c',
  '192ee879-2892-4368-8550-9a36a338d4db',
  '9ad631ad-2077-4233-9e38-c0e24e34110e',
  'd535a4ad-592c-43ba-a7e1-f8a090851b18',
  '0d1f11cc-0ea7-4297-ad5b-0ba5d426e07f',
  '26036abf-7d5a-492c-98e0-06ab4bce14e7',
  'c32e8758-a1e8-4f9e-be7a-e5aeba078f27',
  '1b42c721-c54a-4cc9-9c09-7da8cf0935c0',
];

const DELAY = 15000; // 15 seconds

async function regeneratePerson(personId, index, total) {
  try {
    console.log(`\n📅 [${index + 1}/${total}] ${personId}`);
    console.log(`   🔄 Regenerating...`);
    
    const response = await fetch(`${BASE_URL}/regenerate/${personId}`);
    const data = await response.json();
    
    if (data.success) {
      console.log(`   ✅ Success! ${data.personName}: ${data.eventCount} events`);
      return { success: true, personId, personName: data.personName, eventCount: data.eventCount };
    } else {
      console.log(`   ❌ Failed: ${data.message || data.error}`);
      if (data.reason) console.log(`   Reason: ${data.reason}`);
      return { success: false, personId, error: data.message || data.error, reason: data.reason };
    }
  } catch (error) {
    console.log(`   ❌ Error: ${error.message}`);
    return { success: false, personId, error: error.message };
  }
}

async function testAll() {
  console.log('🧪 Testing 11 people who had JSON errors...');
  console.log('📋 After Notion formula fix\n');
  
  const results = [];
  let successCount = 0;
  let stillFailCount = 0;
  
  const startTime = Date.now();
  
  for (let i = 0; i < FIXED_IDS.length; i++) {
    const personId = FIXED_IDS[i];
    
    const result = await regeneratePerson(personId, i, FIXED_IDS.length);
    results.push(result);
    
    if (result.success) {
      successCount++;
    } else {
      stillFailCount++;
    }
    
    console.log(`   ⏱️  Progress: ${successCount} ✅ | ${stillFailCount} ❌`);
    
    if (i < FIXED_IDS.length - 1) {
      console.log(`   ⏳ Waiting ${DELAY / 1000}s...`);
      await new Promise(resolve => setTimeout(resolve, DELAY));
    }
  }
  
  const totalTime = Math.round((Date.now() - startTime) / 1000);
  
  console.log('\n\n✨ ========== TEST RESULTS ==========');
  console.log(`Total tested: ${FIXED_IDS.length}`);
  console.log(`✅ Now working: ${successCount}`);
  console.log(`❌ Still failing: ${stillFailCount}`);
  console.log(`⏱️  Total time: ${totalTime}s (${Math.round(totalTime / 60)} minutes)`);
  
  if (successCount > 0) {
    console.log('\n🎉 Successfully fixed and cached:');
    results.filter(r => r.success).forEach(result => {
      console.log(`  ✅ ${result.personName}: ${result.eventCount} events`);
    });
  }
  
  if (stillFailCount > 0) {
    console.log('\n⚠️  Still having issues:');
    results.filter(r => !r.success).forEach(result => {
      console.log(`  ❌ ${result.personId.substring(0, 8)}: ${result.reason || result.error}`);
    });
  }
  
  console.log('\n📊 GRAND TOTAL: ' + (25 + successCount) + ' people cached and ready!');
}

testAll().catch(console.error);

