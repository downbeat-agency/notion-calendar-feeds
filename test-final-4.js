// Test the final 4 people who still had JSON errors

const BASE_URL = 'https://notion-calendar-feeds-production.up.railway.app';

// The 4 people who still had issues after first fix
const FINAL_4 = [
  '97192f16-33cd-45e6-a9a3-75f106284b54',
  '192ee879-2892-4368-8550-9a36a338d4db',
  'd535a4ad-592c-43ba-a7e1-f8a090851b18',
  '0d1f11cc-0ea7-4297-ad5b-0ba5d426e07f',
];

const DELAY = 15000; // 15 seconds

async function regeneratePerson(personId, index, total) {
  try {
    console.log(`\n📅 [${index + 1}/${total}] ${personId}`);
    console.log(`   🔄 Regenerating...`);
    
    const response = await fetch(`${BASE_URL}/regenerate/${personId}`);
    const data = await response.json();
    
    if (data.success) {
      console.log(`   ✅ SUCCESS! ${data.personName}: ${data.eventCount} events`);
      return { success: true, personId, personName: data.personName, eventCount: data.eventCount };
    } else {
      console.log(`   ❌ Failed: ${data.message || data.error}`);
      if (data.reason) console.log(`   Reason: ${data.reason.substring(0, 100)}...`);
      return { success: false, personId, error: data.message || data.error, reason: data.reason };
    }
  } catch (error) {
    console.log(`   ❌ Error: ${error.message}`);
    return { success: false, personId, error: error.message };
  }
}

async function testFinal4() {
  console.log('🧪 Testing final 4 people with JSON errors...');
  console.log('📋 After escaping newlines in addresses\n');
  
  const results = [];
  let successCount = 0;
  let stillFailCount = 0;
  
  for (let i = 0; i < FINAL_4.length; i++) {
    const personId = FINAL_4[i];
    
    const result = await regeneratePerson(personId, i, FINAL_4.length);
    results.push(result);
    
    if (result.success) {
      successCount++;
    } else {
      stillFailCount++;
    }
    
    console.log(`   ⏱️  Progress: ${successCount} ✅ | ${stillFailCount} ❌`);
    
    if (i < FINAL_4.length - 1) {
      console.log(`   ⏳ Waiting ${DELAY / 1000}s...`);
      await new Promise(resolve => setTimeout(resolve, DELAY));
    }
  }
  
  console.log('\n\n✨ ========== FINAL TEST RESULTS ==========');
  console.log(`Total tested: ${FINAL_4.length}`);
  console.log(`✅ Now working: ${successCount}`);
  console.log(`❌ Still failing: ${stillFailCount}`);
  
  if (successCount > 0) {
    console.log('\n🎉 Successfully fixed and cached:');
    results.filter(r => r.success).forEach(result => {
      console.log(`  ✅ ${result.personName}: ${result.eventCount} events`);
    });
  }
  
  if (stillFailCount > 0) {
    console.log('\n⚠️  Still having issues (may need Transportation formula fix):');
    results.filter(r => !r.success).forEach(result => {
      console.log(`  ❌ ${result.personId.substring(0, 8)}`);
      if (result.reason) {
        const errorType = result.reason.includes('Transportation') ? 'TRANSPORTATION' : 'FLIGHTS';
        console.log(`     Type: ${errorType} JSON error`);
      }
    });
  }
  
  console.log('\n📊 GRAND TOTAL: ' + (32 + successCount) + ' people cached and ready!');
  console.log('🔄 Background job continues updating calendars every 30 minutes.');
}

testFinal4().catch(console.error);

