// Test parallel regeneration of 15 people simultaneously

const BASE_URL = 'https://notion-calendar-feeds-production.up.railway.app';

// 15 people from our successful batch to test parallel processing
const TEST_IDS = [
  'ac294b7c-1907-4977-b5ba-191890a397a3', // Adrian - 94 events
  '29aaf64b-cf86-4d3c-b117-3a58cf6c76f2', // Joshua - 100 events
  '3cfd19e6-3a2e-4209-bd77-f523812c3f83', // Daniel - 46 events
  'f5a0225c-3f0d-4d93-b1ee-ae5ab564d678', // Hubert - 64 events
  'c35efa76-6cb1-4cac-abbf-97f4362a1fab', // David - 113 events
  'fff39e4a-65a9-8147-9001-cb597f8b49e0', // Joseph - 26 events
  '51b050cc-4765-41aa-be46-2a8e3b0632ba', // Christian - 118 events
  'c7ba522b-1f47-4fb2-8996-2cdcdda578a9', // Alex - 16 events
  '4d58fe1a-0687-44d0-90e3-e5b7b84967bb', // Joel - 66 events
  '330ae3dd-b0c3-47d5-a660-ce3b1c925b75', // Gabriel - 93 events
  '345984c3-1f94-4476-a27c-1b98f51c56d8', // Andrew - 110 events
  '948e3520-8fd4-403f-8402-65d250161669', // Diego - 113 events
  '20339e4a-65a9-8163-a33b-d5b359fbf7c0', // D'Nasya - 1 event
  '10a39e4a-65a9-815d-896d-db7e38a6d96a', // Casey - 3 events
  'c20ff1e6-c399-4d04-8981-90fd6a857b33', // Chezzarai - 63 events
];

async function regeneratePerson(personId, index) {
  const startTime = Date.now();
  try {
    console.log(`[${index + 1}] Starting ${personId.substring(0, 8)}...`);
    
    const response = await fetch(`${BASE_URL}/regenerate/${personId}`);
    const data = await response.json();
    
    const elapsed = Math.round((Date.now() - startTime) / 1000);
    
    if (data.success) {
      console.log(`[${index + 1}] ✅ ${data.personName}: ${data.eventCount} events (${elapsed}s)`);
      return { success: true, personId, personName: data.personName, eventCount: data.eventCount, time: elapsed };
    } else {
      console.log(`[${index + 1}] ❌ Failed: ${data.message} (${elapsed}s)`);
      return { success: false, personId, error: data.message, time: elapsed };
    }
  } catch (error) {
    const elapsed = Math.round((Date.now() - startTime) / 1000);
    console.log(`[${index + 1}] ❌ Error: ${error.message} (${elapsed}s)`);
    return { success: false, personId, error: error.message, time: elapsed };
  }
}

async function testParallel() {
  console.log('🧪 Testing parallel regeneration of 15 people...');
  console.log('⚡ All 15 will start SIMULTANEOUSLY\n');
  
  const startTime = Date.now();
  
  // Launch all 15 simultaneously
  console.log('🚀 Launching all 15 requests...\n');
  const promises = TEST_IDS.map((personId, index) => regeneratePerson(personId, index));
  
  // Wait for all to complete
  const results = await Promise.all(promises);
  
  const totalTime = Math.round((Date.now() - startTime) / 1000);
  
  // Analyze results
  const successful = results.filter(r => r.success);
  const failed = results.filter(r => !r.success);
  const avgTime = successful.length > 0 ? Math.round(successful.reduce((sum, r) => sum + r.time, 0) / successful.length) : 0;
  const maxTime = successful.length > 0 ? Math.max(...successful.map(r => r.time)) : 0;
  const minTime = successful.length > 0 ? Math.min(...successful.map(r => r.time)) : 0;
  
  console.log('\n\n✨ ========== PARALLEL TEST RESULTS ==========');
  console.log(`Total people: ${TEST_IDS.length}`);
  console.log(`✅ Successful: ${successful.length}`);
  console.log(`❌ Failed: ${failed.length}`);
  console.log(`⏱️  Total wall time: ${totalTime}s`);
  console.log(`⏱️  Individual times: min ${minTime}s, avg ${avgTime}s, max ${maxTime}s`);
  
  if (successful.length > 0) {
    console.log(`\n📊 Efficiency: ${Math.round((successful.reduce((sum, r) => sum + r.time, 0)) / totalTime)}x faster than sequential`);
  }
  
  if (failed.length > 0) {
    console.log('\n⚠️  Failures:');
    failed.forEach(r => {
      console.log(`  ❌ ${r.personId.substring(0, 8)}: ${r.error}`);
    });
  }
  
  console.log('\n💡 Conclusion:');
  if (failed.length === 0) {
    console.log('  ✅ All 15 succeeded! Parallel processing is safe.');
    console.log(`  ✅ With 5 parallel workers: 200 people in ~${Math.round((200 / 5) * avgTime / 60)} minutes`);
  } else if (failed.length <= 2) {
    console.log('  ⚠️  Mostly successful. Parallel processing feasible with error handling.');
  } else {
    console.log('  ❌ Too many failures. Need to reduce parallelism.');
  }
}

testParallel().catch(console.error);

