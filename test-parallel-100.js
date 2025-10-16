// Test 100 parallel regeneration requests
// Using our known working IDs and duplicating them to reach 100

const BASE_URL = 'https://notion-calendar-feeds-production.up.railway.app';

// Known working person IDs (34 unique people)
const WORKING_IDS = [
  'ac294b7c-1907-4977-b5ba-191890a397a3',
  '29aaf64b-cf86-4d3c-b117-3a58cf6c76f2',
  '3cfd19e6-3a2e-4209-bd77-f523812c3f83',
  'f5a0225c-3f0d-4d93-b1ee-ae5ab564d678',
  'c35efa76-6cb1-4cac-abbf-97f4362a1fab',
  'fff39e4a-65a9-8147-9001-cb597f8b49e0',
  '51b050cc-4765-41aa-be46-2a8e3b0632ba',
  'c7ba522b-1f47-4fb2-8996-2cdcdda578a9',
  '4d58fe1a-0687-44d0-90e3-e5b7b84967bb',
  '330ae3dd-b0c3-47d5-a660-ce3b1c925b75',
  '345984c3-1f94-4476-a27c-1b98f51c56d8',
  '948e3520-8fd4-403f-8402-65d250161669',
  '20339e4a-65a9-8163-a33b-d5b359fbf7c0',
  '1ec39e4a-65a9-819a-a157-e498c7a8be90',
  '9ceddff1-ca4e-429f-952d-5110d5819613',
  '10a39e4a-65a9-815d-896d-db7e38a6d96a',
  'c20ff1e6-c399-4d04-8981-90fd6a857b33',
  'c13e1f17-d797-4626-ba8a-a9935bb0e154',
  '6b02aa2e-9cd4-4e67-a0ec-1d9c25ffe4fc',
  '426643e3-dd75-4e10-ba7b-117ff820bd52',
  'c9144be1-0f8d-4881-9c54-6373d879fac9',
  '4bb84d1b-a2b3-4177-aaed-85c88633bf60',
  '88d00f44-30c2-4e18-8a8f-67d0cc7088a3',
  'c890fd84-2dcf-4ee7-941f-4c946833af0c',
  '89fba2d8-faaa-4ce9-bc73-4b616d5a5707',
  'cce05de8-3b00-497d-8cab-2f3aad4e843c',
  '10b39e4a-65a9-8156-88a6-d3bf9a15855c',
  'e1799ad5-57fd-465a-bb22-69540548cd7c',
  '9ad631ad-2077-4233-9e38-c0e24e34110e',
  '26036abf-7d5a-492c-98e0-06ab4bce14e7',
  'c32e8758-a1e8-4f9e-be7a-e5aeba078f27',
  '1b42c721-c54a-4cc9-9c09-7da8cf0935c0',
  '97192f16-33cd-45e6-a9a3-75f106284b54',
  '0d1f11cc-0ea7-4297-ad5b-0ba5d426e07f',
];

// Create 100-item array by cycling through the 34 IDs
const TEST_IDS = [];
for (let i = 0; i < 100; i++) {
  TEST_IDS.push(WORKING_IDS[i % WORKING_IDS.length]);
}

async function regeneratePerson(personId, index) {
  const startTime = Date.now();
  try {
    const response = await fetch(`${BASE_URL}/regenerate/${personId}`);
    const data = await response.json();
    
    const elapsed = Math.round((Date.now() - startTime) / 1000);
    
    if (data.success) {
      return { success: true, personId, personName: data.personName, eventCount: data.eventCount, time: elapsed };
    } else {
      return { success: false, personId, error: data.message, time: elapsed };
    }
  } catch (error) {
    const elapsed = Math.round((Date.now() - startTime) / 1000);
    return { success: false, personId, error: error.message, time: elapsed };
  }
}

async function testParallel100() {
  console.log('🧪 Testing 100 PARALLEL regeneration requests...');
  console.log('⚡ All 100 will fire SIMULTANEOUSLY');
  console.log('📊 Using 34 unique people (cycled to reach 100)\n');
  
  const startTime = Date.now();
  
  console.log('🚀 Launching all 100 requests NOW...\n');
  
  // Launch all 100 simultaneously
  const promises = TEST_IDS.map((personId, index) => regeneratePerson(personId, index));
  
  // Wait for all to complete
  const results = await Promise.all(promises);
  
  const totalWallTime = Math.round((Date.now() - startTime) / 1000);
  
  // Analyze results
  const successful = results.filter(r => r.success);
  const failed = results.filter(r => !r.success);
  const timeouts = failed.filter(r => r.error && r.error.includes('timeout'));
  
  const avgTime = successful.length > 0 ? Math.round(successful.reduce((sum, r) => sum + r.time, 0) / successful.length) : 0;
  const maxTime = successful.length > 0 ? Math.max(...successful.map(r => r.time)) : 0;
  const minTime = successful.length > 0 ? Math.min(...successful.map(r => r.time)) : 0;
  
  console.log('\n\n✨ ========== 100 PARALLEL TEST RESULTS ==========');
  console.log(`Total requests: ${TEST_IDS.length}`);
  console.log(`✅ Successful: ${successful.length}`);
  console.log(`❌ Failed: ${failed.length}`);
  console.log(`⏱️  Timeouts: ${timeouts.length}`);
  console.log(`\n⏱️  Wall time: ${totalWallTime}s (${Math.round(totalWallTime / 60)} min)`);
  console.log(`⏱️  Individual times: min ${minTime}s, avg ${avgTime}s, max ${maxTime}s`);
  
  if (successful.length > 0) {
    const totalProcessingTime = successful.reduce((sum, r) => sum + r.time, 0);
    console.log(`⏱️  Total processing time: ${totalProcessingTime}s`);
    console.log(`📊 Parallelization efficiency: ${Math.round(totalProcessingTime / totalWallTime)}x speedup`);
  }
  
  if (timeouts.length > 0) {
    console.log(`\n⚠️  Timeout rate: ${Math.round((timeouts.length / TEST_IDS.length) * 100)}%`);
  }
  
  console.log('\n💡 Conclusion:');
  if (failed.length === 0) {
    console.log('  ✅ PERFECT! All 100 succeeded with no timeouts!');
    console.log(`  ✅ 200 people could be regenerated in ~${Math.round(totalWallTime * 2 / 60)} minutes`);
  } else if (timeouts.length === 0) {
    console.log('  ✅ No timeouts! Failures likely due to bad data, not API limits.');
  } else if (timeouts.length < 10) {
    console.log('  ⚠️  Some timeouts but mostly successful.');
    console.log(`  ⚠️  Success rate: ${Math.round((successful.length / TEST_IDS.length) * 100)}%`);
  } else {
    console.log('  ❌ Too many timeouts. Reduce parallelism to 50 or fewer.');
  }
}

testParallel100().catch(console.error);

