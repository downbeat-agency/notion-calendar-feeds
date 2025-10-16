// Test batched parallel concept with known working IDs
// Simulates processing 200+ people by using our 34 working IDs multiple times

const BASE_URL = 'https://notion-calendar-feeds-production.up.railway.app';

// 34 known working person IDs
const KNOWN_IDS = [
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

// Create 250 total IDs by cycling through the 34 known IDs
// This simulates having 250 people to test the batching concept
const ALL_IDS = [];
for (let i = 0; i < 250; i++) {
  ALL_IDS.push(KNOWN_IDS[i % KNOWN_IDS.length]);
}

async function regeneratePerson(personId) {
  const startTime = Date.now();
  try {
    const response = await fetch(`${BASE_URL}/regenerate/${personId}`);
    const data = await response.json();
    const elapsed = Math.round((Date.now() - startTime) / 1000);
    
    if (data.success) {
      return { success: true, time: elapsed };
    } else {
      return { success: false, reason: data.reason, time: elapsed };
    }
  } catch (error) {
    const elapsed = Math.round((Date.now() - startTime) / 1000);
    return { success: false, error: error.message, time: elapsed };
  }
}

async function processBatch(personIds, batchNumber, totalBatches) {
  console.log(`🚀 Batch ${batchNumber}/${totalBatches}: ${personIds.length} people in parallel...`);
  
  const batchStartTime = Date.now();
  
  // Launch all in parallel
  const promises = personIds.map(personId => regeneratePerson(personId));
  const results = await Promise.all(promises);
  
  const batchTime = Math.round((Date.now() - batchStartTime) / 1000);
  const successful = results.filter(r => r.success);
  const failed = results.filter(r => !r.success);
  
  console.log(`   ✅ Done in ${batchTime}s: ${successful.length} success | ${failed.length} failed\n`);
  
  return { batchNumber, results, batchTime, successful: successful.length, failed: failed.length };
}

async function testBatchedParallel() {
  console.log('🧪 TESTING BATCHED PARALLEL PROCESSING');
  console.log('📋 Simulating 250 people (using 34 unique IDs cycled)');
  console.log('🎯 Goal: Prove batches of 100 can complete within 5-min window\n');
  console.log('='.repeat(60) + '\n');
  
  const overallStart = Date.now();
  
  // Split into batches of 100
  const batches = [];
  for (let i = 0; i < ALL_IDS.length; i += 100) {
    batches.push(ALL_IDS.slice(i, i + 100));
  }
  
  console.log(`📦 ${batches.length} batches: ${batches.map(b => b.length).join(', ')} people\n`);
  console.log('='.repeat(60) + '\n');
  
  // Process each batch
  const batchResults = [];
  
  for (let i = 0; i < batches.length; i++) {
    const batchResult = await processBatch(batches[i], i + 1, batches.length);
    batchResults.push(batchResult);
    
    // 5-second pause between batches
    if (i < batches.length - 1) {
      console.log(`   ⏳ 5s pause...\n`);
      await new Promise(resolve => setTimeout(resolve, 5000));
    }
  }
  
  const totalTime = Math.round((Date.now() - overallStart) / 1000);
  const totalSuccess = batchResults.reduce((sum, b) => sum + b.successful, 0);
  const totalFailed = batchResults.reduce((sum, b) => sum + b.failed, 0);
  
  console.log('='.repeat(60));
  console.log('✨ FINAL RESULTS');
  console.log('='.repeat(60) + '\n');
  
  console.log(`📊 Total: 250 requests (simulating 250 people)`);
  console.log(`📦 Batches: ${batches.length} batches of 100`);
  console.log(`⏱️  TOTAL TIME: ${totalTime}s (${Math.floor(totalTime / 60)}m ${totalTime % 60}s)`);
  console.log(`📈 Success: ${totalSuccess} | Failed: ${totalFailed}`);
  
  console.log('\n📊 Batch Performance:');
  batchResults.forEach(b => {
    console.log(`  Batch ${b.batchNumber}: ${b.batchTime}s`);
  });
  
  const avgBatchTime = Math.round(batchResults.reduce((sum, b) => sum + b.batchTime, 0) / batchResults.length);
  console.log(`  Average: ${avgBatchTime}s per batch`);
  
  console.log('\n💡 5-MINUTE CYCLE FEASIBILITY:');
  if (totalTime < 180) {
    console.log(`  ✅ YES! ${Math.floor(totalTime / 60)}m ${totalTime % 60}s fits comfortably in 5-min cycle`);
    console.log(`  ✅ Buffer: ${300 - totalTime}s idle time between cycles`);
  } else if (totalTime < 240) {
    console.log(`  ⚠️  Tight fit - ${Math.floor(totalTime / 60)}m ${totalTime % 60}s`);
    console.log(`  ⚠️  Recommend 5-6 minute cycle`);
  } else {
    console.log(`  ❌ Too slow for 5-min cycle`);
    console.log(`  ❌ Recommend 7-10 minute cycle`);
  }
}

testBatchedParallel().catch(console.error);

