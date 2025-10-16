// Test batched parallel - get IDs from Calendar Data with minimal properties

import dotenv from 'dotenv';
import { Client } from '@notionhq/client';

dotenv.config();

const notion = new Client({ auth: process.env.NOTION_API_KEY });
const CALENDAR_DATA_DB = process.env.CALENDAR_DATA_DATABASE_ID;
const BASE_URL = 'https://notion-calendar-feeds-production.up.railway.app';

async function getAllPersonIdsFromCalendarData() {
  console.log('🔍 Step 1: Fetching person IDs from Calendar Data (minimal properties)...\n');
  
  const startTime = Date.now();
  
  try {
    // Query with filter_properties to avoid computing formulas
    const response = await notion.databases.query({
      database_id: CALENDAR_DATA_DB,
      page_size: 100,
      filter_properties: ['Personnel'] // Only fetch Personnel relation, skip formulas
    });
    
    const fetchTime = Math.round((Date.now() - startTime) / 1000);
    console.log(`   ✅ Fetched ${response.results.length} rows in ${fetchTime}s`);
    
    // Extract person IDs
    const personIds = [];
    response.results.forEach(row => {
      const personnel = row.properties.Personnel?.relation || [];
      if (personnel.length > 0) {
        personIds.push(personnel[0].id);
      }
    });
    
    console.log(`   ✅ Found ${personIds.length} people with calendar data\n`);
    
    return { success: true, personIds, fetchTime };
    
  } catch (error) {
    const elapsed = Math.round((Date.now() - startTime) / 1000);
    console.error(`   ❌ Failed after ${elapsed}s:`, error.message);
    return { success: false, error: error.message };
  }
}

async function regeneratePerson(personId) {
  const startTime = Date.now();
  try {
    const response = await fetch(`${BASE_URL}/regenerate/${personId}`);
    const data = await response.json();
    const elapsed = Math.round((Date.now() - startTime) / 1000);
    
    if (data.success) {
      return { success: true, personId, personName: data.personName, eventCount: data.eventCount, time: elapsed };
    } else {
      return { success: false, personId, reason: data.reason, time: elapsed };
    }
  } catch (error) {
    const elapsed = Math.round((Date.now() - startTime) / 1000);
    return { success: false, personId, error: error.message, time: elapsed };
  }
}

async function processBatch(personIds, batchNumber, totalBatches) {
  console.log(`🚀 Batch ${batchNumber}/${totalBatches}: Processing ${personIds.length} people in parallel...`);
  
  const batchStartTime = Date.now();
  
  // Launch all in parallel
  const promises = personIds.map(personId => regeneratePerson(personId));
  const results = await Promise.all(promises);
  
  const batchTime = Math.round((Date.now() - batchStartTime) / 1000);
  const successful = results.filter(r => r.success);
  const failed = results.filter(r => !r.success);
  const noEvents = failed.filter(r => r.reason === 'no_events');
  const errors = failed.filter(r => !r.reason || r.reason !== 'no_events');
  
  console.log(`   ✅ Complete in ${batchTime}s: ${successful.length} success | ${noEvents.length} no events | ${errors.length} errors\n`);
  
  return { batchNumber, results, batchTime, successful: successful.length, failed: failed.length };
}

async function testBatchedParallel() {
  console.log('🧪 TESTING BATCHED PARALLEL REGENERATION');
  console.log('=' .repeat(60) + '\n');
  
  const overallStart = Date.now();
  
  // Step 1: Get all person IDs (lightweight)
  const fetchResult = await getAllPersonIdsFromCalendarData();
  
  if (!fetchResult.success) {
    console.error('❌ Failed to fetch person IDs. Aborting.');
    return;
  }
  
  const { personIds, fetchTime } = fetchResult;
  const totalPeople = personIds.length;
  
  console.log(`📊 Ready to process ${totalPeople} people\n`);
  
  // Step 2: Split into batches of 100
  const batches = [];
  for (let i = 0; i < personIds.length; i += 100) {
    batches.push(personIds.slice(i, i + 100));
  }
  
  console.log(`📦 Split into ${batches.length} batches:`);
  batches.forEach((batch, i) => {
    console.log(`   Batch ${i + 1}: ${batch.length} people`);
  });
  console.log('\n' + '='.repeat(60) + '\n');
  
  // Step 3: Process each batch
  const batchResults = [];
  
  for (let i = 0; i < batches.length; i++) {
    const batchResult = await processBatch(batches[i], i + 1, batches.length);
    batchResults.push(batchResult);
    
    // 5-second pause between batches
    if (i < batches.length - 1) {
      console.log(`   ⏳ 5-second pause before batch ${i + 2}...\n`);
      await new Promise(resolve => setTimeout(resolve, 5000));
    }
  }
  
  const totalTime = Math.round((Date.now() - overallStart) / 1000);
  const regenTime = totalTime - fetchTime;
  
  const totalSuccess = batchResults.reduce((sum, b) => sum + b.successful, 0);
  const totalFailed = batchResults.reduce((sum, b) => sum + b.failed, 0);
  
  console.log('\n' + '='.repeat(60));
  console.log('✨ FINAL RESULTS');
  console.log('='.repeat(60) + '\n');
  
  console.log(`📊 Processed: ${totalPeople} people`);
  console.log(`📦 Batches: ${batches.length}`);
  
  console.log(`\n⏱️  Timing Breakdown:`);
  console.log(`  1️⃣  Fetch person IDs: ${fetchTime}s`);
  console.log(`  2️⃣  Regenerate batches: ${regenTime}s`);
  console.log(`  ⏱️  TOTAL TIME: ${totalTime}s (${Math.floor(totalTime / 60)}m ${totalTime % 60}s)`);
  
  console.log(`\n📈 Results:`);
  console.log(`  ✅ Successful: ${totalSuccess} (${Math.round((totalSuccess / totalPeople) * 100)}%)`);
  console.log(`  ❌ Failed: ${totalFailed} (${Math.round((totalFailed / totalPeople) * 100)}%)`);
  
  console.log('\n📊 Per-Batch Performance:');
  batchResults.forEach(b => {
    console.log(`  Batch ${b.batchNumber}: ${b.batchTime}s (${b.successful} ✅ | ${b.failed} ❌)`);
  });
  
  const avgBatchTime = Math.round(batchResults.reduce((sum, b) => sum + b.batchTime, 0) / batchResults.length);
  console.log(`  Average batch time: ${avgBatchTime}s`);
  
  console.log('\n💡 FEASIBILITY FOR 5-MINUTE CYCLE:');
  if (totalTime < 180) {
    console.log(`  ✅ PERFECT! Total time ${Math.floor(totalTime / 60)}m ${totalTime % 60}s < 3 minutes`);
    console.log(`  ✅ 5-minute background cycle is FEASIBLE!`);
    console.log(`  ✅ Data freshness: ${Math.ceil(totalTime / 60)}-5 minutes old`);
    console.log(`  ✅ Ready to implement!`);
  } else if (totalTime < 300) {
    console.log(`  ⚠️  Took ${Math.floor(totalTime / 60)}m ${totalTime % 60}s - tight for 5-min cycle`);
    console.log(`  ⚠️  Recommend 7-10 minute cycle instead`);
  } else {
    console.log(`  ❌ Too slow (${Math.floor(totalTime / 60)}m ${totalTime % 60}s)`);
    console.log(`  ❌ Need 10-15 minute cycle minimum`);
  }
  
  console.log('\n🎯 Implementation Ready: ' + (totalTime < 180 ? 'YES ✅' : 'NEEDS ADJUSTMENT ⚠️'));
}

testBatchedParallel().catch(console.error);

