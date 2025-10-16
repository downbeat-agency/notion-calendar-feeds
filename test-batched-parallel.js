// Test batched parallel processing: 100 people per batch

import dotenv from 'dotenv';
import { Client } from '@notionhq/client';

dotenv.config();

const notion = new Client({ auth: process.env.NOTION_API_KEY });
const CALENDAR_DATA_DB = process.env.CALENDAR_DATA_DATABASE_ID;
const PERSONNEL_DB = process.env.PERSONNEL_DATABASE_ID;
const BASE_URL = 'https://notion-calendar-feeds-production.up.railway.app';

async function getAllPersonIds() {
  console.log('🔍 Step 1: Fetching person IDs (lightweight query)...\n');
  
  const startTime = Date.now();
  
  try {
    // Query Personnel database directly - faster than Calendar Data
    const response = await notion.databases.query({
      database_id: PERSONNEL_DB,
      page_size: 100,
      filter: {
        property: 'Active',
        checkbox: {
          equals: true
        }
      }
    });
    
    let allPeople = [...response.results];
    let hasMore = response.has_more;
    let nextCursor = response.next_cursor;
    let pageCount = 1;
    
    // Handle pagination
    while (hasMore) {
      pageCount++;
      console.log(`   Fetching page ${pageCount}...`);
      
      const nextPage = await notion.databases.query({
        database_id: PERSONNEL_DB,
        page_size: 100,
        start_cursor: nextCursor,
        filter: {
          property: 'Active',
          checkbox: {
            equals: true
          }
        }
      });
      
      allPeople = [...allPeople, ...nextPage.results];
      hasMore = nextPage.has_more;
      nextCursor = nextPage.next_cursor;
    }
    
    const personIds = allPeople.map(p => p.id);
    const fetchTime = Math.round((Date.now() - startTime) / 1000);
    
    console.log(`   ✅ Fetched ${personIds.length} person IDs in ${fetchTime}s (${pageCount} pages)\n`);
    
    return { success: true, personIds, fetchTime };
    
  } catch (error) {
    const elapsed = Math.round((Date.now() - startTime) / 1000);
    console.error(`   ❌ Failed after ${elapsed}s:`, error.message);
    return { success: false, error: error.message };
  }
}

async function regeneratePerson(personId, globalIndex, batchIndex) {
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
  console.log(`\n🚀 Batch ${batchNumber}/${totalBatches}: Processing ${personIds.length} people in parallel...`);
  
  const batchStartTime = Date.now();
  
  // Launch all in parallel
  const promises = personIds.map((personId, index) => 
    regeneratePerson(personId, (batchNumber - 1) * 100 + index, index)
  );
  
  const results = await Promise.all(promises);
  
  const batchTime = Math.round((Date.now() - batchStartTime) / 1000);
  const successful = results.filter(r => r.success);
  const failed = results.filter(r => !r.success);
  const noEvents = failed.filter(r => r.reason === 'no_events');
  const errors = failed.filter(r => !r.reason || r.reason !== 'no_events');
  
  console.log(`   ✅ Batch ${batchNumber} complete in ${batchTime}s`);
  console.log(`   Success: ${successful.length} | No events: ${noEvents.length} | Errors: ${errors.length}`);
  
  return { batchNumber, results, batchTime, successful: successful.length, failed: failed.length };
}

async function testBatchedParallel() {
  console.log('🧪 TESTING BATCHED PARALLEL REGENERATION\n');
  console.log('=' . repeat(50) + '\n');
  
  const overallStart = Date.now();
  
  // Step 1: Get all person IDs
  const fetchResult = await getAllPersonIds();
  
  if (!fetchResult.success) {
    console.error('❌ Failed to fetch person IDs. Aborting.');
    return;
  }
  
  const { personIds, fetchTime } = fetchResult;
  const totalPeople = personIds.length;
  
  console.log(`📊 Found ${totalPeople} active people to process\n`);
  
  // Step 2: Split into batches of 100
  const batches = [];
  for (let i = 0; i < personIds.length; i += 100) {
    batches.push(personIds.slice(i, i + 100));
  }
  
  console.log(`📦 Split into ${batches.length} batches of up to 100 people each\n`);
  console.log('=' .repeat(50));
  
  // Step 3: Process each batch sequentially (each batch processes 100 in parallel)
  const batchResults = [];
  
  for (let i = 0; i < batches.length; i++) {
    const batchResult = await processBatch(batches[i], i + 1, batches.length);
    batchResults.push(batchResult);
    
    // Small pause between batches to avoid overwhelming server
    if (i < batches.length - 1) {
      console.log(`   ⏳ 5-second pause before next batch...\n`);
      await new Promise(resolve => setTimeout(resolve, 5000));
    }
  }
  
  const totalTime = Math.round((Date.now() - overallStart) / 1000);
  const regenTime = totalTime - fetchTime;
  
  const totalSuccess = batchResults.reduce((sum, b) => sum + b.successful, 0);
  const totalFailed = batchResults.reduce((sum, b) => sum + b.failed, 0);
  
  console.log('\n\n✨ ========== FINAL RESULTS ==========');
  console.log(`Total people: ${totalPeople}`);
  console.log(`Total batches: ${batches.length}`);
  console.log(`\n⏱️  Timing:`);
  console.log(`  Fetch IDs: ${fetchTime}s`);
  console.log(`  Regenerate: ${regenTime}s`);
  console.log(`  TOTAL: ${totalTime}s (${Math.round(totalTime / 60)} min ${totalTime % 60}s)`);
  
  console.log(`\n📊 Results:`);
  console.log(`  ✅ Successful: ${totalSuccess}`);
  console.log(`  ❌ Failed: ${totalFailed}`);
  console.log(`  Success rate: ${Math.round((totalSuccess / totalPeople) * 100)}%`);
  
  console.log('\n📈 Per-Batch Performance:');
  batchResults.forEach(b => {
    console.log(`  Batch ${b.batchNumber}: ${b.batchTime}s (${b.successful} success, ${b.failed} failed)`);
  });
  
  console.log('\n💡 Conclusion:');
  if (totalTime < 180) {
    console.log(`  ✅ EXCELLENT! All ${totalPeople} people processed in under 3 minutes`);
    console.log(`  ✅ 5-minute background cycle is FEASIBLE!`);
    console.log(`  ✅ Data will be ${Math.round(totalTime / 60)}-5 minutes old max`);
  } else if (totalTime < 300) {
    console.log(`  ⚠️  Took ${Math.round(totalTime / 60)} minutes - tight for 5-min cycle`);
    console.log(`  ⚠️  Consider 10-minute cycle instead`);
  } else {
    console.log(`  ❌ Too slow (${Math.round(totalTime / 60)} min) for 5-min cycle`);
    console.log(`  ❌ Need 10-15 minute cycle`);
  }
}

testBatchedParallel().catch(console.error);

