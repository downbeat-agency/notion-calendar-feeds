// Test fetching ALL rows from Calendar Data with pagination (100 at a time)

import dotenv from 'dotenv';
import { Client } from '@notionhq/client';

dotenv.config();

const notion = new Client({ auth: process.env.NOTION_API_KEY });
const CALENDAR_DATA_DB = process.env.CALENDAR_DATA_DATABASE_ID;

async function testFetchAllWithPagination() {
  console.log('🧪 Testing paginated fetch of ALL Calendar Data...\n');
  console.log('📋 Fetching 100 rows at a time with pagination\n');
  
  const overallStartTime = Date.now();
  
  try {
    let allResults = [];
    let hasMore = true;
    let nextCursor = undefined;
    let pageCount = 0;
    
    // Fetch all pages
    while (hasMore) {
      pageCount++;
      const pageStartTime = Date.now();
      
      console.log(`⏱️  Fetching page ${pageCount}...`);
      
      const response = await notion.databases.query({
        database_id: CALENDAR_DATA_DB,
        page_size: 100,
        ...(nextCursor && { start_cursor: nextCursor })
      });
      
      const pageTime = Math.round((Date.now() - pageStartTime) / 1000);
      console.log(`   ✅ Got ${response.results.length} rows in ${pageTime}s`);
      
      allResults = [...allResults, ...response.results];
      hasMore = response.has_more;
      nextCursor = response.next_cursor;
      
      if (hasMore) {
        console.log(`   ↪️  More data available, fetching next page...\n`);
      }
    }
    
    const totalTime = Math.round((Date.now() - overallStartTime) / 1000);
    
    // Analyze the data
    let withPersonnel = 0;
    let withoutPersonnel = 0;
    const personIds = [];
    
    allResults.forEach(row => {
      const personnel = row.properties.Personnel?.relation || [];
      if (personnel.length > 0) {
        withPersonnel++;
        personIds.push(personnel[0].id);
      } else {
        withoutPersonnel++;
      }
    });
    
    console.log('\n\n✨ ========== PAGINATION TEST RESULTS ==========');
    console.log(`Total pages fetched: ${pageCount}`);
    console.log(`Total rows: ${allResults.length}`);
    console.log(`⏱️  Total time: ${totalTime}s (${Math.round(totalTime / 60)} min ${totalTime % 60}s)`);
    console.log(`⏱️  Average per page: ${Math.round(totalTime / pageCount)}s`);
    
    console.log(`\n📊 Data Breakdown:`);
    console.log(`  ✅ With personnel linked: ${withPersonnel}`);
    console.log(`  ⏭️  Without personnel: ${withoutPersonnel}`);
    console.log(`  📋 Unique person IDs: ${new Set(personIds).size}`);
    
    console.log('\n💡 Feasibility Check:');
    if (totalTime < 120) {
      console.log(`  ✅ EXCELLENT! Fetched ${allResults.length} rows in ${totalTime}s`);
      console.log(`  ✅ Pagination approach is FEASIBLE!`);
      console.log(`  ✅ Can fetch all data + regenerate ${withPersonnel} calendars in parallel`);
      console.log(`  ✅ Estimated total time: ${totalTime}s (fetch) + ~60s (parallel regen) = ~${totalTime + 60}s`);
    } else if (totalTime < 300) {
      console.log(`  ⚠️  Took ${totalTime}s - workable but getting slow`);
      console.log(`  ⚠️  Total cycle time: ~${Math.round((totalTime + 60) / 60)} minutes`);
    } else {
      console.log(`  ❌ Too slow - took ${totalTime}s (${Math.round(totalTime / 60)} min)`);
      console.log(`  ❌ May need different approach`);
    }
    
    console.log('\n🎯 Next Step:');
    console.log(`  → Process ${withPersonnel} people in batches of 100 parallel`);
    console.log(`  → Batches needed: ${Math.ceil(withPersonnel / 100)}`);
    console.log(`  → Estimated regen time: ${Math.ceil(withPersonnel / 100)} × 60s = ~${Math.ceil(withPersonnel / 100)}min`);
    
    return { success: true, totalRows: allResults.length, withPersonnel, totalTime, pageCount };
    
  } catch (error) {
    const elapsed = Math.round((Date.now() - overallStartTime) / 1000);
    console.error(`\n❌ ERROR after ${elapsed}s:`, error.message);
    console.error('Code:', error.code);
    return { success: false, error: error.message, time: elapsed };
  }
}

testFetchAllWithPagination().catch(console.error);

