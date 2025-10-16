// Test fetching ALL rows from Calendar Data database in one call

import dotenv from 'dotenv';
import { Client } from '@notionhq/client';

dotenv.config();

const notion = new Client({ auth: process.env.NOTION_API_KEY });
const CALENDAR_DATA_DB = process.env.CALENDAR_DATA_DATABASE_ID;

async function testFetchAll() {
  console.log('🧪 Testing single API call to fetch ALL Calendar Data rows...\n');
  
  const startTime = Date.now();
  
  try {
    console.log('⏱️  Starting query...');
    
    const response = await notion.databases.query({
      database_id: CALENDAR_DATA_DB,
      page_size: 100 // Max page size
    });
    
    const firstFetchTime = Math.round((Date.now() - startTime) / 1000);
    console.log(`✅ First page fetched: ${response.results.length} rows in ${firstFetchTime}s`);
    
    let allResults = [...response.results];
    let pageCount = 1;
    
    // Handle pagination if there are more results
    let hasMore = response.has_more;
    let nextCursor = response.next_cursor;
    
    while (hasMore) {
      pageCount++;
      console.log(`⏱️  Fetching page ${pageCount}...`);
      
      const pageStartTime = Date.now();
      const nextPage = await notion.databases.query({
        database_id: CALENDAR_DATA_DB,
        page_size: 100,
        start_cursor: nextCursor
      });
      
      const pageTime = Math.round((Date.now() - pageStartTime) / 1000);
      console.log(`✅ Page ${pageCount} fetched: ${nextPage.results.length} rows in ${pageTime}s`);
      
      allResults = [...allResults, ...nextPage.results];
      hasMore = nextPage.has_more;
      nextCursor = nextPage.next_cursor;
    }
    
    const totalTime = Math.round((Date.now() - startTime) / 1000);
    
    console.log('\n\n✨ ========== RESULTS ==========');
    console.log(`Total rows fetched: ${allResults.length}`);
    console.log(`Total pages: ${pageCount}`);
    console.log(`⏱️  Total time: ${totalTime}s`);
    console.log(`⏱️  Average per page: ${Math.round(totalTime / pageCount)}s`);
    
    // Count how many have personnel linked
    let withPersonnel = 0;
    let withoutPersonnel = 0;
    
    allResults.forEach(row => {
      const personnel = row.properties.Personnel?.relation || [];
      if (personnel.length > 0) {
        withPersonnel++;
      } else {
        withoutPersonnel++;
      }
    });
    
    console.log(`\n📊 Breakdown:`);
    console.log(`  ✅ With personnel: ${withPersonnel}`);
    console.log(`  ⏭️  Without personnel: ${withoutPersonnel}`);
    
    console.log('\n💡 Conclusion:');
    if (totalTime < 60) {
      console.log(`  ✅ EXCELLENT! Fetched ${allResults.length} rows in under 1 minute.`);
      console.log(`  ✅ Single API call approach is FEASIBLE!`);
      console.log(`  ✅ Could regenerate all ${withPersonnel} people's calendars in ~${totalTime + 30}s total`);
    } else if (totalTime < 120) {
      console.log(`  ⚠️  Fetched in ${totalTime}s - borderline for timeout (60s limit)`);
      console.log(`  ⚠️  Might need pagination handling`);
    } else {
      console.log(`  ❌ Too slow - took ${totalTime}s (exceeds 60s timeout)`);
      console.log(`  ❌ Need different approach`);
    }
    
    return { success: true, totalRows: allResults.length, withPersonnel, totalTime };
    
  } catch (error) {
    const elapsed = Math.round((Date.now() - startTime) / 1000);
    console.error(`\n❌ ERROR after ${elapsed}s:`, error.message);
    console.error('Code:', error.code);
    return { success: false, error: error.message, time: elapsed };
  }
}

testFetchAll().catch(console.error);

