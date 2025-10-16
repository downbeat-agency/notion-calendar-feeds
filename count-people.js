// Count how many people we actually have in the system

import dotenv from 'dotenv';
import { Client } from '@notionhq/client';

dotenv.config();

const notion = new Client({ auth: process.env.NOTION_API_KEY });
const PERSONNEL_DB = process.env.PERSONNEL_DATABASE_ID;

async function countPeople() {
  console.log('🔍 Counting total people in Personnel database...\n');
  
  const startTime = Date.now();
  
  try {
    let allPeople = [];
    let hasMore = true;
    let nextCursor = undefined;
    let pageCount = 0;
    
    while (hasMore) {
      pageCount++;
      console.log(`   Fetching page ${pageCount}...`);
      
      const response = await notion.databases.query({
        database_id: PERSONNEL_DB,
        page_size: 100,
        ...(nextCursor && { start_cursor: nextCursor })
      });
      
      console.log(`   Got ${response.results.length} people`);
      
      allPeople = [...allPeople, ...response.results];
      hasMore = response.has_more;
      nextCursor = response.next_cursor;
    }
    
    const totalTime = Math.round((Date.now() - startTime) / 1000);
    
    // Count active vs inactive
    const active = allPeople.filter(p => p.properties.Active?.checkbox === true);
    const inactive = allPeople.filter(p => p.properties.Active?.checkbox !== true);
    
    console.log('\n📊 ========== RESULTS ==========');
    console.log(`Total people in database: ${allPeople.length}`);
    console.log(`  ✅ Active: ${active.length}`);
    console.log(`  ⏭️  Inactive: ${inactive.length}`);
    console.log(`⏱️  Fetch time: ${totalTime}s\n`);
    
    console.log('💡 For background job:');
    if (active.length <= 100) {
      console.log(`  → 1 batch of ${active.length} in parallel (~60s)`);
      console.log(`  → 5-minute cycle: VERY COMFORTABLE`);
    } else if (active.length <= 200) {
      console.log(`  → 2 batches of 100 in parallel (~2min 10s)`);
      console.log(`  → 5-minute cycle: Feasible`);
      console.log(`  → 6-minute cycle: Comfortable ⭐`);
    } else if (active.length <= 300) {
      console.log(`  → 3 batches of 100 in parallel (~3min 15s)`);
      console.log(`  → 5-minute cycle: Tight`);
      console.log(`  → 6-minute cycle: Good ⭐`);
      console.log(`  → 7-minute cycle: Very comfortable`);
    } else {
      console.log(`  → ${Math.ceil(active.length / 100)} batches needed`);
      console.log(`  → Estimated time: ~${Math.ceil(active.length / 100) * 60}s`);
      console.log(`  → Recommend: ${Math.ceil((Math.ceil(active.length / 100) * 65) / 60) + 1}-minute cycle`);
    }
    
    return { total: allPeople.length, active: active.length, inactive: inactive.length };
    
  } catch (error) {
    const elapsed = Math.round((Date.now() - startTime) / 1000);
    console.error(`❌ Error after ${elapsed}s:`, error.message);
    return { error: error.message };
  }
}

countPeople().catch(console.error);

