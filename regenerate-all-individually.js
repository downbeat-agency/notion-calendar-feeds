// Script to regenerate ALL calendars one person at a time
// Automatically fetches list of personnel from Notion
// More fault-tolerant than the bulk endpoint

import dotenv from 'dotenv';
import { Client } from '@notionhq/client';

dotenv.config();

const notion = new Client({ auth: process.env.NOTION_API_KEY });
const CALENDAR_DATA_DB = process.env.CALENDAR_DATA_DATABASE_ID;

const BASE_URL = 'https://notion-calendar-feeds-production.up.railway.app';
// const BASE_URL = 'http://localhost:8080'; // For local testing

const DELAY_BETWEEN_PEOPLE = 15000; // 15 seconds between each person

async function getAllPersonIds() {
  console.log('🔍 Fetching all personnel IDs from Calendar Data database...');
  
  try {
    const response = await notion.databases.query({
      database_id: CALENDAR_DATA_DB
    });
    
    const personIds = [];
    
    for (const page of response.results) {
      const personnelRelations = page.properties.Personnel?.relation || [];
      
      if (personnelRelations.length > 0) {
        const personId = personnelRelations[0].id;
        personIds.push(personId);
      }
    }
    
    console.log(`✅ Found ${personIds.length} people with calendar data\n`);
    return personIds;
    
  } catch (error) {
    console.error('❌ Error fetching personnel IDs:', error.message);
    throw error;
  }
}

async function regeneratePerson(personId) {
  try {
    console.log(`🔄 Regenerating calendar for ${personId}...`);
    
    const response = await fetch(`${BASE_URL}/regenerate/${personId}`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json'
      }
    });
    
    const data = await response.json();
    
    if (data.success) {
      console.log(`   ✅ Success! Generated ${data.eventCount} events for ${data.personName}`);
      return { success: true, personId, personName: data.personName, eventCount: data.eventCount };
    } else {
      console.log(`   ❌ Failed: ${data.message || data.error}`);
      return { success: false, personId, error: data.message || data.error };
    }
  } catch (error) {
    console.log(`   ❌ Error: ${error.message}`);
    return { success: false, personId, error: error.message };
  }
}

async function regenerateAll() {
  console.log('🚀 Starting individual calendar regeneration...\n');
  
  // Get all person IDs
  const personIds = await getAllPersonIds();
  
  console.log(`⏱️  Processing ${personIds.length} people with ${DELAY_BETWEEN_PEOPLE / 1000}s delay between each`);
  console.log(`⏱️  Estimated time: ~${Math.round((personIds.length * (DELAY_BETWEEN_PEOPLE + 10000)) / 60000)} minutes\n`);
  
  const results = [];
  let successCount = 0;
  let failCount = 0;
  let skippedCount = 0;
  
  const startTime = Date.now();
  
  for (let i = 0; i < personIds.length; i++) {
    const personId = personIds[i];
    console.log(`\n📅 [${i + 1}/${personIds.length}] ${personId}`);
    
    const result = await regeneratePerson(personId);
    results.push(result);
    
    if (result.success) {
      successCount++;
    } else if (result.error && result.error.includes('no events')) {
      skippedCount++;
    } else {
      failCount++;
    }
    
    // Show progress
    const elapsed = Math.round((Date.now() - startTime) / 1000);
    const remaining = Math.round(((personIds.length - i - 1) * (DELAY_BETWEEN_PEOPLE + 10000)) / 1000);
    console.log(`   ⏱️  Elapsed: ${elapsed}s | Remaining: ~${remaining}s`);
    
    // Wait before processing next person (except for last one)
    if (i < personIds.length - 1) {
      await new Promise(resolve => setTimeout(resolve, DELAY_BETWEEN_PEOPLE));
    }
  }
  
  const totalTime = Math.round((Date.now() - startTime) / 1000);
  
  console.log('\n\n✨ ========== SUMMARY ==========');
  console.log(`Total: ${personIds.length}`);
  console.log(`✅ Success: ${successCount}`);
  console.log(`⏭️  Skipped (no events): ${skippedCount}`);
  console.log(`❌ Failed: ${failCount}`);
  console.log(`⏱️  Total time: ${totalTime}s (${Math.round(totalTime / 60)} minutes)`);
  
  console.log('\n📊 Successful generations:');
  results.filter(r => r.success).forEach(result => {
    console.log(`  ✅ ${result.personName}: ${result.eventCount} events`);
  });
  
  if (failCount > 0) {
    console.log('\n⚠️  Failed generations:');
    results.filter(r => !r.success && !r.error?.includes('no events')).forEach(result => {
      console.log(`  ❌ ${result.personId}: ${result.error}`);
    });
  }
  
  console.log('\n✅ Done! All calendars that could be generated are now cached.');
  console.log('🔄 Background job will keep them updated every 30 minutes.');
}

// Run it
regenerateAll().catch(error => {
  console.error('Fatal error:', error);
  process.exit(1);
});

