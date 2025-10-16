// Test Personnel database integration
import dotenv from 'dotenv';
import { Client } from '@notionhq/client';

dotenv.config();

const notion = new Client({ auth: process.env.NOTION_API_KEY });
const PERSONNEL_DB = process.env.PERSONNEL_DATABASE_ID;

async function testPersonnelIntegration() {
  console.log('🔍 Testing Personnel database integration...\n');
  
  try {
    console.log('📋 Querying Personnel database...');
    const startTime = Date.now();
    
    const response = await notion.databases.query({
      database_id: PERSONNEL_DB,
      page_size: 100
    });
    
    const queryTime = Math.round((Date.now() - startTime) / 1000);
    
    console.log(`✅ Personnel query successful in ${queryTime}s`);
    console.log(`📊 Found ${response.results.length} people in Personnel database\n`);
    
    // Extract person IDs
    const personIds = response.results.map(page => page.id);
    
    console.log('👥 Sample person IDs:');
    personIds.slice(0, 5).forEach((id, index) => {
      console.log(`   ${index + 1}. ${id}`);
    });
    
    if (personIds.length > 5) {
      console.log(`   ... and ${personIds.length - 5} more`);
    }
    
    console.log('\n📈 Batch calculation:');
    const batchSize = 100;
    const batches = Math.ceil(personIds.length / batchSize);
    const estimatedTime = batches * 60 + (batches - 1) * 5; // 60s per batch + 5s pause
    
    console.log(`   Total people: ${personIds.length}`);
    console.log(`   Batches needed: ${batches} (${batchSize} people each)`);
    console.log(`   Estimated regeneration time: ${estimatedTime}s (${Math.ceil(estimatedTime / 60)}m ${estimatedTime % 60}s)`);
    
    if (estimatedTime < 300) {
      console.log(`   ✅ Perfect for 5-minute background refresh cycle!`);
    } else if (estimatedTime < 600) {
      console.log(`   ✅ Good for 10-minute background refresh cycle`);
    } else {
      console.log(`   ⚠️  Consider longer refresh cycle or smaller batches`);
    }
    
    console.log('\n🎉 Personnel database integration test PASSED!');
    return { success: true, personCount: personIds.length, personIds };
    
  } catch (error) {
    console.error('❌ Personnel database integration test FAILED:', error.message);
    console.error('Code:', error.code);
    return { success: false, error: error.message };
  }
}

testPersonnelIntegration().catch(console.error);
