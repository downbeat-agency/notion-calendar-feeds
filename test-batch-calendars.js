#!/usr/bin/env node

import { exec } from 'child_process';
import { promisify } from 'util';

const execAsync = promisify(exec);

const personIds = [
  '6b20e26dc6c04af1990d9ee3e1418269',
  '51b050cc476541aabe462a8e3b0632ba',
  '345984c31f944476a27c1b98f51c56d8',
  '4d58fe1a068744d090e3e5b7b84967bb',
  'ac294b7c19074977b5ba191890a397a3',
  'c35efa766cb14cacabbf97f4362a1fab',
  '29aaf64bcf864d3cb1173a58cf6c76f2',
  '10b39e4a65a9815688a6d3bf9a15855c',
  '330ae3ddb0c347d5a660ce3b1c925b75',
  '1b42c721c54a4cc99c097da8cf0935c0',
  'a658a27a30824e5fa3250cfe1752cc81',
  '3d337c8b8c8149a584371499162c5954',
  '18e3a82c4d204f5586cd0ef5ee782f18',
  '26036abf7d5a492c98e006ab4bce14e7',
  'cce05de83b00497d8cab2f3aad4e843c',
  '05aedbfe70b6408fa4102a74665c1912',
  'dffbe646e4b34792baed3d4399fb5986',
  'f4a6662525464368b322f67c03aec53a',
  'f5a0225c3f0d4d93b1eeae5ab564d678',
  'c20ff1e6c3994d04898190fd6a857b33',
  '948e35208fd4403f840265d250161669',
  'fff39e4a65a981479001cb597f8b49e0',
  '8326e63bac3440ea9f4dda28003012a4',
  '88d00f4430c24e188a8f67d0cc7088a3',
  'c13e1f17d7974626ba8aa9935bb0e154',
  '426643e3dd754e10ba7b117ff820bd52',
  '7d6f17dd1b5b42c19c41a38ccedd0888',
  '3cfd19e63a2e4209bd77f523812c3f83',
  'de2564fc975148ee9208efad3d135a04',
  'f8ea7e56c628483e98a4c1e85b76e0df',
  'aafb082b5e574449b1946484aa7f7417',
  '1ff8f29369154061b7b0c8f9b40fc2c6',
  'dd1e204a0d554e20b831925a87713d6c',
  '2394be187c54430eb139aac78569b4dd',
  'a1c1055d82504dd3b79180f2abcf1291',
  'ada4539830b44390aac94bd9a781f30c',
  '82116a828d064aaa8947951950d0243f',
  'c32e8758a1e84f9ebe7ae5aeba078f27',
  'a40bc11943534fd6a2ef98381b6e0cd5',
  'a75ae8b931aa48d988329eb0e348d093',
  '37ef1692a32d449c89e3e0325ff85689',
  '97192f1633cd45e6a9a375f106284b54',
  '9ad631ad207742339e38c0e24e34110e',
  '1bf6172141c84a5da700d1a48f29981a',
  '192ee8792892436885509a36a338d4db',
  '8f20de36c119440fbb2b82afeb14f8f9',
  '9c185e2578094ffd968fe082826f43f7',
  'e1799ad557fd465abb2269540548cd7c',
  '0d1f11cc0ea74297ad5b0ba5d426e07f',
  'a364001a0ebf439bb1149fb4f64c8b3a',
  '1ed39e4a65a981cfb792ff1ee6b73a26',
  '5ed6b4a7ebe94982878654376b94df07'
];

async function testCalendar(personId) {
  
  try {
    const startTime = Date.now();
    const { stdout } = await execAsync(`curl -s -X GET "http://localhost:3000/regenerate/${personId}"`);
    const endTime = Date.now();
    const duration = Math.round((endTime - startTime) / 1000);
    
    const result = JSON.parse(stdout);
    
    if (result.success) {
      return {
        personId,
        status: 'SUCCESS',
        personName: result.personName,
        eventCount: result.eventCount,
        duration: `${duration}s`
      };
    } else {
      return {
        personId,
        status: 'FAILED',
        error: result.reason || result.error,
        duration: `${duration}s`
      };
    }
  } catch (error) {
    return {
      personId,
      status: 'ERROR',
      error: error.message,
      duration: 'N/A'
    };
  }
}

async function testAllCalendars() {
  console.log(`🧪 Testing ${personIds.length} calendars for malformed data...\n`);
  
  const results = [];
  let successCount = 0;
  let failedCount = 0;
  let errorCount = 0;
  
  for (let i = 0; i < personIds.length; i++) {
    const personId = personIds[i];
    const progress = `[${i + 1}/${personIds.length}]`;
    
    console.log(`${progress} Testing ${personId}...`);
    
    const result = await testCalendar(personId);
    results.push(result);
    
    if (result.status === 'SUCCESS') {
      successCount++;
      console.log(`  ✅ ${result.personName} - ${result.eventCount} events (${result.duration})`);
    } else if (result.status === 'FAILED') {
      failedCount++;
      console.log(`  ❌ FAILED: ${result.error}`);
    } else {
      errorCount++;
      console.log(`  ⚠️  ERROR: ${result.error}`);
    }
    
    // Add small delay to avoid overwhelming the server
    if (i < personIds.length - 1) {
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
  }
  
  console.log('\n📊 SUMMARY:');
  console.log(`✅ Success: ${successCount}`);
  console.log(`❌ Failed: ${failedCount}`);
  console.log(`⚠️  Errors: ${errorCount}`);
  console.log(`📋 Total: ${personIds.length}`);
  
  if (failedCount > 0 || errorCount > 0) {
    console.log('\n🔍 FAILED CALENDARS:');
    results.filter(r => r.status !== 'SUCCESS').forEach(result => {
      console.log(`  ${result.personId}: ${result.error}`);
    });
  }
  
  if (successCount === personIds.length) {
    console.log('\n🎉 All calendars are working correctly - no malformed data detected!');
  }
}

testAllCalendars().catch(console.error);
