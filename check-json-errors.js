// Check which people have JSON parse errors

const BASE_URL = 'https://notion-calendar-feeds-production.up.railway.app';

// All the people that failed from batch 2
const FAILED_IDS = [
  '19139e4a-65a9-803d-a097-d26b61d25feb',
  '19139e4a-65a9-8069-a3c6-e5124edc5b99',
  '19139e4a-65a9-80a3-b4f6-dd5f16d5460a',
  '17339e4a-65a9-802e-a8f2-cd7f6dc59c1f',
  'cce05de8-3b00-497d-8cab-2f3aad4e843c',
  '10b39e4a-65a9-8156-88a6-d3bf9a15855c',
  '97192f16-33cd-45e6-a9a3-75f106284b54',
  'e1799ad5-57fd-465a-bb22-69540548cd7c',
  '192ee879-2892-4368-8550-9a36a338d4db',
  '9ad631ad-2077-4233-9e38-c0e24e34110e',
  '550b8ac9-8e03-4a03-b985-61c0a05aff05',
  'db72170f-9d3c-46cc-a1f1-3dcf303cad54',
  'd535a4ad-592c-43ba-a7e1-f8a090851b18',
  '0d1f11cc-0ea7-4297-ad5b-0ba5d426e07f',
  '26036abf-7d5a-492c-98e0-06ab4bce14e7',
  'c32e8758-a1e8-4f9e-be7a-e5aeba078f27',
  '1b42c721-c54a-4cc9-9c09-7da8cf0935c0',
];

async function checkPerson(personId) {
  try {
    const response = await fetch(`${BASE_URL}/regenerate/${personId}`);
    const data = await response.json();
    return { personId, ...data };
  } catch (error) {
    return { personId, error: error.message };
  }
}

async function checkAll() {
  console.log('🔍 Checking all failed people to identify JSON errors...\n');
  
  const jsonErrors = [];
  const noEvents = [];
  const otherErrors = [];
  
  for (let i = 0; i < FAILED_IDS.length; i++) {
    const personId = FAILED_IDS[i];
    process.stdout.write(`Checking ${i + 1}/${FAILED_IDS.length}... `);
    
    const result = await checkPerson(personId);
    
    if (result.reason && result.reason.includes('JSON parse error')) {
      console.log('⚠️  JSON ERROR');
      jsonErrors.push(result);
    } else if (result.reason === 'no_events') {
      console.log('⏭️  No events');
      noEvents.push(result);
    } else {
      console.log(`❌ ${result.message || result.error || 'Unknown'}`);
      otherErrors.push(result);
    }
    
    // Small delay to avoid overwhelming the server
    await new Promise(resolve => setTimeout(resolve, 500));
  }
  
  console.log('\n\n========== RESULTS ==========\n');
  
  if (jsonErrors.length > 0) {
    console.log(`⚠️  JSON PARSE ERRORS (${jsonErrors.length}):`);
    jsonErrors.forEach(result => {
      console.log(`\nPerson ID: ${result.personId}`);
      console.log(`Error: ${result.reason}`);
    });
  }
  
  if (noEvents.length > 0) {
    console.log(`\n\n⏭️  NO EVENTS (${noEvents.length}):`);
    noEvents.forEach(result => {
      console.log(`  - ${result.personId}`);
    });
  }
  
  if (otherErrors.length > 0) {
    console.log(`\n\n❌ OTHER ERRORS (${otherErrors.length}):`);
    otherErrors.forEach(result => {
      console.log(`  - ${result.personId}: ${result.message || result.error}`);
    });
  }
}

checkAll().catch(console.error);
