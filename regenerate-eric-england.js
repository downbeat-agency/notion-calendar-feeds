// Script to regenerate Eric England's calendar
// We'll try to find his person ID by testing known person IDs

const BASE_URL = 'https://notion-calendar-feeds-production.up.railway.app';

// Known person IDs from the test files
const KNOWN_PERSON_IDS = [
  'ac294b7c-1907-4977-b5ba-191890a397a3', // Adrian
  '29aaf64b-cf86-4d3c-b117-3a58cf6c76f2', // Joshua
  '3cfd19e6-3a2e-4209-bd77-f523812c3f83', // Daniel
  'f5a0225c-3f0d-4d93-b1ee-ae5ab564d678', // Hubert
  'c35efa76-6cb1-4cac-abbf-97f4362a1fab', // David
  'fff39e4a-65a9-8147-9001-cb597f8b49e0', // Joseph
  '51b050cc-4765-41aa-be46-2a8e3b0632ba', // Christian
  'c7ba522b-1f47-4fb2-8996-2cdcdda578a9', // Alex
  '4d58fe1a-0687-44d0-90e3-e5b7b84967bb', // Joel
  '330ae3dd-b0c3-47d5-a660-ce3b1c925b75', // Gabriel
  '345984c3-1f94-4476-a27c-1b98f51c56d8', // Andrew
  '948e3520-8fd4-403f-8402-65d250161669', // Diego
  '20339e4a-65a9-8163-a33b-d5b359fbf7c0', // D'Nasya
  '10a39e4a-65a9-815d-896d-db7e38a6d96a', // Casey
  'c20ff1e6-c399-4d04-8981-90fd6a857b33', // Chezzarai
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
  '0d1f11cc-0ea7-4297-ad5b-0ba5d426e07f'
];

async function regeneratePerson(personId) {
  try {
    console.log(`🔄 Testing ${personId}...`);
    
    const response = await fetch(`${BASE_URL}/regenerate/${personId}`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json'
      }
    });
    
    const data = await response.json();
    
    if (data.success) {
      console.log(`✅ Found Eric England! Person ID: ${personId}`);
      console.log(`   Name: ${data.personName}`);
      console.log(`   Events: ${data.eventCount}`);
      return { success: true, personId, personName: data.personName, eventCount: data.eventCount };
    } else {
      console.log(`   ❌ Not Eric England: ${data.message || data.error}`);
      return { success: false, personId, error: data.message || data.error };
    }
  } catch (error) {
    console.log(`   ❌ Error: ${error.message}`);
    return { success: false, personId, error: error.message };
  }
}

async function findAndRegenerateEricEngland() {
  console.log('🔍 Searching for Eric England among known person IDs...\n');
  
  for (let i = 0; i < KNOWN_PERSON_IDS.length; i++) {
    const personId = KNOWN_PERSON_IDS[i];
    console.log(`[${i + 1}/${KNOWN_PERSON_IDS.length}] Testing ${personId.substring(0, 8)}...`);
    
    const result = await regeneratePerson(personId);
    
    if (result.success && result.personName && result.personName.toLowerCase().includes('eric')) {
      console.log('\n🎉 FOUND ERIC ENGLAND!');
      console.log(`Person ID: ${result.personId}`);
      console.log(`Name: ${result.personName}`);
      console.log(`Events: ${result.eventCount}`);
      console.log('\n✅ Eric England\'s calendar has been regenerated successfully!');
      return;
    }
    
    // Small delay to avoid overwhelming the API
    await new Promise(resolve => setTimeout(resolve, 1000));
  }
  
  console.log('\n❌ Eric England not found among the known person IDs.');
  console.log('You may need to check the personnel database directly or provide the correct person ID.');
}

// Run it
findAndRegenerateEricEngland().catch(error => {
  console.error('Fatal error:', error);
  process.exit(1);
});