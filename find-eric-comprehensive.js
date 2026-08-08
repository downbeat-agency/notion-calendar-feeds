// Comprehensive script to find Eric England among all known person IDs

const BASE_URL = 'https://notion-calendar-feeds-production.up.railway.app';

// All person IDs from various test files
const ALL_PERSON_IDS = [
  // From test-parallel-100.js and other files
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
  '1ec39e4a-65a9-819a-a157-e498c7a8be90',
  '9ceddff1-ca4e-429f-952d-5110d5819613',
  '10a39e4a-65a9-815d-896d-db7e38a6d96a', // Casey
  'c20ff1e6-c399-4d04-8981-90fd6a857b33', // Chezzarai
  'c13e1f17-d797-4626-ba8a-a9935bb0e154', // Greylyn
  '6b02aa2e-9cd4-4e67-a0ec-1d9c25ffe4fc', // David Cano
  '426643e3-dd75-4e10-ba7b-117ff820bd52', // Jacquelyn
  'c9144be1-0f8d-4881-9c54-6373d879fac9', // Shane
  '4bb84d1b-a2b3-4177-aaed-85c88633bf60', // Natalie
  '88d00f44-30c2-4e18-8a8f-67d0cc7088a3', // Daniel Wirick
  'c890fd84-2dcf-4ee7-941f-4c946833af0c', // Elizabeth
  '89fba2d8-faaa-4ce9-bc73-4b616d5a5707', // Charles
  'cce05de8-3b00-497d-8cab-2f3aad4e843c', // Marcus
  '10b39e4a-65a9-8156-88a6-d3bf9a15855c', // Pedro
  'e1799ad5-57fd-465a-bb22-69540548cd7c', // Shannon
  '9ad631ad-2077-4233-9e38-c0e24e34110e', // Skye
  '26036abf-7d5a-492c-98e0-06ab4bce14e7', // Brandon
  'c32e8758-a1e8-4f9e-be7a-e5aeba078f27', // Brennan
  '1b42c721-c54a-4cc9-9c09-7da8cf0935c0', // Briana
  '97192f16-33cd-45e6-a9a3-75f106284b54', // Joseph Leone
  '0d1f11cc-0ea7-4297-ad5b-0ba5d426e07f', // Anna
  
  // Additional IDs from batch 2
  '19139e4a-65a9-803d-a097-d26b61d25feb',
  '19139e4a-65a9-8069-a3c6-e5124edc5b99',
  '19139e4a-65a9-80a3-b4f6-dd5f16d5460a',
  '17339e4a-65a9-802e-a8f2-cd7f6dc59c1f',
  '192ee879-2892-4368-8550-9a36a338d4db',
  '550b8ac9-8e03-4a03-b985-61c0a05aff05',
  'db72170f-9d3c-46cc-a1f1-3dcf303cad54',
  'd535a4ad-592c-43ba-a7e1-f8a090851b18',
  '18e3a82c-4d20-4f55-86cd-0ef5ee782f18',
  '1bf61721-41c8-4a5d-a700-d1a48f29981a',
  '82116a82-8d06-4aaa-8947-951950d0243f',
  '6b20e26d-c6c0-4af1-990d-9ee3e1418269',
  
  // Additional IDs from other files
  'a364001a-0ebf-439b-b114-9fb4f64c8b3a',
  '52151e7a-9823-47dd-8232-2817d8d70a2f'
];

async function testPerson(personId, index, total) {
  try {
    console.log(`[${index + 1}/${total}] Testing ${personId.substring(0, 8)}...`);
    
    const response = await fetch(`${BASE_URL}/regenerate/${personId}`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json'
      }
    });
    
    const data = await response.json();
    
    if (data.success) {
      const name = data.personName || 'Unknown';
      console.log(`   ✅ ${name} (${data.eventCount} events)`);
      
      // Check if this is Eric England
      if (name.toLowerCase().includes('eric') && name.toLowerCase().includes('england')) {
        console.log(`\n🎉 FOUND ERIC ENGLAND!`);
        console.log(`Person ID: ${personId}`);
        console.log(`Name: ${name}`);
        console.log(`Events: ${data.eventCount}`);
        return { found: true, personId, name, eventCount: data.eventCount };
      }
      
      return { found: false, personId, name, eventCount: data.eventCount };
    } else {
      console.log(`   ❌ Failed: ${data.message || data.error}`);
      return { found: false, personId, error: data.message || data.error };
    }
  } catch (error) {
    console.log(`   ❌ Error: ${error.message}`);
    return { found: false, personId, error: error.message };
  }
}

async function findEricEngland() {
  console.log('🔍 Searching for Eric England among all known person IDs...\n');
  console.log(`Testing ${ALL_PERSON_IDS.length} person IDs...\n`);
  
  const results = [];
  let foundEric = false;
  
  for (let i = 0; i < ALL_PERSON_IDS.length; i++) {
    const personId = ALL_PERSON_IDS[i];
    const result = await testPerson(personId, i, ALL_PERSON_IDS.length);
    results.push(result);
    
    if (result.found) {
      foundEric = true;
      console.log(`\n✅ Eric England's calendar has been regenerated successfully!`);
      break;
    }
    
    // Small delay to avoid overwhelming the API
    await new Promise(resolve => setTimeout(resolve, 500));
  }
  
  if (!foundEric) {
    console.log('\n❌ Eric England not found among the known person IDs.');
    console.log('You may need to check the personnel database directly or provide the correct person ID.');
    
    // Show summary of all people found
    console.log('\n📊 Summary of all people found:');
    results.filter(r => r.name).forEach(result => {
      console.log(`  ${result.name}: ${result.eventCount} events`);
    });
  }
}

// Run it
findEricEngland().catch(error => {
  console.error('Fatal error:', error);
  process.exit(1);
});