// Try to find Eric England by testing common UUID patterns

const BASE_URL = 'https://notion-calendar-feeds-production.up.railway.app';

// Common UUID patterns that might be Eric England
const POSSIBLE_ERIC_IDS = [
  // Try some common patterns based on the existing IDs
  'a364001a-0ebf-439b-b114-9fb4f64c8b3a', // Already tested - Joshua Ferrer
  '52151e7a-9823-47dd-8232-2817d8d70a2f', // Already tested - failed
  '19139e4a-65a9-803d-a097-d26b61d25feb', // Already tested - failed
  '19139e4a-65a9-8069-a3c6-e5124edc5b99', // Already tested - failed
  '19139e4a-65a9-80a3-b4f6-dd5f16d5460a', // Already tested - failed
  '17339e4a-65a9-802e-a8f2-cd7f6dc59c1f', // Already tested - failed
  '550b8ac9-8e03-4a03-b985-61c0a05aff05', // Already tested - failed
  'db72170f-9d3c-46cc-a1f1-3dcf303cad54', // Already tested - failed
  
  // Try some new patterns that might be Eric England
  'a364001a-0ebf-439b-b114-9fb4f64c8b3b', // Variation of Joshua's ID
  '52151e7a-9823-47dd-8232-2817d8d70a2e', // Variation of failed ID
  '19139e4a-65a9-803d-a097-d26b61d25fec', // Variation of failed ID
  '19139e4a-65a9-8069-a3c6-e5124edc5b98', // Variation of failed ID
  '19139e4a-65a9-80a3-b4f6-dd5f16d5460b', // Variation of failed ID
  '17339e4a-65a9-802e-a8f2-cd7f6dc59c1e', // Variation of failed ID
  '550b8ac9-8e03-4a03-b985-61c0a05aff06', // Variation of failed ID
  'db72170f-9d3c-46cc-a1f1-3dcf303cad55', // Variation of failed ID
  
  // Try some completely new patterns
  'e364001a-0ebf-439b-b114-9fb4f64c8b3a', // E for Eric
  'e2151e7a-9823-47dd-8232-2817d8d70a2f', // E for Eric
  'e9139e4a-65a9-803d-a097-d26b61d25feb', // E for Eric
  'e9139e4a-65a9-8069-a3c6-e5124edc5b99', // E for Eric
  'e9139e4a-65a9-80a3-b4f6-dd5f16d5460a', // E for Eric
  'e7339e4a-65a9-802e-a8f2-cd7f6dc59c1f', // E for Eric
  'e50b8ac9-8e03-4a03-b985-61c0a05aff05', // E for Eric
  'edb72170f-9d3c-46cc-a1f1-3dcf303cad54', // E for Eric
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
  console.log('🔍 Trying to find Eric England by testing UUID patterns...\n');
  console.log(`Testing ${POSSIBLE_ERIC_IDS.length} possible person IDs...\n`);
  
  const results = [];
  let foundEric = false;
  
  for (let i = 0; i < POSSIBLE_ERIC_IDS.length; i++) {
    const personId = POSSIBLE_ERIC_IDS[i];
    const result = await testPerson(personId, i, POSSIBLE_ERIC_IDS.length);
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
    console.log('\n❌ Eric England not found among the tested patterns.');
    console.log('Eric England might not have a person ID in the system, or it might be different from the patterns tested.');
  }
}

// Run it
findEricEngland().catch(error => {
  console.error('Fatal error:', error);
  process.exit(1);
});