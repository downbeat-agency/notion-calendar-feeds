#!/usr/bin/env node

// Helper script to add person IDs to the system
// Usage: node add-person-id.js <person-id> [name]

import fs from 'fs';
import path from 'path';

const args = process.argv.slice(2);

if (args.length === 0) {
  console.log('📋 Usage: node add-person-id.js <person-id> [name]');
  console.log('');
  console.log('Examples:');
  console.log('  node add-person-id.js f5a0225c3f0d4d93b1eeae5ab564d678 "John Doe"');
  console.log('  node add-person-id.js 330ae3ddb0c347d5a660ce3b1c925b75');
  console.log('');
  console.log('💡 This will add the person ID to your .env file as PERSON_ID_N');
  process.exit(1);
}

const personId = args[0];
const name = args[1] || 'Unknown';

// Validate person ID format (should be 32 characters, lowercase with hyphens)
if (!/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/.test(personId)) {
  console.error('❌ Invalid person ID format. Expected format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx');
  process.exit(1);
}

const envPath = path.join(process.cwd(), '.env');

try {
  let envContent = '';
  let maxIndex = 0;
  
  // Read existing .env file
  if (fs.existsSync(envPath)) {
    envContent = fs.readFileSync(envPath, 'utf8');
    
    // Find the highest PERSON_ID_N index
    const matches = envContent.match(/PERSON_ID_(\d+)/g);
    if (matches) {
      matches.forEach(match => {
        const index = parseInt(match.split('_')[2]);
        if (index > maxIndex) maxIndex = index;
      });
    }
  }
  
  // Check if person ID already exists
  if (envContent.includes(personId)) {
    console.log(`⚠️  Person ID ${personId} already exists in .env file`);
    process.exit(0);
  }
  
  // Add new person ID
  const nextIndex = maxIndex + 1;
  const newLine = `PERSON_ID_${nextIndex}=${personId}  # ${name}`;
  
  if (envContent && !envContent.endsWith('\n')) {
    envContent += '\n';
  }
  envContent += `\n${newLine}`;
  
  // Write updated .env file
  fs.writeFileSync(envPath, envContent);
  
  console.log(`✅ Added person ID ${personId} (${name}) as PERSON_ID_${nextIndex}`);
  console.log('');
  console.log('📋 Next steps:');
  console.log('1. Deploy the updated .env file to your server');
  console.log('2. The system will automatically pick up the new person ID');
  console.log('3. Run /regenerate-all to test the new person ID');
  
} catch (error) {
  console.error('❌ Error updating .env file:', error.message);
  process.exit(1);
}
