import 'dotenv/config';
import { Client } from '@notionhq/client';

const notion = new Client({ auth: process.env.NOTION_API_KEY });
const PERSONNEL_DB = process.env.PERSONNEL_DATABASE_ID;

async function findEricEngland() {
  try {
    console.log('🔍 Searching for Eric England in personnel database...');
    
    const response = await notion.databases.query({
      database_id: PERSONNEL_DB,
      filter: {
        property: 'Full Name',
        formula: {
          string: {
            contains: 'Eric England'
          }
        }
      }
    });
    
    if (response.results.length > 0) {
      const person = response.results[0];
      const personId = person.id;
      const fullName = person.properties['Full Name']?.formula?.string || 'Unknown';
      console.log('✅ Found Eric England:');
      console.log('Person ID:', personId);
      console.log('Full Name:', fullName);
      return personId;
    } else {
      console.log('❌ Eric England not found in personnel database');
      return null;
    }
  } catch (error) {
    console.error('❌ Error:', error.message);
    return null;
  }
}

findEricEngland();