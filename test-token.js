import { Client } from '@notionhq/client';

// Replace this with your actual Notion API token (starts with ntn_)
const NOTION_API_KEY = process.env.NOTION_API_KEY || 'paste_your_ntn_token_here';

const notion = new Client({ auth: NOTION_API_KEY });

async function testToken() {
  try {
    const response = await notion.users.me();
    console.log('✅ Token is valid!');
    console.log('User:', response.name || 'Bot');
    console.log('Workspace:', response.bot?.owner?.workspace_name || 'N/A');
  } catch (error) {
    console.log('❌ Token is invalid:');
    console.log(error.body || error.message);
  }
}

testToken();
