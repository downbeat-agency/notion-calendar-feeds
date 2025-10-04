# Notion Calendar Feeds Setup Guide

## Issue Identified

The webcal:// subscription is failing because the server is missing required environment variables for the Notion API connection.

## Required Environment Variables

The application needs the following environment variables to connect to Notion:

1. **NOTION_API_KEY** - Your Notion integration API key
2. **PERSONNEL_DATABASE_ID** - The UUID of your Notion database containing personnel information

## Setup Steps

### 1. Create Environment File

Create a `.env` file in the project root with the following content:

```bash
# Notion Calendar Feeds Environment Variables
NOTION_API_KEY=your_notion_api_key_here
PERSONNEL_DATABASE_ID=your_personnel_database_id_here
PORT=3000
```

### 2. Get Your Notion API Key

1. Go to [https://www.notion.so/my-integrations](https://www.notion.so/my-integrations)
2. Create a new integration or use an existing one
3. Copy the "Internal Integration Token" - this is your `NOTION_API_KEY`

### 3. Get Your Personnel Database ID

1. Open your Notion database in a web browser
2. Look at the URL - it should look like:
   `https://www.notion.so/your-workspace/6b20e26d-c6c0-4af1-990d-9ee3e1418269`
3. The UUID part (`6b20e26d-c6c0-4af1-990d-9ee3e1418269`) is your `PERSONNEL_DATABASE_ID`

### 4. Grant Database Access

1. In your Notion database, click the "Share" button
2. Add your integration (use the integration name from step 2)
3. Grant "Read" permissions

### 5. Restart the Server

After creating the `.env` file with the correct values:

```bash
# Stop the current server (Ctrl+C)
# Then restart it
npm start
```

## Testing the Fix

Once the environment variables are set up, test the calendar subscription:

1. Visit: `http://localhost:3000/subscribe/6b20e26d-c6c0-4af1-990d-9ee3e1418269`
2. Try clicking the "Apple Calendar" button
3. The webcal:// URL should now work properly

## Troubleshooting

If you're still getting errors:

1. Check that the person ID exists in your Notion database
2. Verify the integration has access to the database
3. Check server logs for detailed error messages
4. Test the debug endpoint: `http://localhost:3000/debug/simple-test/6b20e26d-c6c0-4af1-990d-9ee3e1418269`

## Current Status

- ✅ Server is running on port 3000
- ✅ Subscription page loads correctly
- ❌ Calendar endpoint fails due to missing Notion API credentials
- ❌ Webcal:// URLs fail because calendar generation fails

After setting up the environment variables, all functionality should work correctly.

