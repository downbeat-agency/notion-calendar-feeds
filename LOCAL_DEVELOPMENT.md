# Local Development Setup for Railway Production

## Current Situation

Your Notion API key is configured on Railway production server, but you're running locally. Here are several ways to get your local development environment working:

## Option 1: Manual Environment Variables (Recommended)

### Step 1: Get Environment Variables from Railway

1. **Login to Railway CLI:**
   ```bash
   railway login
   ```

2. **Get your project variables:**
   ```bash
   railway variables
   ```

3. **Copy the values and create a local .env file:**
   ```bash
   # Create .env file with the values from Railway
   echo "NOTION_API_KEY=your_actual_notion_key_from_railway" > .env
   echo "PERSONNEL_DATABASE_ID=your_actual_database_id_from_railway" >> .env
   echo "PORT=3000" >> .env
   ```

### Step 2: Restart Local Server
```bash
# Stop current server (Ctrl+C) then restart
npm start
```

## Option 2: Use Railway CLI to Run Locally

### Step 1: Login to Railway
```bash
railway login
```

### Step 2: Link to Your Project
```bash
railway link
```

### Step 3: Run with Railway Environment
```bash
railway run npm start
```

This will automatically inject the Railway environment variables into your local process.

## Option 3: Copy from Railway Dashboard

1. Go to your Railway dashboard
2. Select your project
3. Go to "Variables" tab
4. Copy the `NOTION_API_KEY` and `PERSONNEL_DATABASE_ID` values
5. Create a local `.env` file with these values

## Testing the Fix

Once you have the environment variables set up:

1. **Test the calendar endpoint:**
   ```bash
   curl "http://localhost:3000/calendar/6b20e26d-c6c0-4af1-990d-9ee3e1418269"
   ```

2. **Test the debug endpoint:**
   ```bash
   curl "http://localhost:3000/debug/simple-test/6b20e26d-c6c0-4af1-990d-9ee3e1418269"
   ```

3. **Test the subscription page:**
   Open: `http://localhost:3000/subscribe/6b20e26d-c6c0-4af1-990d-9ee3e1418269`

## Understanding the Error

The webcal:// URL in your error dialog is pointing to:
`webcal://calendar.downbeat.agency/calendar/6b20e26d-c6c0-4af1-990d-9ee3e1418269`

This means:
- Your production server at `calendar.downbeat.agency` is working
- The person ID `6b20e26d-c6c0-4af1-990d-9ee3e1418269` exists in your Notion database
- The issue is only with your local development environment

## Next Steps

1. Choose one of the options above to set up your local environment
2. Once configured, your local webcal:// URLs will work
3. You can test calendar subscriptions locally before deploying to Railway

## Security Note

Never commit your `.env` file to version control. It's already in `.gitignore` for security.

