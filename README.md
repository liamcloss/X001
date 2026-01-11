# Trading 212 Signal Pipeline

## Setup

1. Copy the example environment file and fill in your credentials:

   ```bash
   cp .env.example .env
   ```

2. Update `.env` with your Trading 212 and Telegram credentials:

   - `T212_API_KEY`: Trading 212 REST API key.
   - `T212_TRADING_SECRET`: Trading 212 trading secret for request signing.
   - `TELEGRAM_BOT_TOKEN`: Telegram bot token used to send alerts.
   - `TELEGRAM_CHAT_ID`: Telegram chat ID that receives alerts.

The application loads `.env` from the repo root when `main.py` starts, so all modules share the same configuration.
