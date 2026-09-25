# Crypto Radar AI V5 Scanner

This worker is designed to run continuously outside Netlify. It consumes Binance WebSocket market data and sends anomaly alerts to Telegram.

Environment variables:
- TELEGRAM_BOT_TOKEN: secret bot token; never commit it.
- TELEGRAM_CHAT_ID: optional destination chat id.
- PORT: default 8787.
- MAX_SYMBOLS: default 250.
- MIN_ALERT_SCORE: default 78.
- ALERT_COOLDOWN_MS: default 300000.

Telegram setup:
1. Open your bot and send /start.
2. Run the worker with TELEGRAM_BOT_TOKEN configured as a secret.
3. If TELEGRAM_CHAT_ID is supplied, V5 sends there. Otherwise V5 attempts to discover the latest chat id from Telegram updates.
4. Keep the bot token only in the hosting provider secret/environment settings.

Market engine:
V5 listens to Binance aggTrade, bookTicker, depth@100ms and all supported spot kline intervals: 1s, 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w and 1M. It uses batched WebSocket connections so the per-connection stream limit is respected. The engine measures price velocity, volume acceleration, trade acceleration, aggressive buy/sell notional, order-book signals, liquidity changes and multi-timeframe agreement.

This is an anomaly scanner, not a guarantee that a coin will pump or dump.

Run:
npm install
TELEGRAM_BOT_TOKEN=your-secret npm start

Health endpoint:
GET /health

For 24/7 operation, deploy this folder as a persistent Node service or container. Netlify Functions are not intended to hold a WebSocket open continuously.