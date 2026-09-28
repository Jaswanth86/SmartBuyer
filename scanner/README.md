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

## Product alert logic

V5 is designed around an early-move alert rather than a simple price-change alert.

Priority universe:
- USDT spot coins priced at or below `MAX_PRICE` (default $2).
- New-listing metadata is tracked when Binance provides an onboard/listing timestamp.
- New listings can be highlighted separately from older low-priced coins.

An alert should be generated only when the market evidence is strong enough:
- abnormal volume/notional acceleration
- abnormal trade-count acceleration
- aggressive buy/sell imbalance
- price velocity
- order-book imbalance/liquidity change
- agreement across multiple timeframes
- cooldown to avoid repeated alerts

News enrichment:
- For a triggered candidate, V5 queries Google News RSS for the coin/base asset and adds recent matching headlines to the Telegram message.
- This is news enrichment, not proof that the news caused the move.
- Social/influencer monitoring should be added through an authenticated X API integration or another licensed social-data provider. Do not scrape or impersonate private feeds.

## Selling as a subscription

The recommended commercial architecture is one scanner + one alert engine + many subscriber destinations.

Each subscriber should have:
- Telegram chat id
- subscription status
- plan
- expiry timestamp
- alert preferences (price ceiling, new-listing window, minimum score, cooldown)
- optional watched coins

The scanner remains shared; the delivery layer decides which subscribers receive each alert. Payment processing should be connected to a real subscription provider before accepting customers.
