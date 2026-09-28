# Crypto Radar AI V6 — News-First Market Reaction Scanner

V6 changes the alert order to:

**NEWS DETECTED → NEWS INVESTIGATED → MARKET MOVEMENT OBSERVED → REACTION CONFIRMED → TELEGRAM ALERT**

The scanner does not alert merely because a headline exists. It first searches related coverage, classifies the event, estimates corroboration, then waits for post-publication market evidence.

## V6 flow

1. **News discovery**
   - Polls recent crypto news through Google News RSS.
   - Prioritizes Binance USDT spot coins at or below `MAX_PRICE` (default $2).
   - Newly listed coins are prioritized when Binance provides listing/onboard metadata.

2. **News investigation**
   - Searches the headline again for related coverage.
   - Counts independent source domains.
   - Classifies the event: security, listing/launch, partnership, network, regulation, capital, tokenomics or market/project.
   - Stores the investigation while waiting for enough post-news candles.

3. **Market reaction**
   - Uses 1-minute Binance candles after the article timestamp.
   - Compares price, quote volume and trade count with the pre-news baseline.
   - Measures taker-buy share and current order-book imbalance.
   - Waits up to 25 minutes for evidence rather than firing immediately.

4. **Alert decision**
   - Reaction score must reach `MIN_REACTION_SCORE` (default 72).
   - The alert identifies the news, sources, confidence, price reaction, volume acceleration, trade acceleration and buy/sell pressure.
   - The message explicitly says that time alignment does not prove causation.

## Market data

V6 listens to Binance `aggTrade`, `bookTicker`, `depth@100ms` and 1m/5m/15m/1h/4h kline streams. Binance documents combined WebSocket streams and a 1024-stream-per-connection limit; V6 batches subscriptions accordingly.

## Environment

- `TELEGRAM_BOT_TOKEN` — secret; never commit it.
- `TELEGRAM_CHAT_ID` — optional fixed destination. Otherwise the worker discovers the latest chat after the user starts the bot.
- `MAX_SYMBOLS` — default 500.
- `MAX_PRICE` — default 2.
- `NEW_COIN_DAYS` — default 30.
- `MIN_REACTION_SCORE` — default 72.
- `NEWS_POLL_MS` — default 30000.
- `ALERT_COOLDOWN_MS` — default 900000.
- `NEWS_LOOKBACK_HOURS` — default 2.
- `MAX_NEWS_CANDIDATES` — default 40.

## Health endpoints

- `GET /health`
- `GET /news-status`

## Important product rule

This is a market-reaction intelligence system, not a guaranteed pump/dump predictor. News can be coincidental, delayed, already priced in, false, or misinterpreted. Alerts should be marketed as anomaly/reaction intelligence rather than guaranteed trade calls.

## Social/influencer signals

Add authenticated/licensed social data as a separate evidence source. Do not rely on fragile scraping or invent influencer activity. When added, social evidence should enrich the same news → investigation → market-reaction pipeline.

## 24/7 hosting

Run the scanner as a persistent Node service/container. Do not use a short-lived Netlify Function as the WebSocket worker.

Run locally:

`npm install`
`TELEGRAM_BOT_TOKEN=your-secret npm start`

Before commercial launch, add subscriber routing, subscription expiry, payment-provider webhooks, per-user alert filters and authenticated social/news providers.
