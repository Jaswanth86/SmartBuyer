const FEEDS = [
  "https://www.coindesk.com/arc/outboundfeeds/rss/",
  "https://cointelegraph.com/rss",
];

function clean(value = "") {
  return value
    .replace(/<!\[CDATA\[|\]\]>/g, "")
    .replace(/<[^>]+>/g, "")
    .trim();
}

function readTag(xml, tag) {
  const match = xml.match(
    new RegExp("<" + tag + ">([\\s\\S]*?)</" + tag + ">", "i")
  );
  return match ? clean(match[1]) : "";
}

export default async function handler() {
  const news = [];

  for (const url of FEEDS) {
    try {
      const response = await fetch(url, {
        headers: { "user-agent": "CryptoRadar/1.0" },
      });

      if (!response.ok) continue;

      const xml = await response.text();
      const items = xml.match(/<item[\\s\\S]*?<\\/item>/gi) || [];

      for (const item of items.slice(0, 12)) {
        const title = readTag(item, "title");
        const link = readTag(item, "link");
        const pubDate = readTag(item, "pubDate");

        if (title && link) {
          news.push({
            title,
            link,
            source: url.includes("coindesk") ? "CoinDesk" : "Cointelegraph",
            time: pubDate ? new Date(pubDate).toLocaleString() : "",
          });
        }
      }
    } catch {
      // Keep the function resilient if one feed is unavailable.
    }
  }

  return new Response(JSON.stringify(news), {
    headers: {
      "content-type": "application/json",
      "cache-control": "public,max-age=60",
    },
  });
}
