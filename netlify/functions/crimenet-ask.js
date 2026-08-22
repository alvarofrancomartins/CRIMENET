// Rate limit: per-IP tracking
const rateMap = new Map();

// Cleanup stale entries every 5 minutes
setInterval(() => {
  const now = Date.now();
  for (const [ip, entry] of rateMap) {
    if (now - entry.start > 300000) rateMap.delete(ip);
  }
}, 300000);

// Allowed origins
const ALLOWED_ORIGINS = [
  'https://alvarofrancomartins.com',
  'https://www.alvarofrancomartins.com',
  'https://afmartins.netlify.app'
];

exports.handler = async (event) => {
  const origin = event.headers['origin'] || '';
  const corsOrigin = ALLOWED_ORIGINS.includes(origin) ? origin : ALLOWED_ORIGINS[0];

  // CORS preflight
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers: { 'Access-Control-Allow-Origin': corsOrigin, 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'Content-Type' } };
  }

  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, body: JSON.stringify({ error: 'POST only' }) };
  }

  // Block requests from unknown origins
  if (origin && !ALLOWED_ORIGINS.includes(origin)) {
    return { statusCode: 403, body: JSON.stringify({ error: 'Forbidden' }) };
  }

  // Rate limit: 20 req/min per IP
  const ip = (event.headers['x-forwarded-for'] || '').split(',')[0].trim() || 'unknown';
  const now = Date.now();
  const entry = rateMap.get(ip) || { count: 0, start: now };
  if (now - entry.start > 60000) { entry.count = 0; entry.start = now; }
  entry.count++;
  rateMap.set(ip, entry);

  if (entry.count > 20) {
    return { statusCode: 429, headers: { 'Access-Control-Allow-Origin': corsOrigin }, body: JSON.stringify({ error: 'Rate limited. Try again in a minute.' }) };
  }

  // Proxy to DeepSeek
  try {
    const res = await fetch('https://api.deepseek.com/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${process.env.DEEPSEEK_KEY}`
      },
      body: event.body
    });

    const data = await res.json();
    return {
      statusCode: res.status,
      headers: { 'Access-Control-Allow-Origin': corsOrigin, 'Content-Type': 'application/json' },
      body: JSON.stringify(data)
    };
  } catch (err) {
    return { statusCode: 500, headers: { 'Access-Control-Allow-Origin': corsOrigin }, body: JSON.stringify({ error: 'Proxy error' }) };
  }
};
