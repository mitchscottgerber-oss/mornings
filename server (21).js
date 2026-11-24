const express = require('express');
const cors = require('cors');
const cron = require('node-cron');
const RSSParser = require('rss-parser');
const rssParser = new RSSParser({ timeout: 8000 });

const app = express();
app.use(cors());
app.use(express.json({ limit: '5mb' }));

// ── Web Push (optional) ───────────────────────────────────────────────────────
let webpush = null;
try {
  webpush = require('web-push');
  const VAPID_PUBLIC = process.env.VAPID_PUBLIC_KEY;
  const VAPID_PRIVATE = process.env.VAPID_PRIVATE_KEY;
  if (VAPID_PUBLIC && VAPID_PRIVATE) {
    try {
      webpush.setVapidDetails('mailto:mitch@triumphnil.com', VAPID_PUBLIC, VAPID_PRIVATE);
      console.log('Web push initialized');
    } catch(vapidErr) {
      console.log('VAPID setup failed:', vapidErr.message);
      webpush = null;
    }
  }
} catch(e) {
  console.log('web-push not available:', e.message);
}

// ── Store subscriptions in memory ─────────────────────────────────────────────
let pushSubscriptions = [];

app.post('/api/push/subscribe', (req, res) => {
  const sub = req.body;
  if (!sub || !sub.endpoint) return res.status(400).json({ error: 'Invalid' });
  pushSubscriptions = pushSubscriptions.filter(s => s.endpoint !== sub.endpoint);
  pushSubscriptions.push(sub);
  console.log('Subscription saved. Total:', pushSubscriptions.length);
  res.json({ success: true });
});

app.get('/api/push/vapid-key', (req, res) => {
  res.json({ publicKey: process.env.VAPID_PUBLIC_KEY || '' });
});

app.get('/api/push/test', async (req, res) => {
  await sendPushToAll('Daily Brief', 'Test notification working! 🎉', '/');
  res.json({ sent: pushSubscriptions.length });
});

async function sendPushToAll(title, body, url) {
  if (!webpush || pushSubscriptions.length === 0) return;
  const payload = JSON.stringify({ title, body, url: url || '/' });
  for (const sub of [...pushSubscriptions]) {
    try {
      await webpush.sendNotification(sub, payload);
    } catch(err) {
      if (err.statusCode === 410) {
        pushSubscriptions = pushSubscriptions.filter(s => s.endpoint !== sub.endpoint);
      }
    }
  }
}


// ── Postgres Sync ─────────────────────────────────────────────────────────────
let pgClient = null;
// New Postgres instance - reset on startup

async function getDB() {
  if (pgClient) return pgClient;
  try {
    const { Client } = require('pg');
    pgClient = new Client({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });
    await pgClient.connect();
    await pgClient.query('CREATE TABLE IF NOT EXISTS tasks (id TEXT PRIMARY KEY, data JSONB NOT NULL, updated_at TIMESTAMPTZ DEFAULT NOW())');
    await pgClient.query('CREATE TABLE IF NOT EXISTS sow_deals (id TEXT PRIMARY KEY, data JSONB NOT NULL, updated_at TIMESTAMPTZ DEFAULT NOW())');
    await pgClient.query('CREATE TABLE IF NOT EXISTS csuite_data (person TEXT PRIMARY KEY, items JSONB NOT NULL, notes TEXT DEFAULT \'\', updated_at TIMESTAMPTZ DEFAULT NOW())');
    await pgClient.query('CREATE TABLE IF NOT EXISTS kv_store (key TEXT PRIMARY KEY, data JSONB NOT NULL, updated_at TIMESTAMPTZ DEFAULT NOW())');
    console.log('Postgres connected and tables ready');
  } catch(e) {
    console.log('Postgres connection failed:', e.message);
    pgClient = null;
  }
  return pgClient;
}

app.get('/api/sync/tasks', async (req, res) => {
  try {
    const db = await getDB();
    if (!db) return res.status(503).json({ error: 'DB unavailable' });
    const result = await db.query('SELECT data FROM tasks ORDER BY (data->>\'order\')::int ASC NULLS LAST, updated_at ASC');
    res.json({ tasks: result.rows.map(r => r.data) });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/sync/tasks', async (req, res) => {
  try {
    const db = await getDB();
    if (!db) return res.status(503).json({ error: 'DB unavailable' });
    const { tasks } = req.body;
    if (!Array.isArray(tasks)) return res.status(400).json({ error: 'Invalid' });
    await db.query('DELETE FROM tasks');
    for (let i = 0; i < tasks.length; i++) {
      const t = Object.assign({}, tasks[i], { order: i });
      await db.query('INSERT INTO tasks (id, data) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET data=$2, updated_at=NOW()', [String(t.id), JSON.stringify(t)]);
    }
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/sync/sow', async (req, res) => {
  try {
    const db = await getDB();
    if (!db) return res.status(503).json({ error: 'DB unavailable' });
    const result = await db.query('SELECT data FROM sow_deals ORDER BY (data->>\'order\')::int ASC NULLS LAST');
    res.json({ deals: result.rows.map(r => r.data) });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/sync/sow', async (req, res) => {
  try {
    const db = await getDB();
    if (!db) return res.status(503).json({ error: 'DB unavailable' });
    const { deals } = req.body;
    if (!Array.isArray(deals)) return res.status(400).json({ error: 'Invalid' });
    await db.query('DELETE FROM sow_deals');
    for (let i = 0; i < deals.length; i++) {
      const d = Object.assign({}, deals[i], { order: i });
      await db.query('INSERT INTO sow_deals (id, data) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET data=$2, updated_at=NOW()', [String(d.id), JSON.stringify(d)]);
    }
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/sync/csuite', async (req, res) => {
  try {
    const db = await getDB();
    if (!db) return res.status(503).json({ error: 'DB unavailable' });
    const result = await db.query('SELECT person, items, notes FROM csuite_data');
    const out = {};
    result.rows.forEach(r => { out[r.person] = { items: r.items, notes: r.notes }; });
    res.json({ csuite: out });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/sync/csuite', async (req, res) => {
  try {
    const db = await getDB();
    if (!db) return res.status(503).json({ error: 'DB unavailable' });
    const { person, items, notes } = req.body;
    if (!person) return res.status(400).json({ error: 'Invalid' });
    await db.query('INSERT INTO csuite_data (person, items, notes) VALUES ($1, $2, $3) ON CONFLICT (person) DO UPDATE SET items=$2, notes=$3, updated_at=NOW()', [person, JSON.stringify(items || []), notes || '']);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── Key/value sync (focus planner: parking lot, focus stats, task order) ─────
app.get('/api/sync/kv/:key', async (req, res) => {
  try {
    const db = await getDB();
    if (!db) return res.status(503).json({ error: 'DB unavailable' });
    const result = await db.query('SELECT data FROM kv_store WHERE key = $1', [req.params.key]);
    res.json({ data: result.rows.length ? result.rows[0].data : null });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/sync/kv/:key', async (req, res) => {
  try {
    const db = await getDB();
    if (!db) return res.status(503).json({ error: 'DB unavailable' });
    const data = req.body && req.body.data;
    if (!data || typeof data !== 'object') return res.status(400).json({ error: 'Invalid' });
    await db.query('INSERT INTO kv_store (key, data) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET data=$2, updated_at=NOW()', [req.params.key, JSON.stringify(data)]);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── Health check ──────────────────────────────────────────────────────────────


// ── Create Calendar Event ─────────────────────────────────────────────────────
app.post('/api/calendar/create', async (req, res) => {
  try {
    const { title, date, startTime, endTime, description, attendees } = req.body;
    
    // Always get a fresh token
    let token = cachedToken;
    if (!token || tokenExpiry < Date.now() + 60000) {
      try {
        const refreshRes = await fetch('https://oauth2.googleapis.com/token', {
          method: 'POST',
          headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
          body: new URLSearchParams({
            client_id: process.env.GOOGLE_CLIENT_ID,
            client_secret: process.env.GOOGLE_CLIENT_SECRET,
            refresh_token: process.env.GOOGLE_REFRESH_TOKEN,
            grant_type: 'refresh_token'
          })
        });
        const refreshData = await refreshRes.json();
        console.log('Token refresh result:', refreshData.access_token ? 'success' : 'failed', refreshData.error || '');
        if (refreshData.access_token) {
          token = refreshData.access_token;
          cachedToken = token;
          tokenExpiry = Date.now() + (refreshData.expires_in * 1000);
        }
      } catch(e) {
        console.log('Token refresh failed:', e.message);
      }
      token = token || process.env.GOOGLE_ACCESS_TOKEN;
    }
    
    if (!token) return res.status(401).json({ error: 'No token' });

    const startISO = date + 'T' + startTime + ':00';
    const endISO = date + 'T' + endTime + ':00';

    const eventBody = {
      summary: title,
      description: description || '',
      start: { dateTime: startISO, timeZone: 'America/New_York' },
      end: { dateTime: endISO, timeZone: 'America/New_York' }
    };

    const attendeeList = Array.isArray(attendees) ? attendees : [];
    if (attendeeList.length > 0) {
      eventBody.attendees = attendeeList.map(function(email) { return { email: email }; });
    }

    console.log('Creating calendar event:', title, date, startTime, 'attendees:', attendees?.length || 0);

    const response = await fetch('https://www.googleapis.com/calendar/v3/calendars/primary/events?sendUpdates=all', {
      method: 'POST',
      headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' },
      body: JSON.stringify(eventBody)
    });

    const data = await response.json();
    console.log('Calendar API response status:', response.status, 'id:', data.id, 'error:', data.error?.message);
    
    if (data.id) {
      res.json({ success: true, eventId: data.id, title: data.summary });
    } else {
      res.status(400).json({ success: false, error: data.error?.message || 'Failed to create event' });
    }
  } catch (err) {
    console.error('Calendar create error:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});



// ── Scout AI Proxy ────────────────────────────────────────────────────────────


// ── RSS News Feed ─────────────────────────────────────────────────────────────
var cachedNewsDigest = null;
var cachedNewsTime = null;

var RSS_FEEDS = [
  { tag: 'NIL', url: 'https://www.on3.com/nil/feed/', label: 'On3' },
  { tag: 'NIL', url: 'https://nilnetwork.com/feed/', label: 'NIL Network' },
  { tag: 'VT',  url: 'https://www.hokiesports.com/rss.aspx?path=football', label: 'HokieSports' },
  { tag: 'VT',  url: 'https://247sports.com/college/virginia-tech/Season/2025-Football/feed/', label: '247Sports' },
  { tag: 'Events', url: 'https://www.pollstar.com/rss', label: 'Pollstar' },
  { tag: 'Events', url: 'https://www.billboard.com/feed/', label: 'Billboard' },
];

async function summarizeStory(headline, snippet, tag) {
  try {
    const prompt = `You are a brief news summarizer for someone running a NIL collective and event production company in Blacksburg, Virginia. Summarize this ${tag} story in 3-4 clear, informative sentences that capture the key facts and why it matters. Do not use filler phrases like "This article discusses" — just deliver the information directly.

Headline: ${headline}
Content: ${snippet}

Write 3-4 sentences only.`;

    const resp = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01'
      },
      body: JSON.stringify({
        model: 'claude-haiku-4-5-20251001',
        max_tokens: 200,
        messages: [{ role: 'user', content: prompt }]
      })
    });
    const data = await resp.json();
    if (data.content && data.content[0]) return data.content[0].text.trim();
  } catch(e) {
    console.log('Summarize error:', e.message);
  }
  return snippet;
}

async function fetchRSSStories() {
  var stories = [];
  var tagsFound = {};

  for (var feed of RSS_FEEDS) {
    if (tagsFound[feed.tag] >= 1) continue;
    try {
      var parsed = await rssParser.parseURL(feed.url);
      if (parsed.items && parsed.items.length > 0) {
        var item = parsed.items[0];
        var pubDate = item.pubDate ? new Date(item.pubDate) : null;
        var age = '';
        if (pubDate) {
          var diffH = Math.round((Date.now() - pubDate) / 3600000);
          age = diffH < 24 ? diffH + 'h ago' : Math.round(diffH/24) + 'd ago';
        }
        var headline = (item.title || '').replace(/<[^>]*>/g, '').trim();
        var rawSnippet = (item.contentSnippet || item.content || '').replace(/<[^>]*>/g, '').trim();
        var summary = await summarizeStory(headline, rawSnippet.substring(0, 1000), feed.tag);
        stories.push({
          tag: feed.tag,
          headline: headline.substring(0, 100),
          summary: summary,
          source: feed.label,
          age: age,
          link: item.link || ''
        });
        tagsFound[feed.tag] = (tagsFound[feed.tag] || 0) + 1;
      }
    } catch(e) {
      console.log('RSS feed error (' + feed.url + '):', e.message);
    }
  }

  ['NIL','VT','Events'].forEach(function(tag) {
    if (!tagsFound[tag]) {
      stories.push({ tag: tag, headline: 'No recent headlines', summary: 'Nothing significant in the last 24 hours.', source: '', age: '', link: '' });
    }
  });

  return stories;
}

async function refreshRSSCache() {
  try {
    console.log('Refreshing RSS cache...');
    var stories = await fetchRSSStories();
    if (stories.length) {
      cachedNewsDigest = stories;
      cachedNewsTime = Date.now();
      console.log('RSS cache updated:', stories.length, 'stories');
    }
  } catch(e) {
    console.error('RSS refresh error:', e.message);
  }
}

// Refresh every hour
cron.schedule('0 * * * *', refreshRSSCache);

// Also refresh at 5AM Eastern (10 UTC)
cron.schedule('0 10 * * *', refreshRSSCache);

// Seed on startup
setTimeout(refreshRSSCache, 3000);

// Also generate on server startup if no cache exists
setTimeout(function() {
  if (!cachedNewsDigest) {
    console.log('No cached digest — generating on startup');
    generateNewsDigest();
  }
}, 5000);


app.get('/api/notes', async (req, res) => {
  try {
    const db = await getDB();
    const result = await db.query("SELECT data FROM csuite_data WHERE key = 'dskNotes'");
    await db.end();
    const notes = result.rows.length ? JSON.parse(result.rows[0].data) : '';
    res.json({ notes });
  } catch(e) {
    res.json({ notes: '' });
  }
});

app.post('/api/notes', async (req, res) => {
  try {
    const { notes } = req.body;
    const db = await getDB();
    await db.query("INSERT INTO csuite_data (key, data) VALUES ('dskNotes', $1) ON CONFLICT (key) DO UPDATE SET data = $1", [JSON.stringify(notes)]);
    await db.end();
    res.json({ ok: true });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/news', async (req, res) => {
  if (!cachedNewsDigest) await refreshRSSCache();
  res.json({ stories: cachedNewsDigest || [], time: cachedNewsTime });
});

app.post('/api/scout', async (req, res) => {
  try {
    const { messages, system } = req.body;
    console.log('Scout request received, messages:', messages?.length);
    const response = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
        'anthropic-beta': 'messages-2023-12-15'
      },
      body: JSON.stringify({
        model: 'claude-haiku-4-5-20251001',
        max_tokens: 1024,
        system: system,
        messages: messages
      })
    });
    console.log('Anthropic response status:', response.status);
    const data = await response.json();
    console.log('Anthropic response type:', data.type);
    res.json(data);
  } catch (err) {
    console.error('Scout proxy error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── Mornings: question of the day ─────────────────────────────────────────────
const morningsHits = {};
app.post('/api/mornings/question', async (req, res) => {
  try {
    const ip = String(req.headers['x-forwarded-for'] || req.ip || '').split(',')[0].trim();
    const day = new Date().toISOString().slice(0, 10);
    if (Object.keys(morningsHits).length > 2000) { for (const k in morningsHits) delete morningsHits[k]; }
    const hk = ip + '|' + day;
    morningsHits[hk] = (morningsHits[hk] || 0) + 1;
    if (morningsHits[hk] > 40) return res.status(429).json({ error: 'Too many questions today' });

    const body = req.body || {};
    const name = String(body.name || 'there').slice(0, 40);
    const bucket = String(body.bucket || '').slice(0, 60);
    const notes = Array.isArray(body.notes) ? body.notes.slice(0, 40).map(n => String(n).slice(0, 400)).filter(Boolean) : [];
    const recent = Array.isArray(body.recent) ? body.recent.slice(0, 10).map(n => String(n).slice(0, 400)) : [];
    if (!bucket || !notes.length) return res.status(400).json({ error: 'bucket and notes are required' });

    const system = [
      'You are a warm, direct life coach. Each morning ' + name + ' reads the notes he keeps for one area of his life, then you give him exactly one question to sit with for the day.',
      'Ground the question in his notes: use their specifics, and you may connect two notes.',
      'Strong questions probe the gap between what he wants and what he actually does, the reason behind a goal, a trade-off he is avoiding, or one concrete next step.',
      'Write in second person, under 45 words, as a single question. No preamble, no advice, no lists, no therapy jargon.',
      'Do not repeat the angle of any recent question.'
    ].join(' ');
    const numbered = notes.map((n, i) => (i + 1) + '. ' + n).join('\n');
    const user = 'Area: ' + bucket + '\n\nMy notes:\n' + numbered +
      (recent.length ? '\n\nRecent questions (do not repeat these angles):\n- ' + recent.join('\n- ') : '') +
      '\n\nReply with only JSON in this shape: {"question": "...", "from": [1, 3]}. "from" lists the note numbers the question draws on.';

    const response = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': process.env.ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01' },
      body: JSON.stringify({ model: process.env.MORNINGS_MODEL || 'claude-haiku-4-5-20251001', max_tokens: 300, system: system, messages: [{ role: 'user', content: user }] })
    });
    const data = await response.json();
    if (!response.ok) return res.status(502).json({ error: (data && data.error && data.error.message) || 'Upstream error' });
    const text = (data.content || []).filter(c => c.type === 'text').map(c => c.text).join('').trim();
    let parsed = null;
    const a = text.indexOf('{'), b = text.lastIndexOf('}');
    if (a !== -1 && b > a) { try { parsed = JSON.parse(text.slice(a, b + 1)); } catch (e) { parsed = null; } }
    const question = parsed && typeof parsed.question === 'string' ? parsed.question.trim() : text;
    const from = parsed && Array.isArray(parsed.from) ? parsed.from.map(Number).filter(n => n >= 1 && n <= notes.length).slice(0, 3) : [];
    if (!question) return res.status(502).json({ error: 'Empty question' });
    res.json({ question: question.slice(0, 400), from: from });
  } catch (err) {
    console.error('Mornings question error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.get('/', (req, res) => {
  res.json({ status: 'Daily Brief backend running', time: new Date().toISOString(), subscribers: pushSubscriptions.length });
});

// ── Token endpoint ────────────────────────────────────────────────────────────
let cachedToken = null;
let tokenExpiry = 0;

app.get('/api/token', async (req, res) => {
  try {
    const now = Date.now();
    const forceRefresh = req.query.refresh === '1';
    if (!forceRefresh && cachedToken && tokenExpiry > now + 300000) return res.json({ token: cachedToken });
    const { GOOGLE_REFRESH_TOKEN, GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_ACCESS_TOKEN } = process.env;
    if (!GOOGLE_REFRESH_TOKEN) return res.json({ token: GOOGLE_ACCESS_TOKEN });
    const response = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        client_id: GOOGLE_CLIENT_ID,
        client_secret: GOOGLE_CLIENT_SECRET,
        refresh_token: GOOGLE_REFRESH_TOKEN,
        grant_type: 'refresh_token'
      })
    });
    const data = await response.json();
    if (data.access_token) {
      cachedToken = data.access_token;
      tokenExpiry = now + (data.expires_in * 1000);
      return res.json({ token: cachedToken });
    }
    return res.json({ token: GOOGLE_ACCESS_TOKEN });
  } catch(err) {
    res.json({ token: process.env.GOOGLE_ACCESS_TOKEN });
  }
});

// ── Google Auth ───────────────────────────────────────────────────────────────
app.get('/auth/google', (req, res) => {
  try {
    const { google } = require('googleapis');
    const oauth2Client = new google.auth.OAuth2(
      process.env.GOOGLE_CLIENT_ID,
      process.env.GOOGLE_CLIENT_SECRET,
      process.env.GOOGLE_REDIRECT_URI
    );
    const scopes = [
      'https://www.googleapis.com/auth/gmail.readonly',
      'https://www.googleapis.com/auth/calendar',
      'https://www.googleapis.com/auth/calendar.events'
    ];
    console.log('Auth requested with scopes:', scopes);
    const url = oauth2Client.generateAuthUrl({
      access_type: 'offline',
      scope: scopes,
      prompt: 'consent'
    });
    res.redirect(url);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/auth/google/callback', async (req, res) => {
  try {
    const { google } = require('googleapis');
    const oauth2Client = new google.auth.OAuth2(
      process.env.GOOGLE_CLIENT_ID,
      process.env.GOOGLE_CLIENT_SECRET,
      process.env.GOOGLE_REDIRECT_URI
    );
    const { tokens } = await oauth2Client.getToken(req.query.code);
    res.send(`<h2>Connected!</h2>
      <p><strong>GOOGLE_ACCESS_TOKEN:</strong> ${tokens.access_token}</p>
      <p><strong>GOOGLE_REFRESH_TOKEN:</strong> ${tokens.refresh_token || '(use existing)'}</p>`);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── Scheduled Notifications ───────────────────────────────────────────────────
// 4:45 AM daily
cron.schedule('45 4 * * *', async () => {
  console.log('Sending morning brief notification...');
  await sendPushToAll('Daily Brief', 'Good morning, Mitch. Time to get after it.', '/');
}, { timezone: 'America/New_York' });

// 5:00 PM Mon-Thu
cron.schedule('0 17 * * 1-4', async () => {
  console.log('Sending EOD notification Mon-Thu...');
  await sendPushToAll('🔔 Closing Bell', "Time to rate your day and build tomorrow's schedule.", '/');
}, { timezone: 'America/New_York' });

// 3:00 PM Friday
cron.schedule('0 15 * * 5', async () => {
  console.log('Sending EOD notification Friday...');
  await sendPushToAll('🔔 Closing Bell — Wrap up your week', 'Rate your tasks and set up for Monday.', '/');
}, { timezone: 'America/New_York' });

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Daily Brief backend running on port ${PORT}`));
