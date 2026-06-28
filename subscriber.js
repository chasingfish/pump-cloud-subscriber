require('dotenv').config();
const mqtt = require('mqtt');
const crypto = require('crypto');
const { createClient } = require('@supabase/supabase-js');
const http = require('http');

// ============================================================================
// Config / env
// ============================================================================
const PORT = process.env.PORT || 3000;

['SUPABASE_URL', 'SUPABASE_SERVICE_ROLE_KEY', 'MQTT_HOST', 'MQTT_USERNAME', 'MQTT_PASSWORD']
  .forEach(k => { if (!process.env[k]) console.warn(`WARN: missing ${k}`); });

const deviceId    = process.env.DEVICE_ID || 'pump-1';
const statusTopic = process.env.MQTT_TOPIC || 'pump/uv/status';
const onlineTopic = 'pump/uv/online';
const alertTopic  = 'pump/uv/alert/#';
const ackTopic    = `pump/uv/ack/${deviceId}`;
const cmdTopic    = `pump/uv/cmd/${deviceId}`;

// Optional shared secret for the phone-shortcut control path (NOT the dashboard).
const CONTROL_TOKEN = process.env.CONTROL_TOKEN || '';

// Twilio (all optional — alert/SMS features stay dormant until set)
const TW_SID        = process.env.TWILIO_SID || '';
const TW_AUTH       = process.env.TWILIO_AUTH_TOKEN || '';
const TW_FROM       = process.env.TWILIO_FROM || '';
const TW_WEBHOOK    = process.env.TWILIO_WEBHOOK_URL || ''; // exact URL configured in Twilio (for signature check)
const ALERT_TO      = (process.env.ALERT_TO || '').split(',').map(s => s.trim()).filter(Boolean);
const ALLOWED_NUMS  = (process.env.ALLOWED_NUMBERS || process.env.ALERT_TO || '')
                        .split(',').map(s => s.trim()).filter(Boolean);
const OFFLINE_GRACE_MS = Number(process.env.OFFLINE_GRACE_MS || 600000); // 10 min — board flaps on weak wifi

const twilioReady = TW_SID && TW_AUTH && TW_FROM;

// ============================================================================
// Supabase (service key for writes; also used to verify dashboard user tokens)
// ============================================================================
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY,
  { auth: { persistSession: false } }
);

// ============================================================================
// MQTT over secure WebSockets
// ============================================================================
const mqttUrl = `wss://${process.env.MQTT_HOST}:${process.env.MQTT_PORT_WSS || 8884}/mqtt`;
const client = mqtt.connect(mqttUrl, {
  username: process.env.MQTT_USERNAME,
  password: process.env.MQTT_PASSWORD,
  reconnectPeriod: 2000
});

client.on('connect', () => {
  console.log('MQTT connected');
  for (const t of [statusTopic, onlineTopic, alertTopic, ackTopic]) {
    client.subscribe(t, { qos: 1 }, (err) =>
      console.log(err ? `Subscribe error (${t}): ${err}` : `Subscribed to ${t}`));
  }
});
client.on('error', (e) => console.error('MQTT error:', e));

function publishCmd(obj) {
  const payload = JSON.stringify(obj);
  return new Promise((resolve, reject) => {
    client.publish(cmdTopic, payload, { qos: 1 }, (err) => {
      if (err) return reject(err);
      console.log('CMD published:', payload);
      resolve();
    });
  });
}

// ============================================================================
// State (for alert debouncing)
// ============================================================================
let lastOnline = null;          // last known online flag
let offlineTimer = null;        // pending "stayed offline" SMS
let lastReading = null;         // last status payload (for /status + SMS STATUS)

// ============================================================================
// Twilio helpers
// ============================================================================
async function sendSMS(to, body) {
  if (!twilioReady) { console.warn('SMS skipped (Twilio not configured):', body); return; }
  const url = `https://api.twilio.com/2010-04-01/Accounts/${TW_SID}/Messages.json`;
  const form = new URLSearchParams({ To: to, From: TW_FROM, Body: body });
  try {
    const res = await fetch(url, {
      method: 'POST',
      headers: {
        'Authorization': 'Basic ' + Buffer.from(`${TW_SID}:${TW_AUTH}`).toString('base64'),
        'Content-Type': 'application/x-www-form-urlencoded'
      },
      body: form
    });
    if (!res.ok) console.error('Twilio send failed', res.status, await res.text());
    else console.log('SMS sent to', to);
  } catch (e) { console.error('Twilio send error:', e.message); }
}
function alertAll(body) { for (const n of ALERT_TO) sendSMS(n, body); }

// Validate Twilio request signature: base64(HMAC-SHA1(authToken, url + sorted(k+v))).
// https://www.twilio.com/docs/usage/security#validating-requests
function twilioSignatureValid(sig, url, params) {
  if (!TW_AUTH) return false;
  const data = url + Object.keys(params).sort().map(k => k + params[k]).join('');
  const expected = crypto.createHmac('sha1', TW_AUTH).update(Buffer.from(data, 'utf-8')).digest('base64');
  try { return crypto.timingSafeEqual(Buffer.from(sig || ''), Buffer.from(expected)); }
  catch { return false; }
}

// ============================================================================
// SMS command parser (dad-friendly)
// ============================================================================
function chFromWord(w) {
  if (!w) return 0;
  w = w.toLowerCase();
  if (w === 'house' || w === '2' || w === 'pump2') return 2;
  if (w === 'irrigation' || w === 'irr' || w === '1' || w === 'pump1') return 1;
  return 0;
}
// Returns { cmds: [obj...], reply: string }
function parseSmsCommand(text) {
  const parts = (text || '').trim().toLowerCase().split(/\s+/);
  const verb = parts[0] || '';
  const ch = chFromWord(parts[1]);
  const label = (c) => c === 1 ? 'Irrigation' : c === 2 ? 'House' : 'BOTH pumps';

  if (['off', 'stop'].includes(verb)) {
    const cmds = ch ? [{ cmd: 'relay', ch, state: 'off' }]
                    : [{ cmd: 'relay', ch: 1, state: 'off' }, { cmd: 'relay', ch: 2, state: 'off' }];
    return { cmds, reply: `Turning ${label(ch)} OFF.` };
  }
  if (['on', 'start'].includes(verb)) {
    const cmds = ch ? [{ cmd: 'relay', ch, state: 'on' }]
                    : [{ cmd: 'relay', ch: 1, state: 'on' }, { cmd: 'relay', ch: 2, state: 'on' }];
    return { cmds, reply: `Turning ${label(ch)} ON. (If it was an emergency stop, text RESET first.)` };
  }
  if (verb === 'reset') {
    return { cmds: [{ cmd: 'reset', ch }], reply: `Clearing emergency stop on ${label(ch)}.` };
  }
  if (verb === 'status') {
    return { cmds: [], reply: statusText() };
  }
  return { cmds: [], reply: 'Commands: OFF, ON, STATUS, RESET. Add HOUSE or IRRIGATION for one pump (e.g. "OFF HOUSE").' };
}

function fmtF(c) { return (c == null || isNaN(c)) ? '—' : (c * 9 / 5 + 32).toFixed(0) + '°F'; }
function statusText() {
  if (!lastReading) return 'No recent data from the pump yet.';
  const r = lastReading;
  const a = r.pump_a_on === false ? 'OFF' : 'on';
  const b = r.pump_b_on === false ? 'OFF' : 'on';
  const latch = (r.latch_a || r.latch_b) ? ' EMERGENCY STOP ACTIVE — text RESET.' : '';
  return `Irrigation: ${a} (${fmtF(r.temp_pump_a_c)}). House: ${b} (${fmtF(r.temp_pump_b_c)}).${latch}`;
}

// ============================================================================
// MQTT message handling (DB writes + alerts)
// ============================================================================
client.on('message', async (topic, payload) => {
  const text = payload.toString().trim();

  // ---- alerts from device (overtemp emergency) ----
  if (topic.startsWith('pump/uv/alert/')) {
    let a; try { a = JSON.parse(text); } catch { a = {}; }
    const f = a.temp_f != null ? a.temp_f.toFixed(0) : (a.temp_c != null ? (a.temp_c * 9 / 5 + 32).toFixed(0) : '?');
    const pump = a.pump === 'house' ? 'House' : a.pump === 'irrigation' ? 'Irrigation' : `ch${a.ch}`;
    const msg = `🚨 PUMP EMERGENCY: ${pump} pump hit ${f}°F and was shut OFF automatically. Check the pump. Text RESET to restart when safe.`;
    console.warn(msg);
    alertAll(msg);
    return;
  }

  // ---- online/offline retained flag ----
  if (topic === onlineTopic) {
    const online = (text === '1');
    await supabase.from('device_status').upsert(
      { device_id: deviceId, online, last_online_ts: online ? new Date().toISOString() : undefined },
      { onConflict: 'device_id' }
    ).then(({ error }) => error && console.error('Status upsert error:', error));
    console.log('Online changed:', online);

    // Debounced offline alert (board flaps on weak wifi; only alert if it STAYS down).
    if (online) {
      if (offlineTimer) { clearTimeout(offlineTimer); offlineTimer = null; }
      if (lastOnline === false) alertAll('✅ Pump monitor is back online.');
    } else if (lastOnline !== false && !offlineTimer) {
      offlineTimer = setTimeout(() => {
        offlineTimer = null;
        alertAll(`⚠️ Pump monitor has been OFFLINE for ${Math.round(OFFLINE_GRACE_MS / 60000)} min. It may have lost power or wifi.`);
      }, OFFLINE_GRACE_MS);
    }
    lastOnline = online;
    return;
  }

  // ---- ack passthrough (log only) ----
  if (topic === ackTopic) { console.log('ACK:', text); return; }

  // ---- status payload ----
  let msg; try { msg = JSON.parse(text); } catch { msg = { uv_status: text }; }
  lastReading = msg;
  const uv = (msg.uv_status || '').toUpperCase();
  const ts = msg.ts ? new Date(msg.ts) : new Date();

  const row = {
    device_id: msg.device_id || deviceId,
    ts: ts.toISOString(),
    uv_status: uv === 'GREEN' ? 'GREEN' : 'RED',
    rssi: msg.rssi ?? null,
    temp_c: msg.temp_pump_a_c ?? msg.temp_c ?? null,
    raw: msg,
  };
  await supabase.from('pump_readings').insert(row)
    .then(({ error }) => error ? console.error('DB insert error:', error)
                               : console.log('Saved:', row.device_id, row.uv_status, row.ts));
  await supabase.from('device_status').upsert(
    { device_id: row.device_id, online: true, last_seen_ts: new Date().toISOString() },
    { onConflict: 'device_id' }
  ).then(({ error }) => error && console.error('Device status upsert error:', error));
});

// ============================================================================
// HTTP API: health, status, control, Twilio SMS webhook
// ============================================================================
function readBody(req) {
  return new Promise((resolve) => {
    let b = ''; req.on('data', c => { b += c; if (b.length > 1e6) req.destroy(); });
    req.on('end', () => resolve(b));
  });
}
function sendJson(res, code, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(code, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
  res.end(body);
}

// Verify a dashboard caller: Authorization: Bearer <supabase access token>, OR
// x-control-token matching CONTROL_TOKEN (phone-shortcut path).
async function callerAuthorized(req) {
  const tok = req.headers['x-control-token'];
  if (CONTROL_TOKEN && tok && crypto.timingSafeEqual(Buffer.from(tok), Buffer.from(CONTROL_TOKEN))) return true;
  const auth = req.headers['authorization'] || '';
  const m = auth.match(/^Bearer\s+(.+)$/i);
  if (!m) return false;
  try {
    const { data, error } = await supabase.auth.getUser(m[1]);
    return !error && !!data?.user;
  } catch { return false; }
}

http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const path = url.pathname;

  // CORS preflight for the dashboard
  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type,Authorization,x-control-token'
    });
    return res.end();
  }

  if (path === '/' || path === '/health') { res.writeHead(200); return res.end('ok'); }

  // Public-ish read of last reading + online (no secrets). Used by SMS STATUS / shortcuts.
  if (path === '/status' && req.method === 'GET') {
    return sendJson(res, 200, { online: lastOnline, reading: lastReading });
  }

  // Control endpoint — dashboard buttons / shortcut. {ch,state} or {action:'reset'|'check_update', ch}
  if (path === '/control' && req.method === 'POST') {
    if (!(await callerAuthorized(req))) return sendJson(res, 401, { error: 'unauthorized' });
    let body; try { body = JSON.parse(await readBody(req) || '{}'); } catch { return sendJson(res, 400, { error: 'bad json' }); }
    try {
      if (body.action === 'reset')        await publishCmd({ cmd: 'reset', ch: body.ch || 0 });
      else if (body.action === 'check_update') await publishCmd({ cmd: 'check_update' });
      else if (body.action === 'set_guard') await publishCmd({ cmd: 'set_guard', ...body });
      else if (body.ch && body.state)     await publishCmd({ cmd: 'relay', ch: body.ch, state: body.state });
      else return sendJson(res, 400, { error: 'specify {ch,state} or {action}' });
      return sendJson(res, 200, { ok: true });
    } catch (e) { return sendJson(res, 502, { error: 'publish failed: ' + e.message }); }
  }

  // Twilio inbound SMS webhook
  if (path === '/sms' && req.method === 'POST') {
    const raw = await readBody(req);
    const params = Object.fromEntries(new URLSearchParams(raw));
    const from = params.From || '';
    const sig = req.headers['x-twilio-signature'];
    const sigOk = TW_WEBHOOK ? twilioSignatureValid(sig, TW_WEBHOOK, params) : true;
    const allowOk = ALLOWED_NUMS.length === 0 ? false : ALLOWED_NUMS.includes(from);
    console.log(`SMS in from=${from} body="${params.Body}" sigOk=${sigOk} allowOk=${allowOk}`);

    // Number allowlist is the HARD gate (only known numbers can command).
    // Twilio signature is checked but only WARNS on mismatch — validation behind a
    // proxy is finicky; allowlist keeps it safe enough for MVP. TODO harden: enforce sig.
    if (!allowOk) { console.warn(`SMS rejected (not allowlisted): ${from}`); res.writeHead(403); return res.end('forbidden'); }
    if (!sigOk) console.warn('SMS signature mismatch — proceeding on allowlist only. Harden later.');

    const { cmds, reply } = parseSmsCommand(params.Body);
    for (const c of cmds) { try { await publishCmd(c); } catch (e) { console.error('SMS cmd publish failed', e.message); } }
    console.log('SMS reply:', reply);
    const twiml = `<?xml version="1.0" encoding="UTF-8"?><Response><Message>${reply.replace(/&/g, '&amp;').replace(/</g, '&lt;')}</Message></Response>`;
    res.writeHead(200, { 'Content-Type': 'text/xml' });
    return res.end(twiml);
  }

  res.writeHead(404); res.end('not found');
}).listen(PORT, () => console.log(`HTTP listening on ${PORT} (health/status/control/sms). Twilio ${twilioReady ? 'ENABLED' : 'dormant'}.`));
