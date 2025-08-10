require('dotenv').config();
const mqtt = require('mqtt');
const { createClient } = require('@supabase/supabase-js');
const http = require('http'); // simple health endpoint

// ---- tiny health server (useful for logs/monitoring) ----
const PORT = process.env.PORT || 3000;
http.createServer((_req, res) => { res.writeHead(200); res.end('ok'); }).listen(PORT);

// ---- required envs (Railway will provide these) ----
['SUPABASE_URL','SUPABASE_SERVICE_ROLE_KEY','MQTT_HOST','MQTT_USERNAME','MQTT_PASSWORD']
  .forEach(k => { if (!process.env[k]) console.warn(`WARN: missing ${k}`); });

// ---- Supabase client (server key; do NOT use anon here) ----
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY,
  { auth: { persistSession: false } }
);

// ---- MQTT over secure WebSockets ----
const topic = process.env.MQTT_TOPIC || 'pump/uv/status';
const onlineTopic = 'pump/uv/online';
const deviceId = process.env.DEVICE_ID || 'pump-1';

const mqttUrl = `wss://${process.env.MQTT_HOST}:${process.env.MQTT_PORT_WSS || 8884}/mqtt`;
const client = mqtt.connect(mqttUrl, {
  username: process.env.MQTT_USERNAME,
  password: process.env.MQTT_PASSWORD,
  reconnectPeriod: 2000
});

client.on('connect', () => {
  console.log('MQTT connected');
  client.subscribe(topic, { qos: 1 }, (err) =>
    console.log(err ? 'Subscribe error:' + err : 'Subscribed to ' + topic)
  );
  client.subscribe(onlineTopic, { qos: 1 }, (err) =>
    console.log(err ? 'Subscribe error:' + err : 'Subscribed to ' + onlineTopic)
  );
});

client.on('message', async (_topic, payload) => {
  const text = payload.toString().trim();

  // online/offline retained flag
  if (_topic === onlineTopic) {
    const online = (text === '1');
    const { error } = await supabase.from('device_status').upsert(
      {
        device_id: deviceId,
        online,
        last_online_ts: online ? new Date().toISOString() : undefined
      },
      { onConflict: 'device_id' }
    );
    if (error) console.error('Status upsert error:', error);
    else console.log('Online changed:', online);
    return;
  }

  // status payload (JSON or plain "GREEN"/"RED")
  let msg;
  try { msg = JSON.parse(text); } catch { msg = { uv_status: text }; }

  const uv = (msg.uv_status || '').toUpperCase();
  const ts = msg.ts ? new Date(msg.ts) : new Date();

  const row = {
    device_id: msg.device_id || deviceId,
    ts: ts.toISOString(),
    uv_status: uv === 'GREEN' ? 'GREEN' : 'RED',
    rssi: msg.rssi ?? null,
    temp_c: msg.temp_c ?? null,
    raw: msg,
  };

  const { error } = await supabase.from('pump_readings').insert(row);
  if (error) console.error('DB insert error:', error);
  else console.log('Saved:', row.device_id, row.uv_status, row.ts);

  // update last_seen
  const { error: dsErr } = await supabase.from('device_status').upsert(
    {
      device_id: row.device_id,
      online: true,
      last_seen_ts: new Date().toISOString()
    },
    { onConflict: 'device_id' }
  );
  if (dsErr) console.error('Device status upsert error:', dsErr);
});

client.on('error', (e) => console.error('MQTT error:', e));
