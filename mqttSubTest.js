#!/usr/bin/env node

const mqtt = require('mqtt');
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');
const fs = require('fs');
const path = require('path');

// ==== 1) Parse CLI flags ====
const argv = yargs(hideBin(process.argv))
  .option('broker', {
    describe: 'MQTT broker URL',
    type: 'string',
    default: 'mqtt://10.17.10.250:1883'
  })
  .option('clientId', {
    describe: 'Unique MQTT client ID (required for persistent sessions)',
    type: 'string',
    default: `subscriber01`
  })
  .option('username', {
    describe: 'MQTT username',
    type: 'string',
    default: 'user1'
  })
  .option('password', {
    describe: 'MQTT password',
    type: 'string',
    default: 'LxUYCw@VUVzKEr6K_'
  })
  .option('topic', {
    describe: 'Topic to subscribe to',
    type: 'string',
    default: 'testtopic/test'
  })
  .option('qos', {
    describe: 'QoS level',
    choices: [0,1,2],
    type: 'number',
    default: 2
  })
  .option('sessionExpiry', {
    describe: 'Session Expiry Interval (seconds)',
    type: 'number',
    default: 120
  })
  .option('clearStats', {
    describe: 'Whether to reset this client\'s total on start (0=no, 1=yes)',
    choices: [0,1],
    type: 'number',
    default: 0
  })
  .option('statsInterval', {
    describe: 'Interval in seconds to print stats',
    type: 'number',
    default: 2
  })
  .help()
  .argv;

// ==== 2) Stats persistence setup in per-client file ====
const statsFile = path.join(process.cwd(), `subscriber-stats-${argv.clientId}.stats`);
let totalReceived = 0;
let sessionReceived = 0;

if (argv.clearStats === 1) {
  totalReceived = 0;
  fs.writeFileSync(statsFile, JSON.stringify({ total: 0 }, null, 2));
} else if (fs.existsSync(statsFile)) {
  try {
    const data = JSON.parse(fs.readFileSync(statsFile, 'utf8'));
    totalReceived = Number(data.total) || 0;
  } catch {
    totalReceived = 0;
    fs.writeFileSync(statsFile, JSON.stringify({ total: 0 }, null, 2));
  }
} else {
  fs.writeFileSync(statsFile, JSON.stringify({ total: 0 }, null, 2));
}

console.log(`▶ Starting subscriber (clientId=${argv.clientId}), previous total=${totalReceived}`);

// ==== 3) MQTT v5 connect ====
const client = mqtt.connect(argv.broker, {
  clientId: argv.clientId,
  protocolVersion: 5,
  clean: false,
  username: argv.username,
  password: argv.password,
  properties: { sessionExpiryInterval: argv.sessionExpiry }
});

client.on('connect', () => {
  console.log('✅ Connected to broker');
  client.subscribe(argv.topic, { qos: argv.qos }, (err, granted) => {
    if (err) console.error('❌ Subscribe error:', err.message);
    else {
      if (granted.length > 0){
        console.log(`🔔 Subscribed to ${argv.topic} (QoS ${granted[0].qos})`);
      }
      
    }
  });
});
client.on('reconnect', () => console.log('🔄 Reconnecting...'));
client.on('close',     () => console.log('❌ Connection closed'));
client.on('error',     err => console.error('🔥 Error:', err.message));
client.on('disconnect', packet => {
  const reason = packet.properties?.reasonString || 'N/A';
  console.log(`⚠️ Disconnected by broker: code=${packet.reasonCode}, reason=${reason}`);
});

// ==== 4) Message handling ====
client.on('message', (topic, payload) => {
  totalReceived++;
  sessionReceived++;
  fs.writeFile(statsFile, JSON.stringify({ total: totalReceived }, null, 2), () => {});
  console.log(`📨 [#${totalReceived}] ${topic} → ${payload.toString()}`);
});

// ==== 5) Stats printer ====
const statsTimer = setInterval(() => {
  console.log(`📊 Stats (last ${argv.statsInterval}s): session=${sessionReceived}, total=${totalReceived}`);
}, argv.statsInterval * 1000);

// ==== 6) Graceful shutdown on Ctrl-C ====
process.on('SIGINT', () => {
  console.log('\n⏹ Received SIGINT, disconnecting…');
  clearInterval(statsTimer);
  client.end(false, {}, () => {
    console.log('👋 Disconnected cleanly. Final session:', sessionReceived, 'lifetime:', totalReceived);
    process.exit(0);
  });
});
