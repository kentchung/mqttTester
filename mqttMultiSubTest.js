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
    default: 'mqtt://10.17.9.144:1883'
  })
  .option('username', {
    describe: 'MQTT username',
    type: 'string',
    default: 'user1'
  })
  .option('password', {
    describe: 'MQTT password',
    type: 'string',
    default: 'U!*Ec2KRJxpeRKF3-'
  })
  .option('topic', {
    describe: 'Topic to subscribe to',
    type: 'string',
    default: 'testtopic/test'
  })
  .option('qos', {
    describe: 'QoS level',
    choices: [0, 1, 2],
    type: 'number',
    default: 0
  })
  .option('sessionExpiry', {
    describe: 'Session Expiry Interval (seconds)',
    type: 'number',
    default: 30
  })
  .option('clearStats', {
    describe: 'Whether to reset stats on start (0=no, 1=yes)',
    choices: [0, 1],
    type: 'number',
    default: 1
  })
  .option('statsInterval', {
    describe: 'Interval in seconds to print stats',
    type: 'number',
    default: 1
  })
  .option('clientCount', {
    describe: 'Number of parallel subscribers to spawn',
    type: 'number',
    default: 2000
  })
  .option('logMessages', {
    describe: 'Whether to output received messages (0=no, 1=yes)',
    choices: [0, 1],
    type: 'number',
    default: 0
  })
  .help()
  .argv;

// ==== 2) Ensure stats directory exists ====
const statsDir = path.join(process.cwd(), 'stats');
if (!fs.existsSync(statsDir)) {
  fs.mkdirSync(statsDir, { recursive: true });
}

const summaryFile = path.join(statsDir, 'summary.stats');
if (argv.clearStats === 1 || !fs.existsSync(summaryFile)) {
  fs.writeFileSync(summaryFile, JSON.stringify({ total: 0 }, null, 2));
}

// ==== 3) Client & global data tracking ====
let globalTotal = 0;
let globalSession = 0;
const clients = [];

// ==== 4) Start single client ====
function startClient(index) {
  const clientId = `mqttsub_${index + 1}`;
  let sessionReceived = 0;

  console.log(`▶ Starting subscriber (clientId=${clientId})`);

  const client = mqtt.connect(argv.broker, {
    clientId,
    protocolVersion: 5,
    clean: false,
    username: argv.username,
    password: argv.password,
    properties: { sessionExpiryInterval: argv.sessionExpiry }
  });

  client.on('connect', () => {
    console.log(`✅ [${clientId}] Connected to broker`);
    client.subscribe(argv.topic, { qos: argv.qos }, (err, granted) => {
      if (err) console.error(`❌ [${clientId}] Subscribe error:`, err.message);
      else if (granted.length > 0)
        console.log(`🔔 [${clientId}] Subscribed to ${argv.topic} (QoS ${granted[0].qos})`);
    });
  });

  client.on('reconnect', () => console.log(`🔄 [${clientId}] Reconnecting...`));
  client.on('close', () => console.log(`❌ [${clientId}] Connection closed`));
  client.on('error', err => console.error(`🔥 [${clientId}] Error:`, err.message));
  client.on('disconnect', packet => {
    const reason = packet.properties?.reasonString || 'N/A';
    console.log(`⚠️ [${clientId}] Disconnected: code=${packet.reasonCode}, reason=${reason}`);
  });

  client.on('message', (topic, payload) => {
    globalTotal++;
    globalSession++;
    sessionReceived++;

    // Update summary file (non-blocking)
    fs.writeFile(summaryFile, JSON.stringify({ total: globalTotal, session: globalSession }, null, 2), () => {});

    if (argv.logMessages === 1) {
      console.log(`📨 [${clientId}] ${topic} → ${payload.toString()}`);
    }
  });

  return {
    clientId,
    client,
    getSessionReceived: () => sessionReceived
  };
}

// ==== 5) Helper to wait (promise-based delay) ====
function wait(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ==== 6) Launch all clients with batch staggering ====
async function launchClientsInBatches() {
  const batchSize = 100;
  for (let i = 0; i < argv.clientCount; i += batchSize) {
    const end = Math.min(i + batchSize, argv.clientCount);
    for (let j = i; j < end; j++) {
      clients.push(startClient(j));
    }
    if (end < argv.clientCount) {
      console.log(`⏳ Waiting 1000ms before next batch...`);
      await wait(1000);
    }
  }
  startStatsTimer();
}

// ==== 7) Start periodic stat logger ====
function startStatsTimer() {
  setInterval(() => {
    console.log(`📊 Global Stats (last ${argv.statsInterval}s): ⏱️ session=${globalSession}, 📥 total=${globalTotal}`);
    // Optionally reset session count
    // globalSession = 0;
  }, argv.statsInterval * 1000);
}

// ==== 8) Graceful shutdown ====
process.on('SIGINT', () => {
  console.log('\n⏹ Received SIGINT, shutting down all clients...');
  let disconnected = 0;

  function tryExit() {
    disconnected++;
    if (disconnected === clients.length) {
      console.log(`👋 All clients disconnected cleanly.`);
      console.log(`✅ Final Stats: Total messages received: ${globalTotal}`);
      // Save to summary file one last time
      fs.writeFileSync(summaryFile, JSON.stringify({ total: globalTotal, session: globalSession }, null, 2));
      process.exit(0);
    }
  }

  clients.forEach(({ client, getSessionReceived, clientId }) => {
    client.end(false, {}, () => {
      console.log(`🚪 [${clientId}] Disconnected: session=${getSessionReceived()}`);
      tryExit();
    });
  });
});

// ==== 9) Start the process ====
launchClientsInBatches();