#!/usr/bin/env node
const mqtt = require('mqtt');
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');

// ==== 1) Parse CLI flags ====
const argv = yargs(hideBin(process.argv))
  .option('broker', {
    describe: 'MQTT broker URL',
    type: 'string',
    default: 'mqtt://10.17.9.144:1883',
  })
  .option('username', { type: 'string', default: 'user1' })
  .option('password', { type: 'string', default: 'U!*Ec2KRJxpeRKF3-' })
  .option('topic', { type: 'string', default: 'testtopic/test' })
  .option('qos', { type: 'number', choices: [0, 1, 2], default: 0 })
  .option('sessionExpiry', { type: 'number', default: 30 })
  .option('messageExpiry', { type: 'number', default: 180 })
  .option('rate', { type: 'number', default: 10 }) // messages/sec per publisher
  .option('numberMessages', { type: 'number', default: 100 }) // per publisher
  .option('publisherCount', { type: 'number', default: 10 })
  .option('statsInterval', { type: 'number', default: 1 })
  .option('payloadSize', { type: 'number', default: 16 })
  .help()
  .argv;

// ==== 2) Payload filler buffer (shared across publishers) ====
const filler = Buffer.alloc(Math.max(0, argv.payloadSize - 10), 'x');

// ==== 3) Global stats ====
const startTime = Date.now();
let totalPublished = 0;
let totalAcked = 0;
let totalFailed = 0;

// ==== 4) Start a publisher client ====
function startPublisher(index) {
  const clientId = `publisher-${index + 1}`;
  let nextMsg = 1;
  let publishedCount = 0;
  let ackedCount = 0;
  let failedCount = 0;

  const client = mqtt.connect(argv.broker, {
    clientId,
    username: argv.username,
    password: argv.password,
    protocolVersion: 5,
    properties: {
      sessionExpiryInterval: argv.sessionExpiry
    },
    reconnectPeriod: 1000
  });

  client.on('connect', () => {
    console.log(`✅ [${clientId}] Connected to broker (rate=${argv.rate}/s, messages=${argv.numberMessages})`);
    scheduleNext();
  });

  client.on('reconnect', () => console.log(`🔄 [${clientId}] Reconnecting...`));
  client.on('close', () => console.log(`❌ [${clientId}] Disconnected`));
  client.on('error', err => console.error(`🔥 [${clientId}] Error:`, err.message));

  function scheduleNext() {
    if (nextMsg > argv.numberMessages) return;

    const delay = getRandomDelayForRate(argv.rate); // per publisher rate
    setTimeout(() => {
      if (!client.connected) {
        scheduleNext(); // try again later when connected
        return;
      }

      const prefix = `${clientId}-${nextMsg}:`;
      const payload = Buffer.concat([Buffer.from(prefix), filler]);

      client.publish(argv.topic, payload, {
        qos: argv.qos,
        properties: {
          messageExpiryInterval: argv.messageExpiry
        }
      }, err => {
        if (err) {
          failedCount++;
          totalFailed++;
          console.error(`❌ [${clientId}] Msg ${nextMsg} failed`);
        } else {
          ackedCount++;
          totalAcked++;
        }

        publishedCount++;
        totalPublished++;
        nextMsg++;

        // Only schedule next if we're still under the limit
        if (nextMsg <= argv.numberMessages) {
          scheduleNext();
        }
      });
    }, delay);
  }

  return {
    clientId,
    getStats: () => ({
      published: publishedCount,
      acked: ackedCount,
      failed: failedCount
    })
  };
}

// ==== 5) Utility: Get random delay to simulate message jitter (but still avg rate) ====
function getRandomDelayForRate(ratePerSecond) {
  const avgDelay = 1000 / ratePerSecond;
  const variance = avgDelay * 0.3;
  const min = Math.max(0, avgDelay - variance);
  const max = avgDelay + variance;
  return Math.random() * (max - min) + min;
}

// ==== 6) Start all publishers ====
const publishers = [];
for (let i = 0; i < argv.publisherCount; i++) {
  publishers.push(startPublisher(i));
}

// ==== 7) Stats Printer ====
const statsInterval = setInterval(() => {
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`\n⏱ ${elapsed}s — Attempts: ${totalPublished}, Acks: ${totalAcked}, Fails: ${totalFailed}`);

  if (totalAcked >= argv.numberMessages * argv.publisherCount) {
    console.log('🎉 All messages acknowledged by all publishers. Exiting.');
    clearInterval(statsInterval);
    publishers.forEach(p => {
      console.log(`📦 ${p.clientId}:`, p.getStats());
    });
    process.exit(0);
  }
}, argv.statsInterval * 1000);