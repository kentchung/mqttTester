#!/usr/bin/env node

const mqtt = require('mqtt');
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');

// parse flags
const argv = yargs(hideBin(process.argv))
  .option('broker', {
    describe: 'MQTT broker URL',
    type: 'string',
    default: 'mqtt://192.168.0.78:1883',
  })
  .option('username', { type: 'string', default: 'user1' })
  .option('password', { type: 'string', default: 'LxUYCw@VUVzKEr6K_' })
  .option('topic', { type: 'string', default: 'testtopic/test' })
  .option('qos', { type: 'number', choices: [0,1,2], default: 2 })
  .option('sessionExpiry', { type: 'number', default: 120 })
  .option('messageExpiry', { type: 'number', default: 180 })
  .option('rate', { type: 'number', default: 10 })
  .option('numberMessages', { type: 'number', default: 1000 })
  .option('statsInterval', { type: 'number', default: 5 })
  .option('payloadSize', { type: 'number', default: 100 })
  .help()
  .argv;

// state
let nextMsg = 1;
let publishedCount = 0;
let ackedCount = 0;
let failedCount = 0;
const startTime = Date.now();

// build static payload buffer (we’ll prefix message index each time)
const filler = Buffer.alloc(Math.max(0, argv.payloadSize - 10), 'x'); 

// connect
const client = mqtt.connect(argv.broker, {
  username: argv.username,
  password: argv.password,
  protocolVersion: 5,
  properties: {
    sessionExpiryInterval: argv.sessionExpiry
  },
  reconnectPeriod: 1000,     // retry every 1s
});

client.on('connect', () => {
  console.log(`✅ Connected. Will publish ${argv.numberMessages} msgs at ${argv.rate}/s`);
});
client.on('reconnect', () => console.log('🔄 Reconnecting...'));
client.on('close', () => console.log('❌ Disconnected'));
client.on('error', err => console.error('🔥 Error:', err.message));

// publish loop
const sendInterval = setInterval(() => {
  if (nextMsg > argv.numberMessages) return;
  if (!client.connected) return;

  // prepare message: "<idx>:<payload…>"
  const prefix = `${nextMsg}:`;
  const payload = Buffer.concat([
    Buffer.from(prefix),
    filler
  ]);

  client.publish(argv.topic, payload, {
    qos: argv.qos,
    properties: {
      messageExpiryInterval: argv.messageExpiry
    }
  }, err => {
    if (err) {
      failedCount++;
      console.error(`❌ Msg ${nextMsg} failed to publish`);
    } else {
      ackedCount++;
    }
  });

  publishedCount++;
  nextMsg++;
}, 1000 / argv.rate);

// stats printer
const statsTimer = setInterval(() => {
  const elapsed = ((Date.now() - startTime)/1000).toFixed(1);
  console.log(`⏱ ${elapsed}s — attempted: ${publishedCount}/${argv.numberMessages}, acks: ${ackedCount}, fails: ${failedCount}`);
  if (ackedCount >= argv.numberMessages) {
    console.log('🎉 All messages acknowledged. Exiting.');
    clearInterval(sendInterval);
    clearInterval(statsTimer);
    client.end(false, () => process.exit(0));
  }
}, argv.statsInterval * 1000);
