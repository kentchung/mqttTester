/* eslint-disable no-console */
import mqtt from 'mqtt';
import fs from 'fs';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import { Parser } from 'json2csv';

// —————————————————————————————————————————————————————————————
// 1) PARSE COMMAND‐LINE FLAGS VIA YARGS
// —————————————————————————————————————————————————————————————
const argv = yargs(hideBin(process.argv))
  .option('broker', {
    describe: 'MQTT broker URL',
    type: 'string',
    default: 'mqtt://10.17.9.83:1883',
  })
  .option('username', {
    describe: 'MQTT username (omit or empty for none)',
    type: 'string',
    default: 'user1',
  })
  .option('password', {
    describe: 'MQTT password (omit or empty for none)',
    type: 'string',
    default: 'iH@gdsHLHBuT7G.qT',
  })
  .option('topic', {
    describe: 'Topic to publish/subscribe',
    type: 'string',
    default: 'testtopic/test',
  })
  .option('qos', {
    describe: 'QoS level (0, 1, or 2)',
    type: 'number',
    choices: [0, 1, 2],
    default: 2,
  })
  .option('sessionExpiry', {
    describe: 'Session expiry interval (seconds) for subscribers',
    type: 'number',
    default: 5,
  })
  .option('subs', {
    describe: 'Number of subscribers',
    type: 'number',
    default: 20,
  })
  .option('publishers', {
    describe: 'Number of publishers',
    type: 'number',
    default: 20,
  })
  .option('messagesPerPub', {
    describe: 'Number of messages per publisher',
    type: 'number',
    default: 20,
  })
  .option('logFile', {
    describe: 'Path to save log file',
    type: 'string',
    default: './logs/inflight-test.log',
  })
  .option('csvFile', {
    describe: 'Path to save CSV report file',
    type: 'string',
    default: './reports/inflight-report.csv',
  })
  .help()
  .argv;

// —————————————————————————————————————————————————————————————
// 2) SET UP LOGGING TO FILE + TIMESTAMP
// —————————————————————————————————————————————————————————————
let logStream = null;
let logStreamClosed = false;
const originalConsoleLog = console.log;
console.log = function (...args) {
  const timestamp = new Date().toISOString();
  const message = `[${timestamp}] ${args.join(' ')}\n`;
  if (logStream && !logStreamClosed) {
    logStream.write(message);
  }
  originalConsoleLog.apply(console, args);
};
function ensureDir(filePath) {
  const dir = filePath.substring(0, filePath.lastIndexOf('/'));
  if (dir && !fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}
ensureDir(argv.logFile);
ensureDir(argv.csvFile);
logStream = fs.createWriteStream(argv.logFile, { flags: 'a' });

// —————————————————————————————————————————————————————————————
// 3) MAIN TEST FUNCTION (INFLIGHT‐MESSAGES VERIFICATION)
// —————————————————————————————————————————————————————————————
async function runInflightTest() {
  try {
    const brokerUrl = argv.broker;
    const username = argv.username || undefined;
    const password = argv.password || undefined;
    const topic = argv.topic;
    const qos = argv.qos;
    const sessionExpiry = argv.sessionExpiry;
    const numSubs = argv.subs;
    const numPublishers = argv.publishers;
    const messagesPerPub = argv.messagesPerPub;
    const logFilePath = argv.logFile;
    const csvReportPath = argv.csvFile;
    const totalMessages = numPublishers * messagesPerPub;
    const halfwayCount = Math.floor(totalMessages / 5);
    if (numSubs < 1 || numPublishers < 1) {
      console.error('Error: --subs and --publishers must both be ≥ 1.');
      process.exit(1);
    }

    console.log(`
Test Configuration:
  - Broker URL:             ${brokerUrl}
  - Username / Password:    ${username || '<none>'} / ${password ? '******' : '<none>'}
  - Topic:                  ${topic}
  - QoS:                    ${qos}
  - Session Expiry:         ${sessionExpiry}s
  - Subscribers:            ${numSubs}
  - Publishers:             ${numPublishers}
  - Messages Per Publisher: ${messagesPerPub}
Log file: ${logFilePath}
CSV report: ${csvReportPath}
    `);

    const receivedMap = new Map();
    for (let i = 0; i < numSubs; i++) {
      receivedMap.set(`sub-${i}`, new Set());
    }

    const subscribers = [];

    function attachClientLogging(client, type, clientId) {
      client.on('error', (err) => {
        console.error(`${type} ${clientId} ERROR:`, err.message);
      });
      client.on('reconnect', () => {
        console.log(`${type} ${clientId} RECONNECTING...`);
      });
      client.on('close', () => {});
      client.on('packetsend', () => {});
      client.on('packetreceive', () => {});
    }

    console.log('Initializing subscribers…');
    for (let i = 0; i < numSubs; i++) {
      const clientId = `sub-${i}`;
      const subOptions = {
        clientId,
        clean: false,
        protocolVersion: 5,
        properties: {
          sessionExpiryInterval: sessionExpiry,
          receiveMaximum: 65535,
        },
        username,
        password,
        reconnectPeriod: 5000,
      };
      const subscriber = mqtt.connect(brokerUrl, subOptions);
      attachClientLogging(subscriber, 'SUB', clientId);
      subscriber.on('connect', () => {
        console.log(`Subscriber ${clientId} connected—subscribing to ${topic}`);
        subscriber.subscribe(topic, { qos }, (err) => {
          if (err) {
            console.error(`Subscriber ${clientId} failed to subscribe:`, err.message);
          }
        });
      });
      subscriber.on('message', (_topic, payloadBuf) => {
        const payload = payloadBuf.toString().trim();
        const match = payload.match(/^MSG from (pub-\d+) #(\d+)$/);
        const set = receivedMap.get(clientId);
        if (match) {
          const pubId = match[1];
          const seq = match[2];
          set.add(`${pubId}:${seq}`);
          console.log(`Subscriber ${clientId} RECEIVED: "${payload}"`);
        } else {
          set.add(payload);
          console.log(`Subscriber ${clientId} RECEIVED unexpected format: "${payload}"`);
        }
      });
      subscribers.push({ clientId, client: subscriber });
    }

    await Promise.all(
      subscribers.map(({ client }) => {
        return new Promise((resolve) => {
          client.on('packetreceive', (packet) => {
            if (packet.cmd === 'suback') resolve();
          });
        });
      })
    );
    console.log('All subscribers are subscribed.');
    await new Promise((r) => setTimeout(r, 2000));

    console.log('Initializing publishers…');
    const publishers = [];
    for (let p = 0; p < numPublishers; p++) {
      const clientId = `pub-${p}`;
      const pubOptions = {
        clientId,
        clean: true,
        username,
        password,
      };
      const publisher = mqtt.connect(brokerUrl, pubOptions);
      attachClientLogging(publisher, 'PUB', clientId);
      publisher.on('connect', () => {
        console.log(`Publisher ${clientId} CONNECTED`);
      });
      publishers.push({ clientId, client: publisher });
    }

    await Promise.all(
      publishers.map(({ client }) => {
        return new Promise((resolve) => {
          client.on('connect', () => resolve());
        });
      })
    );
    console.log('All publishers connected.');
    await new Promise((r) => setTimeout(r, 2000));

    console.log('Publishing messages…');
    let publishedCount = 0;
    let halfwayTriggered = false;

    const publishPromises = publishers.map(({ client: publisher, clientId }) =>
      (async () => {
        for (let seq = 1; seq <= messagesPerPub; seq++) {
          const payload = `MSG from ${clientId} #${seq}`;
          publisher.publish(topic, payload, { qos }, (err) => {
            if (err) console.error(`Publish error:`, err.message);
            else console.log(`Publisher ${clientId} SENT: "${payload}"`);
          });

          publishedCount++;
          if (!halfwayTriggered && publishedCount >= halfwayCount) {
            await new Promise((r) => setTimeout(r, 1000));
            halfwayTriggered = true;
            console.log(
              `\n*** Halfway (${publishedCount}/${totalMessages}). Staggered disconnect of subscribers…`
            );

            // STAGGERED DISCONNECT
            subscribers.forEach(({ clientId, client }, index) => {
              setTimeout(() => {
                client.end(false, {}, () => {
                  console.log(`Subscriber ${clientId} disconnected (after ${index * 100}ms delay)`);
                });
              }, index * 100);
            });

            // STAGGERED RECONNECT AFTER DELAY
            setTimeout(() => {
              console.log('Staggered reconnect of subscribers…');
              for (let i = 0; i < subscribers.length; i++) {
                const { clientId } = subscribers[i];
                setTimeout(() => {
                  const opts = {
                    clientId,
                    clean: false,
                    protocolVersion: 5,
                    properties: {
                      sessionExpiryInterval: sessionExpiry,
                      receiveMaximum: 65535,
                    },
                    username,
                    password,
                    reconnectPeriod: 5000,
                  };
                  const reSub = mqtt.connect(brokerUrl, opts);
                  attachClientLogging(reSub, 'SUB(reconnected)', clientId);
                  reSub.on('connect', () => {
                    console.log(`Subscriber ${clientId} reconnected—resubscribing to ${topic}`);
                    reSub.subscribe(topic, { qos }, (err) => {
                      if (err) console.error(`Re‐subscribe error for ${clientId}:`, err.message);
                    });
                  });
                  reSub.on('message', (_topic, payloadBuf) => {
                    const payload = payloadBuf.toString().trim();
                    const match = payload.match(/^MSG from (pub-\d+) #(\d+)$/);
                    const set = receivedMap.get(clientId);
                    if (match) {
                      const pubId = match[1];
                      const seq = match[2];
                      set.add(`${pubId}:${seq}`);
                      console.log(
                        `Subscriber ${clientId} (reconnected) RECEIVED: "${payload}"`
                      );
                    } else {
                      set.add(payload);
                      console.log(
                        `Subscriber ${clientId} (reconnected) RECEIVED unexpected format: "${payload}"`
                      );
                    }
                  });
                  subscribers[i].client = reSub;
                }, i * 200);
              }
            }, 3000);
            await new Promise((r) => setTimeout(r, 1000));
          }

          await new Promise((r) => setTimeout(r, 200));
        }
      })()
    );

    await Promise.all(publishPromises);
    console.log(`\nAll ${totalMessages} messages have been sent.`);

    const waitTime = 8000;
    console.log(`\nWaiting ${waitTime / 1000}s for broker to deliver pending messages…`);
    await new Promise((r) => setTimeout(r, waitTime));

    console.log('\nVerifying inflight message delivery…');
    const csvRows = [];
    let overallPass = true;

    for (let i = 0; i < numSubs; i++) {
      const clientId = `sub-${i}`;
      const receivedSet = receivedMap.get(clientId);
      const missingByPublisher = {};
      let countReceived = 0;
      receivedSet.forEach((key) => {
        if (/^pub-\d+:\d+$/.test(key)) countReceived++;
      });

      for (let { clientId: pubId } of publishers) {
        missingByPublisher[pubId] = [];
        for (let seq = 1; seq <= messagesPerPub; seq++) {
          const indexKey = `${pubId}:${seq}`;
          if (!receivedSet.has(indexKey)) {
            missingByPublisher[pubId].push(seq);
          }
        }
      }

      const totalReceived = countReceived;
      const pass = Object.values(missingByPublisher).every((arr) => arr.length === 0);
      if (!pass) overallPass = false;
      console.log(
        `Subscriber ${clientId}: received ${totalReceived}/${totalMessages}` +
          (pass
            ? ' → PASS'
            : ` → FAIL (missing ${Object.values(missingByPublisher).reduce(
                (a, b) => a + b.length,
                0
              )})`)
      );
      if (!pass) {
        for (const pubId of Object.keys(missingByPublisher)) {
          if (missingByPublisher[pubId].length) {
            console.log(`  • Missing from ${pubId}: [${missingByPublisher[pubId].join(', ')}]`);
          }
        }
      }

      for (let { clientId: pubId } of publishers) {
        for (let seq = 1; seq <= messagesPerPub; seq++) {
          const indexKey = `${pubId}:${seq}`;
          const status = receivedSet.has(indexKey) ? 'Pass' : 'Fail';
          csvRows.push({
            subscriber: clientId,
            publisher: pubId,
            index: seq,
            status,
          });
        }
      }
    }

    console.log(
      `\n===== FINAL OUTCOME: ${
        overallPass ? 'ALL SUBSCRIBERS PASSED' : 'ONE OR MORE SUBSCRIBERS FAILED'
      } =====\n`
    );

    console.log(`Writing CSV report to ${csvReportPath}…`);
    const fields = ['subscriber', 'publisher', 'index', 'status'];
    const csvParser = new Parser({ fields });
    const csvOutput = csvParser.parse(csvRows);
    fs.writeFileSync(csvReportPath, csvOutput);
    console.log('CSV report generation complete.');

    console.log('\nClosing all MQTT client connections…');
    await Promise.all(
      subscribers.map(({ client }) => new Promise((resolve) => client.end(false, {}, resolve)))
    );
    await Promise.all(
      publishers.map(({ client }) => new Promise((resolve) => client.end(false, {}, resolve)))
    );

    logStreamClosed = true;
    logStream.end(() => {
      console.log('\nLog stream closed. Test complete.');
      process.exit(0);
    });
  } catch (err) {
    console.error('Test encountered an error:', err.message);
    if (logStream && !logStreamClosed) {
      logStreamClosed = true;
      logStream.end(() => process.exit(1));
    } else {
      process.exit(1);
    }
  }
}

runInflightTest();