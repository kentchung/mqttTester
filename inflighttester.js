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
    default: 'mqtt://10.17.10.222:1883',
  })
  .option('username', {
    describe: 'MQTT username (omit or empty for none)',
    type: 'string',
    default: undefined,
  })
  .option('password', {
    describe: 'MQTT password (omit or empty for none)',
    type: 'string',
    default: undefined,
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
    default: 2,
  })
  .option('publishers', {
    describe: 'Number of publishers',
    type: 'number',
    default: 2,
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

// Ensure directories exist
function ensureDir(filePath) {
  const dir = filePath.substring(0, filePath.lastIndexOf('/'));
  if (dir && !fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}
ensureDir(argv.logFile);
ensureDir(argv.csvFile);

// Initialize log file
logStream = fs.createWriteStream(argv.logFile, { flags: 'a' });

// —————————————————————————————————————————————————————————————
// 3) MAIN TEST FUNCTION (INFLIGHT‐MESSAGES VERIFICATION)
// —————————————————————————————————————————————————————————————
async function runInflightTest() {
  try {
    // Extract parameters
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

    // Track received messages per subscriber
    // Map< clientId, Set< `${pubId}:${seq}` > >
    const receivedMap = new Map();
    for (let i = 0; i < numSubs; i++) {
      const clientId = `sub-${i}`;
      receivedMap.set(clientId, new Set());
    }

    // Hold subscriber client objects
    // Each element: { clientId, client }
    const subscribers = [];

    // Common event listeners
    function attachClientLogging(client, type, clientId) {
      client.on('error', (err) => {
        console.error(`${type} ${clientId} ERROR:`, err.message);
      });
      client.on('reconnect', () => {
        console.log(`${type} ${clientId} RECONNECTING...`);
      });
      client.on('close', () => {
        // Connection closed
      });
      client.on('packetsend', (packet) => {
        // no-op
      });
      client.on('packetreceive', (packet) => {
        // no-op
      });
    }

    // —————————————————————————————————————————————
    // 3.1) CREATE SUBSCRIBERS WITH PERSISTENT SESSION
    // —————————————————————————————————————————————
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
        // Extract publisher and sequence: "MSG from pub-0 #3" → pub-0, 3
        const match = payload.match(/^MSG from (pub-\d+) #(\d+)$/);
        if (match) {
          const pubId = match[1];
          const seq = match[2];
          const indexKey = `${pubId}:${seq}`;
          const set = receivedMap.get(clientId);
          set.add(indexKey);
          console.log(`Subscriber ${clientId} RECEIVED: "${payload}" (indexed as ${indexKey})`);
        } else {
          // Fallback: store the whole payload string
          const set = receivedMap.get(clientId);
          set.add(payload);
          console.log(`Subscriber ${clientId} RECEIVED unexpected format: "${payload}"`);
        }
      });

      subscribers.push({ clientId, client: subscriber });
    }

    // Wait for all SUBACKs
    await Promise.all(
      subscribers.map(({ client }) => {
        return new Promise((resolve) => {
          client.on('packetreceive', (packet) => {
            if (packet.cmd === 'suback') {
              resolve();
            }
          });
        });
      })
    );
    console.log('All subscribers are subscribed.');

    // Small delay to ensure broker registration
    await new Promise((r) => setTimeout(r, 2000));

    // —————————————————————————————————————————————
    // 3.2) CREATE PUBLISHERS
    // —————————————————————————————————————————————
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

    // Wait until all publishers connect
    await Promise.all(
      publishers.map(({ client }) => {
        return new Promise((resolve) => {
          client.on('connect', () => resolve());
        });
      })
    );
    console.log('All publishers connected.');
    // Small delay to ensure broker registration
    await new Promise((r) => setTimeout(r, 2000));

    // —————————————————————————————————————————————
    // 3.3) PUBLISH MESSAGES + FORCE SUBSCRIBER OFFLINE MIDWAY (PARALLEL)
    // —————————————————————————————————————————————
    console.log('Publishing messages…');

    let publishedCount = 0;
    let halfwayTriggered = false;

    // Run each publisher’s sequence in parallel
    const publishPromises = publishers.map(({ client: publisher, clientId }) =>
      (async () => {
        for (let seq = 1; seq <= messagesPerPub; seq++) {
          const payload = `MSG from ${clientId} #${seq}`;
          publisher.publish(topic, payload, { qos, retain: false }, (err) => {
            if (err) {
              console.error(`Publish error for "${payload}":`, err.message);
            } else {
              console.log(`Publisher ${clientId} SENT: "${payload}"`);
            }
          });

          publishedCount++;
          if (!halfwayTriggered && publishedCount === halfwayCount) {
            // Ensure any in-flight QoS handshake can finish
            // eslint-disable-next-line no-await-in-loop
            await new Promise((r) => setTimeout(r, 1000));
            halfwayTriggered = true;
            console.log(`\n*** Halfway (${publishedCount}/${totalMessages}). Disconnecting all subscribers…`);
            subscribers.forEach(({ clientId, client }) => {
              client.end(false, {}, () => {
                console.log(`Subscriber ${clientId} disconnected`);
              });
            });
            // Reconnect after short pause (within session expiry)
            setTimeout(() => {
              console.log('Reconnecting all subscribers…');
              for (let i = 0; i < subscribers.length; i++) {
                const { clientId } = subscribers[i];
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
                    if (err) {
                      console.error(`Re‐subscribe error for ${clientId}:`, err.message);
                    }
                  });
                });
                reSub.on('message', (_topic, payloadBuf) => {
                  const payload = payloadBuf.toString().trim();
                  const match = payload.match(/^MSG from (pub-\d+) #(\d+)$/);
                  if (match) {
                    const pubId = match[1];
                    const seq = match[2];
                    const indexKey = `${pubId}:${seq}`;
                    const set = receivedMap.get(clientId);
                    set.add(indexKey);
                    console.log(
                      `Subscriber ${clientId} (reconnected) RECEIVED: "${payload}" (indexed as ${indexKey})`
                    );
                  } else {
                    const set = receivedMap.get(clientId);
                    set.add(payload);
                    console.log(
                      `Subscriber ${clientId} (reconnected) RECEIVED unexpected format: "${payload}"`
                    );
                  }
                });

                subscribers[i].client = reSub;
              }
            }, 3000);
            // eslint-disable-next-line no-await-in-loop
            await new Promise((r) => setTimeout(r, 1000));
          }

          // 200ms delay between publishes
          // eslint-disable-next-line no-await-in-loop
          await new Promise((r) => setTimeout(r, 200));
        }
      })()
    );

    // Wait for all publishers to finish
    await Promise.all(publishPromises);
    console.log(`\nAll ${totalMessages} messages have been sent.`);

    // —————————————————————————————————————————————
    // 3.4) WAIT FOR HARD SESSION EXPIRY WINDOW
    // —————————————————————————————————————————————
    const waitTime = 8000; //Math.max(5000, (sessionExpiry + 5) * 1000);
    console.log(`\nWaiting ${waitTime / 1000}s for broker to deliver pending messages…`);
    await new Promise((r) => setTimeout(r, waitTime));

    // —————————————————————————————————————————————
    // 3.5) VERIFY THAT EACH SUBSCRIBER GOT ALL MESSAGES & BUILD CSV
    // —————————————————————————————————————————————
    console.log('\nVerifying inflight message delivery…');
    const csvRows = [];
    let overallPass = true;

    for (let i = 0; i < numSubs; i++) {
      const clientId = `sub-${i}`;
      const receivedSet = receivedMap.get(clientId);
      const missingByPublisher = {};
      let countReceived = 0;

      // Count how many valid indexed messages we saw
      receivedSet.forEach((key) => {
        if (/^pub-\d+:\d+$/.test(key)) {
          countReceived++;
        }
      });

      // Identify missing indices for each publisher
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
            : ` → FAIL (missing ${Object.values(missingByPublisher).reduce((a, b) => a + b.length, 0)})`)
      );
      if (!pass) {
        for (const pubId of Object.keys(missingByPublisher)) {
          if (missingByPublisher[pubId].length) {
            console.log(
              `  • Missing from ${pubId}: [${missingByPublisher[pubId].join(', ')}]`
            );
          }
        }
      }

      // Build CSV rows
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

    // —————————————————————————————————————————————
    // 3.6) WRITE CSV REPORT
    // —————————————————————————————————————————————
    console.log(`Writing CSV report to ${csvReportPath}…`);
    const fields = ['subscriber', 'publisher', 'index', 'status'];
    const csvParser = new Parser({ fields });
    const csvOutput = csvParser.parse(csvRows);
    fs.writeFileSync(csvReportPath, csvOutput);
    console.log('CSV report generation complete.');

    // —————————————————————————————————————————————
    // 3.7) CLEAN UP ALL MQTT CONNECTIONS
    // —————————————————————————————————————————————
    console.log('\nClosing all MQTT client connections…');
    await Promise.all(
      subscribers.map(
        ({ clientId, client }) =>
          new Promise((resolve) => {
            client.end(false, {}, () => {
              resolve();
            });
          })
      )
    );
    await Promise.all(
      publishers.map(
        ({ clientId, client }) =>
          new Promise((resolve) => {
            client.end(false, {}, () => {
              resolve();
            });
          })
      )
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
