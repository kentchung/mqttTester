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
    describe: 'Base topic to publish/subscribe (no $share prefix)',
    type: 'string',
    default: 'test/topic',
  })
  .option('qos', {
    describe: 'QoS level (0, 1, or 2)',
    type: 'number',
    choices: [0, 1, 2],
    default: 2,
  })
  .option('groups', {
    describe: 'Number of shared‐subscription groups',
    type: 'number',
    default: 1,
  })
  .option('subsPerGroup', {
    describe: 'Number of subscribers in each shared‐subscription group',
    type: 'number',
    default: 2,
  })
  .option('publishers', {
    describe: 'Number of publishers',
    type: 'number',
    default: 1,
  })
  .option('messagesPerPub', {
    describe: 'Number of messages per publisher',
    type: 'number',
    default: 10,
  })
  .option('edgeDisconnect', {
    describe:
      'Include “disconnect one subscriber mid‐test” edge‐case (true/false)',
    type: 'boolean',
    default: false,
  })
  .option('logFile', {
    describe: 'Path to save log file',
    type: 'string',
    default: './logs/shared-test.log',
  })
  .option('csvFile', {
    describe: 'Path to save CSV report file',
    type: 'string',
    default: './reports/shared-report.csv',
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
// 3) MAIN TEST FUNCTION (SHARED‐SUBSCRIPTION VERIFICATION)
// —————————————————————————————————————————————————————————————
async function runSharedSubscriptionTest() {
  try {
    // Extract parameters from argv
    const brokerUrl = argv.broker;
    const username = argv.username || undefined;
    const password = argv.password || undefined;
    const baseTopic = argv.topic;
    const qos = argv.qos;
    const numGroups = argv.groups;
    const subsPerGroup = argv.subsPerGroup;
    const numPublishers = argv.publishers;
    const messagesPerPub = argv.messagesPerPub;
    const edgeDisconnect = argv.edgeDisconnect;
    const logFilePath = argv.logFile;
    const csvReportPath = argv.csvFile;

    // Validate
    if (numGroups < 1 || subsPerGroup < 1 || numPublishers < 1) {
      console.error(
        'Error: --groups, --subsPerGroup, and --publishers must all be ≥ 1.'
      );
      process.exit(1);
    }

    console.log(`
Test Configuration:
  - Broker URL:             ${brokerUrl}
  - Username / Password:    ${username || '<none>'} / ${password ? '******' : '<none>'}
  - Base topic:             ${baseTopic}
  - QoS:                    ${qos}
  - Shared groups:          ${numGroups} (each with ${subsPerGroup} subscribers)
  - Publishers:             ${numPublishers} (each sending ${messagesPerPub} messages)
  - Edge case (disconnect): ${edgeDisconnect ? 'Yes' : 'No'}

Log file: ${logFilePath}
CSV report: ${csvReportPath}
    `);

    // Data structures to track received messages per group
    // groupsReceivedMap: Map< groupName, Map< payloadString, Array<subscriberClientId> > >
    const groupsReceivedMap = new Map();
    for (let g = 0; g < numGroups; g++) {
      const groupName = `Group${g + 1}`;
      groupsReceivedMap.set(groupName, new Map());
    }

    // Keep references to client objects
    const allSubscribers = []; // { groupName, clientId, client }
    const allPublishers = []; // { clientId, client }

    // Common event listeners
    function attachClientLogging(client, type, clientId) {
      client.on('error', (err) => {
        console.error(`${type} ${clientId} ERROR:`, err.message);
      });
      client.on('reconnect', () => {
        console.log(`${type} ${clientId} RECONNECTING...`);
      });
      client.on('close', () => {
        console.log(`${type} ${clientId} CONNECTION CLOSED`);
      });
      client.on('packetsend', (packet) => {
        // console.log(`${type} ${clientId} SENT packet: ${packet.cmd}`);
      });
      client.on('packetreceive', (packet) => {
        // console.log(`${type} ${clientId} RECV packet: ${packet.cmd}`);
      });
    }

    // ——————————————————————————————————————————
    // 3.1) CREATE SUBSCRIBERS FOR EACH SHARED GROUP
    // ——————————————————————————————————————————
    console.log('Initializing subscribers…');

    for (let g = 0; g < numGroups; g++) {
      const groupName = `Group${g + 1}`;
      const shareTopic = `$share/${groupName}/${baseTopic}`;

      for (let s = 0; s < subsPerGroup; s++) {
        // const suffix = Math.random().toString(16).slice(3, 8);
        const clientId = `${groupName}-${s}`;   //-${suffix}`;

        const subOptions = {
          clientId,
          clean: false, // persistent session
          protocolVersion: 5,
          properties: {
            sessionExpiryInterval: 300,
            receiveMaximum: 16,
          },
          username,
          password,
          reconnectPeriod: 5000,
        };

        const subscriber = mqtt.connect(brokerUrl, subOptions);
        attachClientLogging(subscriber, 'SUB', clientId);

        allSubscribers.push({ groupName, clientId, client: subscriber });

        subscriber.on('connect', () => {
          console.log(`Subscriber ${clientId} connected—subscribing to ${shareTopic}`);
          subscriber.subscribe(shareTopic, { qos }, (err) => {
            if (err) {
              console.error(`Subscriber ${clientId} failed to subscribe:`, err.message);
            } else {
              // console.log(`Subscriber ${clientId} subscribed to ${shareTopic}`);
            }
          });
        });

        subscriber.on('message', (topic, payloadBuf) => {
          const payload = payloadBuf.toString().trim();
          const recvMap = groupsReceivedMap.get(groupName);
          if (!recvMap.has(payload)) {
            recvMap.set(payload, []);
          }
          recvMap.get(payload).push(clientId);
          console.log(`Subscriber ${clientId} (in ${groupName}) RECEIVED: "${payload}"`);
        });
      }
    }

    // Wait for all SUBACKs
    await Promise.all(
      allSubscribers.map(({ client }) => {
        return new Promise((resolve) => {
          client.on('packetreceive', (packet) => {
            if (packet.cmd === 'suback') {
              resolve();
            }
          });
        });
      })
    );
    console.log('All subscribers are now subscribed.');

    // Small delay to ensure broker registration
    await new Promise((r) => setTimeout(r, 2000));

    // ——————————————————————————————————————————
    // 3.2) CREATE PUBLISHERS
    // ——————————————————————————————————————————
    console.log('Initializing publishers…');

    for (let p = 0; p < numPublishers; p++) {
      // const suffix = Math.random().toString(16).slice(3, 8);
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

      allPublishers.push({ clientId, client: publisher });
    }

    // Wait until all publishers connect
    await Promise.all(
      allPublishers.map(({ client }) => {
        return new Promise((resolve) => {
          client.on('connect', () => resolve());
        });
      })
    );
    console.log('All publishers connected.');

    // ——————————————————————————————————————————
    // 3.3) PUBLISH MESSAGES (WITH OPTIONAL EDGE‐CASE)
    // ——————————————————————————————————————————
    console.log('Publishing messages…');

    const totalMessages = numPublishers * messagesPerPub;
    let publishedCount = 0;
    const halfwayCount = Math.floor(totalMessages / 2);
    let halfwayTriggered = false;

    for (let { client: publisher, clientId } of allPublishers) {
      for (let seq = 1; seq <= messagesPerPub; seq++) {
        const payload = `MSG from ${clientId} #${seq}`;
        publisher.publish(baseTopic, payload, { qos, retain: false }, (err) => {
          if (err) {
            console.error(`Publish error for "${payload}":`, err.message);
          } else {
            // console.log(`Publisher ${clientId} SENT: "${payload}"`);
          }
        });

        publishedCount++;
        if (edgeDisconnect && !halfwayTriggered && publishedCount === halfwayCount) {
          halfwayTriggered = true;
          console.log(`\n*** Halfway through (${publishedCount}/${totalMessages}). Disconnecting one subscriber per group…`);
          for (let g = 0; g < numGroups; g++) {
            const groupName = `Group${g + 1}`;
            const toDisc = allSubscribers.find((s) => s.groupName === groupName);
            if (toDisc) {
              console.log(`>>> Forcing DISCONNECT of subscriber ${toDisc.clientId} in ${groupName}`);
              toDisc.client.end(false, {}, () => {
                console.log(`>>> Subscriber ${toDisc.clientId} disconnected`);
              });

              setTimeout(() => {
                console.log(`>>> Reconnecting subscriber ${toDisc.clientId} in ${groupName}`);
                const newOpts = {
                  clientId: toDisc.clientId,
                  clean: false,
                  protocolVersion: 5,
                  properties: {
                    sessionExpiryInterval: 300,
                    receiveMaximum: 16,
                  },
                  username,
                  password,
                  reconnectPeriod: 5000,
                };
                const reSub = mqtt.connect(brokerUrl, newOpts);
                attachClientLogging(reSub, 'SUB(reconnected)', toDisc.clientId);

                reSub.on('connect', () => {
                  console.log(`>>> Subscriber ${toDisc.clientId} reconnected—resubscribing to $share/${groupName}/${baseTopic}`);
                  reSub.subscribe(`$share/${groupName}/${baseTopic}`, { qos }, (err) => {
                    if (err) {
                      console.error(`Re‐subscribe error for ${toDisc.clientId}:`, err.message);
                    } else {
                      console.log(`>>> Subscriber ${toDisc.clientId} re‐subscribed successfully`);
                    }
                  });
                });

                reSub.on('message', (topic, payloadBuf) => {
                  const payloadStr = payloadBuf.toString().trim();
                  const recvMap = groupsReceivedMap.get(groupName);
                  if (!recvMap.has(payloadStr)) {
                    recvMap.set(payloadStr, []);
                  }
                  recvMap.get(payloadStr).push(toDisc.clientId);
                  console.log(`>>> (Reconnected) Subscriber ${toDisc.clientId} RECEIVED: "${payloadStr}"`);
                });

                toDisc.client = reSub;
              }, 5000);
            }
          }
        }

        // 300ms delay between publishes
        // eslint-disable-next-line no-await-in-loop
        await new Promise((r) => setTimeout(r, 300));
      }
    }

    console.log(`\nAll ${totalMessages} messages have been sent.`);

    // ——————————————————————————————————————————
    // 3.4) WAIT FOR IN‐FLIGHT DELIVERIES
    // ——————————————————————————————————————————
    console.log('\nWaiting 5 seconds for in‐flight deliveries…');
    await new Promise((r) => setTimeout(r, 5000));

    // ——————————————————————————————————————————
    // 3.5) VERIFY “EXACTLY ONCE” PER GROUP & BUILD CSV
    // ——————————————————————————————————————————
    console.log('\nVerifying shared‐subscription semantics…');
    const csvRows = [];
    let overallPass = true;

    for (let g = 0; g < numGroups; g++) {
      const groupName = `Group${g + 1}`;
      const recvMap = groupsReceivedMap.get(groupName);

      console.log(`\n—– Results for ${groupName} —–`);
      let groupPass = true;

      // Check missing
      const missingPayloads = [];
      for (let { clientId } of allPublishers) {
        for (let seq = 1; seq <= messagesPerPub; seq++) {
          const expected = `MSG from ${clientId} #${seq}`;
          if (!recvMap.has(expected)) {
            missingPayloads.push(expected);
          }
        }
      }
      if (missingPayloads.length) {
        groupPass = false;
      }

      // Check duplicates
      const duplicates = [];
      for (let [payload, subList] of recvMap.entries()) {
        if (subList.length !== 1) {
          groupPass = false;
          if (subList.length > 1) {
            duplicates.push({ payload, subscribers: [...subList] });
          }
        }
      }

      if (groupPass) {
        console.log(`${groupName}: PASS (without missing/duplicate deliveries)`);
      } else {
        overallPass = false;
        console.log(`${groupName}: FAIL`);
        if (missingPayloads.length) {
          console.log(
            `  → Missing (${missingPayloads.length}):`,
            missingPayloads.slice(0, 5).map((s) => `"${s}"`),
            missingPayloads.length > 5 ? `(+${missingPayloads.length - 5} more)` : ''
          );
        }
        if (duplicates.length) {
          console.log(`  → Duplicate deliveries for ${duplicates.length} message(s)`);
          duplicates.slice(0, 3).forEach((d) => {
            console.log(`     • "${d.payload}" → [${d.subscribers.join(', ')}]`);
          });
          if (duplicates.length > 3) {
            console.log(`     (+${duplicates.length - 3} more)`);
          }
        }
      }

      // Build CSV rows
      for (let { clientId } of allPublishers) {
        for (let seq = 1; seq <= messagesPerPub; seq++) {
          const payload = `MSG from ${clientId} #${seq}`;
          const receivedByArr = recvMap.get(payload) || [];
          const status = receivedByArr.length === 1 ? 'Pass' : 'Fail';
          csvRows.push({
            group: groupName,
            message: payload,
            publisher: clientId,
            sequence: seq,
            receivedBy: receivedByArr.length === 1 ? receivedByArr[0] : receivedByArr.join('|'),
            status,
          });
        }
      }
    }

    console.log(
      `\n===== FINAL OUTCOME: ${overallPass ? 'ALL GROUPS PASS' : 'SOME GROUPS FAILED'} =====\n`
    );

    // ——————————————————————————————————————————
    // 3.5.1) **NEW**: PER‐SUBSCRIBER MESSAGE COUNT SUMMARY
    // ——————————————————————————————————————————
    console.log('\nPer‐subscriber message counts:');
    for (let g = 0; g < numGroups; g++) {
      const groupName = `Group${g + 1}`;
      const recvMap = groupsReceivedMap.get(groupName);

      // Initialize a count object for each subscriber in this group
      const subCount = {};
      allSubscribers
        .filter((s) => s.groupName === groupName)
        .forEach((s) => {
          subCount[s.clientId] = 0;
        });

      // Tally up each occurrence
      for (let [_, subscriberList] of recvMap.entries()) {
        subscriberList.forEach((subId) => {
          if (subCount[subId] !== undefined) {
            subCount[subId]++;
          }
        });
      }

      console.log(`  ${groupName}:`);
      let total = 0;
      Object.entries(subCount).forEach(([subId, count]) => {
        console.log(`    • ${subId}: ${count} messages`);
        total += count;
      });
      console.log(`    • Group${g}: ${total} messages`);
    }

    // ——————————————————————————————————————————
    // 3.6) WRITE CSV REPORT
    // ——————————————————————————————————————————
    console.log(`Writing CSV report to ${csvReportPath}…`);
    const fields = ['group', 'message', 'publisher', 'sequence', 'receivedBy', 'status'];
    const csvParser = new Parser({ fields });
    const csvOutput = csvParser.parse(csvRows);
    fs.writeFileSync(csvReportPath, csvOutput);
    console.log('CSV report generation complete.');

    // ——————————————————————————————————————————
    // 3.7) CLEAN UP ALL MQTT CONNECTIONS
    // ——————————————————————————————————————————
    console.log('\nClosing all MQTT client connections…');
    await Promise.all(
      allSubscribers.map(
        (s) =>
          new Promise((resolve) => {
            s.client.end(false, {}, () => {
              console.log(`  • Subscriber ${s.clientId} closed`);
              resolve();
            });
          })
      )
    );
    await Promise.all(
      allPublishers.map(
        (p) =>
          new Promise((resolve) => {
            p.client.end(false, {}, () => {
              console.log(`  • Publisher ${p.clientId} closed`);
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

runSharedSubscriptionTest();
