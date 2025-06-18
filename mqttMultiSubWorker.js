const mqtt = require('mqtt');
const { parentPort, workerData } = require('worker_threads');

const {
  broker,
  username,
  password,
  topic,
  qos,
  sessionExpiry,
  clearStats,
  startIndex,
  endIndex,
  logMessages,
  workerId
} = workerData;

var LocalTotal = 0;
var LocalSession = 0;

const clients = [];

function wait(ms) {
  return new Promise(res => setTimeout(res, ms));
}

function startClient(index) {
  const clientId = `subcriber_${index + 1}`;
  let sessionReceived = 0;

  const client = mqtt.connect(broker, {
    clientId,
    protocolVersion: 5,
    clean: false,
    username,
    password,
    properties: {
      sessionExpiryInterval: sessionExpiry
    }
  });

  client.on('connect', () => {
    client.subscribe(topic, { qos });
    parentPort.postMessage({ type: 'output', text: `[${clientId}] connected` });
  });

  client.on('message', (topic, payload) => {
    LocalTotal++;
    LocalSession++;
    sessionReceived++;

    if (logMessages == 1) {
      parentPort.postMessage({ type: 'log', text: `[${clientId}] ${topic} → ${payload.toString()}` });
    }
  });

  return { client, clientId };
}

// ==== Batch-wise launch ====
(async () => {
  const batch = 50;
  for (let i = startIndex; i < endIndex; i += batch) {
    const end = Math.min(i + batch, endIndex);
    for (let j = i; j < end; j++) {
      clients.push(startClient(j));
    }
    await wait(1000);
  }

  // Start stats timer
  setInterval(() => {
    if (LocalSession > 0) {
      parentPort.postMessage({ type: 'stats', session: LocalSession, total: LocalTotal });
      LocalSession = 0;
    }
  }, 1000);
})();

// ==== Graceful shutdown ====
parentPort.on('message', msg => {
  if (msg.type === 'shutdown') {
    for (const { client } of clients) {
      client.end(true);
    }
    setTimeout(() => process.exit(0), 1000);
  }
});