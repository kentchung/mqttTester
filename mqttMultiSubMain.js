#!/usr/bin/env node
const os = require('os');
const path = require('path');
const { Worker } = require('worker_threads');
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');


// ==== Parse CLI ====
const argv = yargs(hideBin(process.argv))
  .option('broker', { type: 'string', default: 'mqtt://10.17.9.144:1883' })
  .option('username', { type: 'string', default: 'user1' })
  .option('password', { type: 'string', default: 'U!*Ec2KRJxpeRKF3-' })
  .option('topic', { type: 'string', default: 'testtopic/test' })
  .option('qos', { type: 'number', default: 1 })
  .option('clientCount', { type: 'number', default: 1000 })
  .option('sessionExpiry', { type: 'number', default: 30 })
  .option('statsInterval', { type: 'number', default: 1 })
  .option('clearStats', { type: 'number', choices: [0, 1], default: 1 })
  .option('logMessages', { type: 'number', choices: [0, 1], default: 0 })
  .option('batchSize', { type: 'number', default: 100 })
  .help()
  .argv;

// ==== CPU-aware worker balancing ====
const cpuCount = 10; //os.cpus().length;
const numWorkers = Math.min(cpuCount, argv.clientCount);
const baseClientsPerWorker = Math.floor(argv.clientCount / numWorkers);
const summary = {
  total: 0,
  session: 0
};

console.log(`📈 Spawning ${numWorkers} workers for ${argv.clientCount} clients`);

// ==== Launch workers ====
let shuttingDown = false;
let workers = [];
let totals = new Map();
let workersDone = 0;


for (let i = 0; i < numWorkers; i++) {
  const clientStart = i * baseClientsPerWorker;
  const clientEnd = (i === numWorkers - 1) ? argv.clientCount : clientStart + baseClientsPerWorker;

  const worker = new Worker(path.join(__dirname, 'mqttMultiSubWorker.js'), {
    workerData: {
      ...argv,
      startIndex: clientStart,
      endIndex: clientEnd,
      workerId: i
    }
  });

  totals.set(worker, 0);

  worker.on('message', (msg) => {
    if (msg.type === 'stats') {
    //   summary.total += msg.total;
    //   summary.session += msg.session;
    totals[worker] = msg.total;

    }
    if (msg.type === 'log' && argv.logMessages === 1) {
      console.log(msg.text);
    }
    if (msg.type === 'output') {
      console.log(msg.text);
    }
  });

  worker.on('exit', () => {
    workersDone++;
    if (workersDone === numWorkers) {
      console.log('✅ All workers completed.');
      console.log(`✔ Final Total Received Messages: ${summary.total}`);
      process.exit(0);
    }
  });

  workers.push(worker);
}

// ==== Periodic global stats log ====
setInterval(() => {

    // Sum all values
let sum = 0;
for (let i = 0; i < workers.length; i++) {
  sum += totals[workers[i]];
}

  console.log(`📊 Aggregated Stats: total=${sum}, session=${summary.session}`);
  //summary.session = 0;
}, argv.statsInterval * 1000);

// ==== Graceful Shutdown ====
process.on('SIGINT', () => {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log('\n⏹ SIGINT received. Stopping workers...');
  for (const w of workers) w.postMessage({ type: 'shutdown' });
});