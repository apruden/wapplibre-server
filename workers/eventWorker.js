import { parentPort } from 'node:worker_threads';
import db from '../db.js';
import logger from '../logger.js';

const BATCH_SIZE = 50;

// Simple notify queue for blocking waits
const signals = [];
let notifyResolver = null;
function notify() {
  if (notifyResolver) {
    const r = notifyResolver;
    notifyResolver = null;
    r();
  } else {
    signals.push(true);
  }
}

function waitForNotify() {
  if (signals.length > 0) {
    signals.pop();
    return Promise.resolve();
  }
  return new Promise((resolve) => {
    notifyResolver = resolve;
  });
}

if (parentPort) {
  parentPort.on('message', (msg) => {
    if (msg === 'notify') notify();
    if (msg === 'shutdown') process.exit(0);
  });
}

async function fetchAndLogBatch() {
  const rows = db.query('select id, data from event limit ?', [BATCH_SIZE]);

  if (!rows || rows.length === 0) {
    // Block until notified of new events
    await waitForNotify();
    return;
  }

  for (const row of rows) {
    const payload = typeof row.data === 'string' ? row.data : JSON.stringify(row.data);
    logger.info({ id: row.id?.toString('hex') || '', payload }, '[eventWorker] Event');
  }

  const ids = rows.map(r => r.id);
  const placeholders = ids.map(() => '?').join(',');
  db.execute(`delete from event where id in (${placeholders})`, ids);
}

async function run() {
  while (true) {
    try {
      await fetchAndLogBatch();
    } catch (err) {
      logger.error({ err }, '[eventWorker] Error');
      // In case of errors, avoid tight loop
      await new Promise(r => setTimeout(r, 500));
    }
  }
}

run();

// Listen for shutdown signal from parent
if (parentPort) {
  parentPort.on('message', (msg) => {
    if (msg === 'shutdown') {
      process.exit(0);
    }
  });
}
