import { parentPort } from 'node:worker_threads';
import db from '../db.js';

const BATCH_SIZE = 50;
const POLL_INTERVAL_MS = 0; // rely on blocking via transaction when no rows

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchAndLogBatch() {
  const rows = db.query(
    'select id, data from event limit ?',
    [BATCH_SIZE]
  );

  if (!rows || rows.length === 0) {
    await sleep(1000);
    return;
  }

  for (const row of rows) {
    const payload = typeof row.data === 'string' ? row.data : JSON.stringify(row.data);
    console.log(`[eventWorker] Event id=${row.id?.toString('hex') || ''} data=${payload}`);
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
      console.error('[eventWorker] Error:', err);
      await sleep(1000);
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
