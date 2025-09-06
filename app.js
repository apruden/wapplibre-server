import { JSONRPCServer } from "json-rpc-2.0";
import { parse as uuidParse } from 'uuid';
import { readdir, readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import db from './db.js';
import {addDocument, searchDocuments} from './search.js';
import { Worker } from 'node:worker_threads';
import logger from './logger.js';
import Fastify from 'fastify';
import _ from 'lodash';

const server = new JSONRPCServer();

server.addMethod('saveEntitySchema', ({ name, data }) => {
  db.execute('insert into entity_schema (name, data) values (?, ?)', [name, JSON.stringify(data)]);
});

server.addMethod('getEntitySchema', ({ name }) => {
  
  function mapValuesRecursive(obj, iteratee) {
    return _.mapValues(obj, (value, key) => {
      if (_.isPlainObject(value)) {
        return mapValuesRecursive(value, iteratee); // Recursive call for nested objects
      }
      return iteratee(value, key); // Apply iteratee to non-object values
    });
  }

  function resolveSchema(n, definitions) {
    const schema = JSON.parse(db.queryOne('select data from entity_schema where name = ?', [n])?.data);
    return mapValuesRecursive(schema.model, (prop, key) => {
      if (key === '$ref') {
        const refName = prop.split('.json#')[0];
        if (refName && refName !== n) {
          definitions[refName] = resolveSchema(refName, definitions); // Recursive resolution for referenced schemaj
          return "#/definitions/" + refName; // Return a placeholder for definitions
        }
      }
      return prop;
    });
  }

  const schema = JSON.parse(db.queryOne('select data from entity_schema where name = ?', [name])?.data);
  const definitions = {};
  schema.model = resolveSchema(name, definitions);
  schema.model.definitions = definitions;

  return schema;
});

server.addMethod('saveEntity', ({ name, id, data }) => {
  logger.debug({ name, id, data }, 'saveEntity');
  db.execute('insert into entity (id, name, data) values (?, ?, ?)', [uuidParse(id), name, JSON.stringify(data)]);
  addDocument(id, data);
});

server.addMethod('getEntity', ({ name, id }) => {
  const result = db.queryOne('select data from entity where name = ? and id = ?', [name, uuidParse(id)]);

  return JSON.parse(result?.data);
});

server.addMethod('getEntities', ({ name, query }) => {
  const ids = searchDocuments(query);

  if (!ids || ids.length === 0) {
    logger.info('No matching documents found');
    return [];
  }

  const idsToFetch = ids.map(id => uuidParse(id));
  const idsCriteria = `id in (${idsToFetch.map(() => '?').join(',')})`;
  const results = db.query(`select data from entity where name = ? and ${idsCriteria}`, [name, ...idsToFetch]);

  return results.map(r => JSON.parse(r.data));
});

db.execute('create table if not exists entity (id blob primary key, name text, data jsonb)');
db.execute('create table if not exists entity_rel (from_entity blob, to_entity blob, name text, data jsonb, primary key(from_entity, to_entity, name))');
db.execute('create table if not exists entity_schema (name text primary key, data jsonb)');

db.execute('create table if not exists workflow (id blob primary key, data jsonb)');
db.execute('create table if not exists event (id blob primary key, data jsonb)');


// Resolve __dirname for ESM
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Reads all JSON files under data/schemas and returns a list of parsed objects
async function readAllSchemas() {
  const schemasDir = path.join(__dirname, 'resources', 'schemas');
  let entries;
  try {
    entries = await readdir(schemasDir, { withFileTypes: true });
  } catch (err) {
    if (err && err.code === 'ENOENT') return [];
    throw err;
  }

  const jsonFiles = entries
    .filter((e) => e.isFile() && e.name.toLowerCase().endsWith('.json'))
    .map((e) => path.join(schemasDir, e.name));

  const results = [];
  for (const filePath of jsonFiles) {
    try {
      const content = await readFile(filePath, 'utf8');
      const fileNameWithoutExt = path.basename(filePath, '.json');
      results.push({name: fileNameWithoutExt, schema: JSON.parse(content)});
    } catch (err) {
      logger.warn({ file: path.basename(filePath), err: err.message }, 'Skipping schema');
    }
  }
  return results;
}

const schemas = await readAllSchemas();
for (const schema of schemas) {
  const jsonSchema = JSON.stringify(schema.schema);
  db.execute('insert into entity_schema (name, data) values (?, ?) on conflict (name) do update set data = ?', [schema.name, jsonSchema, jsonSchema]);
}

const port = 3000
const app = Fastify({ logger: true })

// Start background event worker
const eventWorker = new Worker(new URL('./workers/eventWorker.js', import.meta.url));

// Allow CORS and handle preflight
app.addHook('onRequest', (request, reply, done) => {
  reply.header('Access-Control-Allow-Origin', 'http://localhost:5173');
  reply.header('Access-Control-Allow-Methods', '*');
  reply.header('Access-Control-Allow-Headers', 'content-type, authorization');
  reply.header('Access-Control-Allow-Credentials', 'true');
  if (request.method === 'OPTIONS') {
    reply.code(204).send();
    return;
  }
  done();
});

// Fastify parses JSON bodies by default

app.post('/api', async (request, reply) => {
  const jsonRPCRequest = request.body;
  const jsonRPCResponse = await server.receive(jsonRPCRequest);
  if (jsonRPCResponse) {
    reply.send(jsonRPCResponse);
  } else {
    reply.code(204).send();
  }
});

app.get('/', async (request, reply) => {
  reply.send('wapplibre');
});

const address = await app.listen({ port })
logger.info({ port, address }, 'Listening on port')

// Graceful shutdown
async function shutdown() {
  try { eventWorker.postMessage('shutdown'); } catch {}
  try { await app.close(); } catch {}
  process.exit(0);
}
process.on('SIGINT', () => { shutdown(); });
process.on('SIGTERM', () => { shutdown(); });
