import express from 'express';
import bodyParser from "body-parser";
import { JSONRPCServer } from "json-rpc-2.0";
import { parse as uuidParse } from 'uuid';
import { readdir, readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import db from './db.js';

const server = new JSONRPCServer();

server.addMethod('saveEntitySchema', ({ name, data }) => {
  db.execute('insert into entity_schema (name, data) values (?, ?)', [name, JSON.stringify(data)]);
});
server.addMethod('getEntitySchema', ({ name }) => {
  return JSON.parse(db.queryOne('select data from entity_schema where name = ?', [name])?.data);
});
server.addMethod('saveEntity', ({ id, data }) => {
  db.execute('insert into entity (id, data) values (?, ?)', [uuidParse(id), JSON.stringify(data)]);
});
server.addMethod('getEntity', ({ id }) => {
  return db.queryOne('select data from entity where id = ?', [uuidParse(id)]);
});

db.execute('create table if not exists entity (id blob primary key, data jsonb)');
db.execute('create table if not exists entity_rel (from_entity blob, to_entity blob, name text, data jsonb, primary key(from_entity, to_entity, name))');
db.execute('create table if not exists entity_schema (name text primary key, data jsonb)');

// Resolve __dirname for ESM
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Reads all JSON files under data/schemas and returns a list of parsed objects
async function readAllSchemas() {
  const schemasDir = path.join(__dirname, 'data', 'schemas');
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
      console.warn(`Skipping schema "${path.basename(filePath)}": ${err.message}`);
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
const app = express()

// Allow CORS from all origins and handle preflight
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', 'http://localhost:5173');
  res.setHeader('Access-Control-Allow-Methods', '*');
  res.setHeader('Access-Control-Allow-Headers', 'content-type, authorization');
  res.setHeader('Access-Control-Allow-Credentials', 'true');
  if (req.method === 'OPTIONS') {
    return res.sendStatus(204);
  }
  next();
});

app.use(bodyParser.json());

app.post("/api", (req, res) => {
  const jsonRPCRequest = req.body;
  server.receive(jsonRPCRequest).then((jsonRPCResponse) => {
    if (jsonRPCResponse) {
      res.json(jsonRPCResponse);
    } else {
      res.sendStatus(204);
    }
  });
});

app.get('/', (req, res) => {
  res.send('wapplibre');
})

app.listen(port, () => {
    console.log(`Listening on port ${port}`)
})
