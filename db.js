import Database from 'better-sqlite3';
import * as fs from 'fs'

fs.mkdirSync('./data/db', { recursive: true })

class Db {
  constructor() {
    this.sqliteDb = new Database('./data/db/wapplibre.db', {});
    this.sqliteDb.pragma('journal_mode = WAL');
  }

  execute(sql, params = []) {
    const stmt = this.sqliteDb.prepare(sql);
    return stmt.run(...params);
  }

  query(sql, params = []) {
    const stmt = this.sqliteDb.prepare(sql);
    return stmt.all(...params);
  }

  queryOne(sql, params = []) {
    const stmt = this.sqliteDb.prepare(sql);
    return stmt.get(...params);
  }
}

export const db = new Db();
export default db;
