import * as duckdb from "@duckdb/duckdb-wasm";
import duckdb_wasm from "@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url";
import mvp_worker from "@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url";

const BUNDLES = {
  mvp: {
    mainModule: duckdb_wasm,
    mainWorker: mvp_worker,
  },
};

let dbPromise = null;

async function initDb() {
  const bundle = BUNDLES.mvp;
  const worker = new Worker(bundle.mainWorker);
  const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING);
  const db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule);
  return db;
}

export async function getDb() {
  if (!dbPromise) {
    dbPromise = initDb();
  }
  return dbPromise;
}

/** Run an arbitrary SQL query against already-registered tables/files. */
export async function query(sql) {
  const db = await getDb();
  const conn = await db.connect();
  try {
    const result = await conn.query(sql);
    return result.toArray().map((row) => row.toJSON());
  } finally {
    await conn.close();
  }
}

export async function registerParquet(url, virtualName) {
  const db = await getDb();
  const buf = await (await fetch(url)).arrayBuffer();
  await db.registerFileBuffer(virtualName, new Uint8Array(buf));
}
