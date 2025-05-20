// Copyright 2024 Roy T. Hashimoto. All Rights Reserved.

import * as SQLite from '../src/sqlite-api.js';
import SQLiteESMFactory from '../dist/wa-sqlite.mjs';
import { SyncMemoryProxyAsyncWorkerVFS } from '../src/examples/SyncMemoryProxyAsyncWorkerVFS.js';

const searchParams = new URLSearchParams(location.search);

// SQLite instance and database connection
/** @type {SQLiteAPI} */ let sqlite3;
let db;
/** @type {SyncMemoryProxyAsyncWorkerVFS} */ let vfsInstance;
/** @type {Worker} */ let worker;
let parsedQueries = [];
let isTerminated = false;
let testIterationCount = 0;
const maxIterationCount = 5000;
let iterationQueriesRun = 0;
let totalQueriesRun = 0;


const output = document.getElementById('output');

// Initialize SQLite with the selected VFS
async function initSQLite() {
  try {
    let dbName = searchParams.get('dbName') ?? 'hello';

    // Instantiate SQLite
    const start = performance.now();
    const module = await SQLiteESMFactory();
    sqlite3 = SQLite.Factory(module);

    worker = new Worker(new URL('../src/examples/EncryptedOPFSWorker.js', import.meta.url), { type: 'module' });
    vfsInstance = await SyncMemoryProxyAsyncWorkerVFS.create("demo", module, {
      dbName,
      syncLatencyMsec: 25,
      encryptionPassword: searchParams.get('password') || 'abcd123',
      worker: worker,
    })
    sqlite3.vfs_register(vfsInstance, true);

    // Open the database
    db = sqlite3.sync_open(dbName);
    const end = performance.now();
    console.log(`SQLite opened ${dbName} in ${(end - start).toFixed(2)} ms`);

    sqlite3.sync_exec(db, 'PRAGMA cache_size=-64000');
    sqlite3.sync_exec(db, 'PRAGMA journal_mode=MEMORY');
    sqlite3.sync_exec(db, 'PRAGMA page_size=4096');
    sqlite3.sync_exec(db, 'PRAGMA legacy_alter_table=ON');

    // Return success
    document.getElementById('output').innerHTML =
      JSON.stringify([...new URLSearchParams(location.search).entries()]);
    return true;
  } catch (e) {
    console.error(e);
    document.getElementById('output').innerHTML = `<pre>${cvtErrorToCloneable(e).stack}</pre>`;
    return false;
  }
}

async function runSoakTest() {
  isTerminated = false;
  totalQueriesRun += iterationQueriesRun;
  iterationQueriesRun = 0;

  if (testIterationCount > maxIterationCount) {
    output.innerHTML = `Soak test successfully completed after ${maxIterationCount} iterations, ${totalQueriesRun} queries.`;
    return;
  }
  testIterationCount++;
  output.innerHTML = `Running soak test iteration ${testIterationCount}...`;
  output.innerHTML += `${totalQueriesRun} queries have run so far.`;

  // blow everything up after a random delay
  const randomDelay = 200 + Math.floor(Math.random() * 5000);
  setTimeout(async () => {
    output.innerHTML = `Soak test ${testIterationCount} interrupted after ${randomDelay}ms (${iterationQueriesRun} queries), terminating...`;
    interruptTest();

    const passed = await checkIntegrity();
    if (passed) {
      output.innerHTML += `<br/>Integrity check passed.`;

      // destroy and recreate the database
      destroyDatabase();
      await initSQLite()

      setTimeout(() => {
        runSoakTest();
      }, 100);
    }
    else {
      output.innerHTML += `<br/>Integrity check failed, not continuing.`;
    }
  }, randomDelay);

  runSampleQueries(parsedQueries);
}

function interruptTest() {
  isTerminated = true;
  if (worker) {
    worker.terminate();
    worker = null;
  }
  if (vfsInstance) {
    vfsInstance.shutdown();
    vfsInstance = null;
  }
  if (db) {
    try {
      sqlite3.sync_close(db);
    } catch (e) {
      console.error('Error closing database', e);
    }
    db = null;
  }
  sqlite3 = null;
}

async function checkIntegrity() {
  // now reload it all, and confirm there is something in the database
  output.innerHTML += '<br/>Reloading SQLite...';
  await initSQLite()

  output.innerHTML += '<br/>Reloaded, checking integrity.';
  const tableSizes = {};
  const tableResult = executeSingleQuery(`SELECT name FROM sqlite_master WHERE type='table'`)
  for (const row of tableResult.rows) {
    const tableName = row[0];
    const rowCount = executeSingleQuery(`SELECT COUNT(*) FROM ${tableName}`);
    tableSizes[tableName] = rowCount.rows[0][0];
  }
  output.innerHTML += `<br/>Tables are:<pre>${JSON.stringify(tableSizes, null, 2)}</pre>`;

  const integrityCheck = executeSingleQuery(`PRAGMA integrity_check`);
  const integrityCheckPassed = integrityCheck.rows.length === 1 && integrityCheck.rows[0][0] === 'ok';
  if (integrityCheckPassed) {
    output.innerHTML += `<br/>Integrity check: OK`;
  } else {
    output.innerHTML += `<br/>Integrity check: <pre>${JSON.stringify(integrityCheck.rows, null, 2)}</pre>`;
    return false;
  }

  return tableResult.rows.length > 0 && integrityCheckPassed;
}

async function destroyDatabase() {
  vfsInstance.destroyDatabase();
  interruptTest();
}

function executeSingleQuery(sql, queryArguments) {
  try {
    iterationQueriesRun++;
    const stmt = sqlite3.sync_prepare(db, sql);
  
    // Bind parameters if provided
    if (queryArguments && queryArguments.length > 0) {
        sqlite3.bind_collection(stmt, queryArguments);
    }

    let columnNames;
    const rows = [];
    while (sqlite3.sync_step(stmt) === SQLite.SQLITE_ROW) {
        const rowData = sqlite3.row(stmt);
        columnNames = columnNames ?? sqlite3.column_names(stmt);
        rows.push(rowData);
    }

    const rowsModified = sqlite3.changes(db);

    return {
      rows,
      columnNames,
      rowsModified,
    }
  } catch (e) {
    console.error(`Error with SQL statement ${sql}`, e);
    if (isTerminated) {
      // ignore this, we know we're doing a hard shutdown
      return;
    }
    throw e;
  }
}

/**
 * Run a series of sample queries from a JSON file
 * @param {Array} sampleQueries - Array of query objects with query and optional params
 */
async function runSampleQueries(sampleQueries) {
  const timestamp = document.getElementById('timestamp');
  timestamp.textContent = new Date().toLocaleTimeString();
  const timing = [
    {checkpoint: "start", start: performance.now(), sleepTime: 0},
  ];
  let sleepStart = null;
  let sleepTime = 0;
  let totalSleep = 0;
  const sleepEvery = 500;
  const sleepDuration = 30;

  for (let i = 0; i < sampleQueries.length; i++) {
    if (isTerminated) {
      return;
    }
    if (sampleQueries[i]["checkpoint"]) {
      sleepStart = performance.now();
      await new Promise(resolve => setTimeout(resolve, sleepDuration * 3));
      sleepTime += (performance.now() - sleepStart);
      
      timing[timing.length - 1]["sleepTime"] = sleepTime;
      totalSleep += sleepTime;
      sleepTime = 0;
      timing.push({
        checkpoint: sampleQueries[i]["checkpoint"], 
        start: performance.now(), 
        sleepTime: 0
      });
    }

    const sql = sampleQueries[i]["query"];
    if (sql) {
      const params = sampleQueries[i]["params"];
      executeSingleQuery(sql, params);
    }

    // sleep regularly, to permit background tasks to run
    if (i % sleepEvery === 0) {
      sleepStart = performance.now();
      await new Promise(resolve => setTimeout(resolve, sleepDuration));
      sleepTime += (performance.now() - sleepStart);
    }
  }

  timing[timing.length - 1]["sleepTime"] = sleepTime;
  totalSleep += sleepTime;
  timing.push({
    checkpoint: "end", 
    start: performance.now(), 
    sleepTime: 0
  });

  const periods = {};
  for (let i = 0; i < timing.length - 1; i++) {
    const name = timing[i]["checkpoint"];
    const start = timing[i]["start"];
    const end = timing[i + 1]["start"];
    const duration = end - start - timing[i]["sleepTime"];
    periods[name] = `${duration.toFixed(1)}ms`;
  }
  const totalDuration = timing[timing.length - 1]["start"] - timing[0]["start"];

  timestamp.textContent = `${(totalDuration - totalSleep).toFixed(1)} msec (${totalDuration.toFixed(1)} total, including ${totalSleep.toFixed(1)} msec sleep), periods: ${JSON.stringify(periods, null, 2)}\nStatements prepared: ${statementsPrepared}, statements reused: ${statementsReused}`;
}

async function init() {
  const executeTestButton = /** @type {HTMLButtonElement} */(document.getElementById('execute-soak-test'));
  const fileInput = /** @type {HTMLInputElement} */(document.getElementById('sql-file'));
  const fileInfo = document.getElementById('sql-file-info');
  
  // Initialize SQLite
  await maybeReset(searchParams);
  await initSQLite();
  executeTestButton.disabled = true;

  // Handle file selection
  fileInput.addEventListener('change', async function() {
    if (fileInput.files && fileInput.files.length > 0) {
      const file = fileInput.files[0];
      fileInfo.textContent = `Selected: ${file.name} (${formatFileSize(file.size)})`;

      try {
        const fileContent = await file.text();
        parsedQueries = JSON.parse(fileContent);
        if (parsedQueries.length > 0) {
          fileInfo.textContent = `${fileInfo.textContent}, ${parsedQueries.length} queries`;
          executeTestButton.disabled = false;
        }
      } catch (e) {
        output.innerHTML = `<pre>Error parsing JSON: ${e.message}</pre>`;
      }

    } else {
      fileInfo.textContent = '';
    }
  });

  // Start soak test file on button click
  executeTestButton.addEventListener('click', async function() {
    executeTestButton.disabled = true;

    output.innerHTML = 'Running soak test...';
    runSoakTest();
    
    executeTestButton.disabled = false;
  });
}

// Helper function to format file size
function formatFileSize(bytes) {
  if (bytes < 1024) {
    return bytes + ' bytes';
  } else if (bytes < 1024 * 1024) {
    return (bytes / 1024).toFixed(1) + ' KB';
  } else {
    return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
  }
}

async function maybeReset(searchParams) {
  if (searchParams.has('reset')) {
    console.log('clearing OPFS and IndexedDB');

    const root = await navigator.storage?.getDirectory();
    if (root) {
      // @ts-ignore
      for await (const name of root.keys()) {
        await root.removeEntry(name, { recursive: true });
      }
    }

    // Clear IndexedDB
    const dbList = indexedDB.databases ?
      await indexedDB.databases() :
      ['demo', 'demo-floor'].map(name => ({ name }));
    await Promise.all(dbList.map(({name}) => {
      console.log('deleting IndexedDB database', name);
      return new Promise((resolve, reject) => {
        const request = indexedDB.deleteDatabase(name);
        request.onsuccess = resolve;
        request.onerror = reject;
      });
    }));
  }
}

function cvtErrorToCloneable(e) {
  if (e instanceof Error) {
    const props = new Set([
      ...['name', 'message', 'stack'].filter(k => e[k] !== undefined),
      ...Object.getOwnPropertyNames(e)
    ]);
    return Object.fromEntries(Array.from(props, k => [k, e[k]])
      .filter(([_, v]) => {
        // Skip any non-cloneable properties
        try {
          structuredClone(v);
          return true;
        } catch (e) {
          return false;
        }
      }));
  }
  return e;
}

if (document.readyState !== 'loading') {
  init();
} else {
  document.addEventListener('DOMContentLoaded', function () {
      init();
  });
}
