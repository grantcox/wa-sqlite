// Copyright 2024 Roy T. Hashimoto. All Rights Reserved.

import * as SQLite from '../src/sqlite-api.js';
import SQLiteESMFactory from '../dist/wa-sqlite.mjs';
import { SyncMemoryProxyAsyncWorkerVFS } from '../src/examples/SyncMemoryProxyAsyncWorkerVFS.js';

// Get parameters from parent window
const params = new URLSearchParams(window.location.search);
const dbName = params.get('dbName') || 'hello';
const encryptionPassword = params.get('password') || 'abcd123';
const testIterationCount = parseInt(params.get('iteration') || '1', 10);

// Queries will be received via postMessage
let parsedQueries = [];
let queriesReceived = false;

// SQLite instance and database connection
/** @type {SQLiteAPI} */ let sqlite3;
let db;
/** @type {SyncMemoryProxyAsyncWorkerVFS} */ let vfsInstance;
/** @type {Worker} */ let worker;
let isTerminated = false;
let iterationQueriesRun = 0;

const output = document.getElementById('output');

// Initialize SQLite with the selected VFS
async function initSQLite() {
  try {
    // Instantiate SQLite
    const start = performance.now();
    const module = await SQLiteESMFactory();
    sqlite3 = SQLite.Factory(module);

    worker = new Worker(new URL('../src/examples/EncryptedOPFSWorker.js', import.meta.url), { type: 'module' });
    vfsInstance = await SyncMemoryProxyAsyncWorkerVFS.create("demo", module, {
      dbName,
      syncLatencyMsec: 25,
      encryptionPassword,
      worker: worker,
    })
    sqlite3.vfs_register(vfsInstance, true);

    // Open the database
    db = sqlite3.sync_open(dbName);
    const end = performance.now();
    console.log(`SQLite opened ${dbName} in ${(end - start).toFixed(2)} ms`);

    // Configure SQLite settings
    sqlite3.sync_exec(db, 'PRAGMA cache_size=-64000');
    sqlite3.sync_exec(db, 'PRAGMA journal_mode=MEMORY');
    sqlite3.sync_exec(db, 'PRAGMA page_size=4096');
    sqlite3.sync_exec(db, 'PRAGMA legacy_alter_table=ON');

    output.innerHTML = `Initialized SQLite for iteration ${testIterationCount}`;
    return true;
  } catch (e) {
    console.error(e);
    output.innerHTML = `<pre>${cvtErrorToCloneable(e).stack}</pre>`;
    return false;
  }
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
  await initSQLite();

  output.innerHTML += '<br/>Reloaded, checking integrity.';
  const tableSizes = {};
  const tableResult = executeSingleQuery(`SELECT name FROM sqlite_master WHERE type='table'`);
  for (const row of tableResult.rows) {
    const tableName = row[0];
    const rowCount = executeSingleQuery(`SELECT COUNT(*) FROM ${tableName}`);
    tableSizes[tableName] = rowCount.rows[0][0];
  }

  const integrityCheck = executeSingleQuery(`PRAGMA integrity_check`);
  const integrityCheckPassed = integrityCheck.rows.length === 1 && integrityCheck.rows[0][0] === 'ok';

  // Send results to parent
  window.parent.postMessage({
    type: 'integrityCheck',
    iteration: testIterationCount,
    passed: integrityCheckPassed,
    tableSizes,
    queriesRun: iterationQueriesRun,
    integrityResult: integrityCheck.rows
  }, '*');

  return integrityCheckPassed;
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
      return { rows: [], columnNames: [], rowsModified: 0 };
    }
    throw e;
  }
}

async function destroyDatabase() {
  if (vfsInstance) {
    vfsInstance.destroyDatabase();
  }
  interruptTest();
}

/**
 * Run a series of sample queries from a JSON file
 * @param {Array} sampleQueries - Array of query objects with query and optional params
 */
async function runSampleQueries(sampleQueries) {
  if (!sampleQueries || sampleQueries.length === 0) {
    return;
  }

  let sleepStart = null;
  let sleepTime = 0;
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
}

// Function to load queries directly from a file
async function loadQueriesFromFile(fileName) {
  try {
    // Fetch the file 
    const response = await fetch(fileName);
    if (!response.ok) {
      throw new Error(`Failed to load file: ${response.status} ${response.statusText}`);
    }
    
    // Parse the JSON
    const fileContent = await response.text();
    parsedQueries = JSON.parse(fileContent);
    console.log(`Loaded ${parsedQueries.length} queries directly from file ${fileName}`);
    
    return true;
  } catch (error) {
    console.error(`Error loading queries from file: ${error.message}`);
    output.innerHTML += `<br>Error loading queries from file: ${error.message}`;
    return false;
  }
}

// Set up message handler to receive queries from the parent
function setupMessageHandler() {
  window.addEventListener('message', async function(event) {
    // Handle direct query data
    if (event.data.type === 'queries') {
      console.log(`Received ${event.data.queries.length} queries from parent`);
      parsedQueries = event.data.queries;
      queriesReceived = true;
      
      // Now that we have the queries, we can start the actual test
      await startTest();
    } 
    // Handle file reference method
    else if (event.data.type === 'queryFileReference') {
      console.log(`Received file reference: ${event.data.fileName}`);
      output.innerHTML += `<br>Loading queries from file: ${event.data.fileName}...`;
      
      // Load the queries from the file
      const success = await loadQueriesFromFile(event.data.fileName);
      
      if (success) {
        queriesReceived = true;
        await startTest();
      } else {
        // Report failure to parent
        window.parent.postMessage({
          type: 'iterationComplete',
          iteration: testIterationCount,
          queriesRun: 0,
          passed: false,
          error: 'Failed to load queries from file'
        }, '*');
      }
    }
  });
  
  // Notify parent we're ready to receive queries
  window.parent.postMessage({
    type: 'iframeReady',
    iteration: testIterationCount
  }, '*');
}

// Run a single iteration of the soak test
async function runTest() {
  isTerminated = false;
  iterationQueriesRun = 0;
  
  output.innerHTML = `Initializing soak test iteration ${testIterationCount}...`;
  
  // Initialize SQLite
  await initSQLite();
  
  // Set up message handler to receive queries from parent
  setupMessageHandler();
}

// Start the actual test after receiving queries
async function startTest() {
  if (!queriesReceived) {
    console.error("Cannot start test without queries");
    return;
  }
  
  output.innerHTML += `<br>Received ${parsedQueries.length} queries, starting test execution...`;
  
  // blow everything up after a random delay
  const randomDelay = 10 + Math.floor(Math.random() * 2000);
  
  // Start running queries
  const queryPromise = runSampleQueries(parsedQueries);
  
  // Set timeout to interrupt
  setTimeout(async () => {
    output.innerHTML = `Soak test ${testIterationCount} interrupted after ${randomDelay}ms (${iterationQueriesRun} queries), terminating...`;
    interruptTest();

    const passed = await checkIntegrity();
    if (passed) {
      output.innerHTML += `<br/>Integrity check passed.`;
      
      // Destroy database for cleanup
      destroyDatabase();
      
      // Report completion to parent window
      window.parent.postMessage({
        type: 'iterationComplete',
        iteration: testIterationCount,
        queriesRun: iterationQueriesRun,
        passed: true
      }, '*');
    } else {
      output.innerHTML += `<br/>Integrity check failed, not continuing.`;
      
      // Report failure to parent window
      window.parent.postMessage({
        type: 'iterationComplete',
        iteration: testIterationCount,
        queriesRun: iterationQueriesRun,
        passed: false
      }, '*');
    }
  }, randomDelay);
  
  // Wait for queries to finish (will be interrupted)
  await queryPromise;
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

// Start the test when the page loads
window.addEventListener('DOMContentLoaded', runTest);
