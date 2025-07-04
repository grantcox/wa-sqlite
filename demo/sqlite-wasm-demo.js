// Copyright 2024 Roy T. Hashimoto. All Rights Reserved.
import sqlite3InitModule from '@sqlite.org/sqlite-wasm';
import { initMemoryVfs } from '../src/examples/SqliteWasmMemoryVFS.js';
import { registerVfs } from '../src/examples/SqliteWasmMemoryToWorkerVFS.js';
import { StatementCache } from './statement-cache.js';

// This is the path to the Monaco editor distribution. For development
// this loads from the local server (uses Yarn 2 path).
const MONACO_VS = location.hostname.endsWith('localhost') ?
  '/.yarn/unplugged/monaco-editor-npm-0.34.1-03d887d213/node_modules/monaco-editor/dev/vs' :
  'https://cdnjs.cloudflare.com/ajax/libs/monaco-editor/0.34.1/min/vs';

const SQL_KEY = 'wa-sqlite demo sql';
const DEFAULT_SQL = `
-- Optionally select statements to execute.

CREATE TABLE IF NOT EXISTS t(x PRIMARY KEY, y);
INSERT OR REPLACE INTO t VALUES ('good', 'bad'), ('hot', 'cold'), ('up', 'down');
SELECT * FROM t;
`.trim();

const searchParams = new URLSearchParams(location.search);

/**
 * @typedef Config
 * @property {string} name
 * @property {string} vfsModule path of the VFS module
 * @property {string} [vfsClassName] name of the VFS class
 * @property {string} [vfsName] name of the VFS instance
 * @property {object} [vfsOptions] VFS constructor arguments
 * @property {object} [hookOptions] Commit Hook options
 */

/** @type {Map<string, Config>} */ const VFS_CONFIGS = new Map([
  {
    name: 'default',
    vfsModule: null
  },
  {
    name: 'MemoryVFS',
    vfsModule: '../src/examples/MemoryVFS.js',
  },
  {
    name: 'SMPAWOPFSVFS',
    vfsModule: '../src/examples/SyncMemoryProxyAsyncWorkerVFS.js',
    vfsOptions: { 
      encryptionPassword: searchParams.get('password') || 'abcd123',
      worker: () => {
        return new Worker(new URL('../src/examples/EncryptedOPFSWorker.js', import.meta.url), { type: 'module' });
      }
    }
  },
].map(config => [config.name, config]));


const log = console.log;
const error = console.error;

// SQLite instance and database connection
/** @type {Sqlite3Static} */ let sqlite3;
/** @type {Database} */ let db;
/** @type {any} */ let vfsInstance;

// Initialize SQLite with the selected VFS
async function initSQLite() {
  try {
    await maybeReset(searchParams);
    
    const configName = searchParams.get('config') || VFS_CONFIGS.keys().next().value;
    const config = VFS_CONFIGS.get(configName);

    let dbName = searchParams.get('dbName') ?? 'hello';

    // Instantiate SQLite
    const start = performance.now();
    log('Loading and initializing SQLite3 module...');

    sqlite3 = await sqlite3InitModule({
      print: log,
      printErr: error,
    });

    // const memoryVfs = initMemoryVfs(sqlite3);
    // db = new sqlite3.oo1.DB({
    //   filename: dbName,
    //   vfs: memoryVfs.name
    // });

    const worker = new Worker(new URL('../src/examples/EncryptedOPFSWorker.js', import.meta.url), { type: 'module' });

    // const vfsName = 'memory-worker';
    // vfsInstance = new SqliteWasmMemoryToWorkerVFS(vfsName, sqlite3, {
    //   dbName,
    //   encryptionPassword: searchParams.get('password') || 'abcd123',
    //   worker: worker,
    //   syncLatencyMsec: 25
    // });
    // registerVfs(sqlite3, vfsInstance);
    // db = new sqlite3.oo1.DB({
    //   filename: dbName,
    //   vfs: vfsName
    // });

    const vfsName = 'memory-worker';
    const vfsController = registerVfs(sqlite3, {
      name: vfsName,
      dbName: dbName,
      worker: worker,
      syncLatencyMsec: 25,
      encryptionPassword: searchParams.get('password') || 'abcd123',
    });
    await vfsController.isReady();

    db = new sqlite3.oo1.DB({
      filename: dbName,
      vfs: vfsName
    });

    const end = performance.now();
    console.log(`SQLite opened ${dbName} in ${(end - start).toFixed(2)} ms`);


    db.exec('PRAGMA cache_size=-64000');
    db.exec('PRAGMA journal_mode=MEMORY');
    db.exec('PRAGMA page_size=4096');

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

// Execute SQL queries
function executeSQL(query) {
  const start = performance.now();
  const statements = query.split(';').map(s => s.trim()).filter(s => s.length > 0);

  const results = [];
  statements.forEach(sql => {
    const result = executeSingleQuery(sql);
    if (result.columns.length > 0) {
      results.push(result);
    }
  });

  const end = performance.now();
  const elapsed = Math.trunc(end - start) / 1000;
  
  return { results, elapsed };
}

const statementCache = new StatementCache(100, stmt => {
  // release the statement handle
  stmt.finalize();
});

let statementsPrepared = 0;
let statementsReused = 0;

function executeSingleQuery(sql, queryArguments) {
  // Check if the statement is already cached
  let stmt = statementCache.get(sql);
  if (stmt) {
    // Reuse the cached statement
    stmt.reset(true);
    statementsReused++;
  } else {
    stmt = db.prepare(sql);
    statementCache.set(sql, stmt);
    statementsPrepared++;
  }

  // Bind parameters if provided
  if (queryArguments && queryArguments.length > 0) {
    stmt.bind(queryArguments);
  }

  let columns = [];
  if (stmt.columnCount > 0) {
    columns = stmt.getColumnNames();
  }

  const rows = [];
  while (stmt.step()) {
    const row = stmt.get([]);
    rows.push(row);

    // get rows as objects
    // const rowData = stmt.get({})
    // rows.push(rowData);
  }
  const rowsModified = db.changes(true);
  return { columns, rows, rowsModified }
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
  // Load the Monaco editor
  const executeButton = /** @type {HTMLButtonElement} */(document.getElementById('execute'));
  const executeFileButton = /** @type {HTMLButtonElement} */(document.getElementById('execute-file'));
  const fileInput = /** @type {HTMLInputElement} */(document.getElementById('sql-file'));
  const fileInfo = document.getElementById('sql-file-info');
  
  const editorReady = createMonacoEditor().then(editor => {
    // Change the button text with selection
    editor.onDidChangeCursorSelection(({selection}) => {
      executeButton.textContent = selection.isEmpty() ?
        'Execute' :
        'Execute selection';
    });

    // Persist editor content across page loads
    let change;
    editor.onDidChangeModelContent(function() {
      clearTimeout(change);
      change = setTimeout(function() {
        localStorage.setItem(SQL_KEY, editor.getValue());
      }, 1000);
    });
    editor.setValue(localStorage.getItem(SQL_KEY) ?? DEFAULT_SQL);

    return editor;
  });

  // Initialize SQLite
  const sqliteReady = initSQLite();

  const destroyDbButton = /** @type {HTMLButtonElement} */(document.getElementById('destroy-db'));

  // Wait for both editor and SQLite to be ready
  const [editor, sqliteInitialized] = await Promise.all([editorReady, sqliteReady]);

  if (sqliteInitialized) {
    executeButton.disabled = false;
    executeFileButton.disabled = false;
    destroyDbButton.disabled = false;
  }

  // Handle file selection
  fileInput.addEventListener('change', function() {
    if (fileInput.files && fileInput.files.length > 0) {
      const file = fileInput.files[0];
      fileInfo.textContent = `Selected: ${file.name} (${formatFileSize(file.size)})`;
    } else {
      fileInfo.textContent = '';
    }
  });

  // Execute SQL file on button click
  executeFileButton.addEventListener('click', async function() {
    if (!fileInput.files || fileInput.files.length === 0) {
      alert('Please select a file first');
      return;
    }

    executeButton.disabled = true;
    executeFileButton.disabled = true;

    // Read the file
    const file = fileInput.files[0];
    const fileContent = await file.text();
    
    // Clear any previous output on the page
    const output = document.getElementById('output');
    while (output.firstChild) output.removeChild(output.lastChild);

    const timestamp = document.getElementById('timestamp');
    timestamp.textContent = `${new Date().toLocaleTimeString()} - Processing ${file.name}`;

    // Check file extension to determine how to process
    const fileExtension = file.name.split('.').pop().toLowerCase();
    
    if (fileExtension === 'json') {
      // Process JSON file as sample queries
      let sampleQueries;
      try {
        sampleQueries = JSON.parse(fileContent);
      } catch (e) {
        output.innerHTML = `<pre>Error parsing JSON: ${e.message}</pre>`;
        return;
      }
      await runSampleQueries(sampleQueries);
    }

    executeButton.disabled = false;
    executeFileButton.disabled = false;
    destroyDbButton.disabled = false;
  });

  // Add event listener for the Destroy DB button
  destroyDbButton.addEventListener('click', async function() {
    if (!vfsInstance || typeof vfsInstance.destroyDatabase !== 'function') {
      alert('Database destruction not supported with the current VFS configuration');
      return;
    }

    if (confirm('Are you sure you want to destroy the database? This action cannot be undone.')) {
      try {
        destroyDbButton.disabled = true;
        executeButton.disabled = true;
        executeFileButton.disabled = true;

        await vfsInstance.destroyDatabase();

        const timestamp = document.getElementById('timestamp');
        timestamp.textContent = `${new Date().toLocaleTimeString()} - Database destroyed successfully`;
      } catch (error) {
        console.error('Error destroying database:', error);
        alert(`Failed to destroy database: ${error.message}`);
      } finally {
        destroyDbButton.disabled = false;
        executeButton.disabled = false;
        executeFileButton.disabled = false;
      }
    }
  });

  // Execute SQL on button click
  executeButton.addEventListener('click', async function() {
    executeButton.disabled = true;
    executeFileButton.disabled = true;
    destroyDbButton.disabled = true;

    // Get SQL from editor
    const selection = editor.getSelection();
    const queries = selection.isEmpty() ?
      editor.getValue() :
      editor.getModel().getValueInRange(selection);

    // Clear any previous output on the page
    const output = document.getElementById('output');
    while (output.firstChild) output.removeChild(output.lastChild);

    const timestamp = document.getElementById('timestamp');
    timestamp.textContent = new Date().toLocaleTimeString();

    // Execute the SQL and process results
    const time = performance.now();
    const response = await executeSQL(queries);
    timestamp.textContent += ` ${(performance.now() - time).toFixed(1)} milliseconds`;
    
    if (response.results) {
      // Format the results as tables
      response.results
        .map(formatTable)
        .forEach(table => output.append(table));        
    } else {
      output.innerHTML = `<pre>${response.error.message}</pre>`;
    }

    executeButton.disabled = false;
    executeFileButton.disabled = false;
    destroyDbButton.disabled = false;
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

async function createMonacoEditor() {
  // Insert a script element to bootstrap the monaco loader
  await new Promise(resolve => {
    const loader = document.createElement('script');
    loader.src = `${MONACO_VS}/loader.js`;
    loader.async = true;
    loader.addEventListener('load', resolve, { once: true });
    document.head.appendChild(loader);
  });

  // Load monaco itself
  /** @type {any} */ const require = globalThis.require;
  require.config({ paths: { vs: MONACO_VS } });
  const monaco = await new Promise(resolve => {
    require(['vs/editor/editor.main'], resolve);
  });

  // Create editor
  // https://microsoft.github.io/monaco-editor/api/modules/monaco.editor.html#create
  return monaco.editor.create(document.getElementById('editor-container'), {
    language: 'sql',
    minimap: { enabled: false },
    automaticLayout: true
  });
}

function formatTable({ columns, rows }) {
  const table = document.createElement('table');

  const thead = table.appendChild(document.createElement('thead'));
  thead.appendChild(formatRow(columns, 'th'));

  const tbody = table.appendChild(document.createElement('tbody'));
  for (const row of rows) {
    tbody.appendChild(formatRow(row));
  }

  return table;
}

function formatRow(data, tag = 'td') {
  const row = document.createElement('tr');
  for (const value of data) {
    const cell = row.appendChild(document.createElement(tag));
    cell.textContent = value !== null ? value.toString() : 'null';
  }
  return row;
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
