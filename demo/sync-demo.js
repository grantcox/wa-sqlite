// Copyright 2024 Roy T. Hashimoto. All Rights Reserved.

import * as SQLite from '../src/sqlite-api.js';

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

const BUILDS = new Map([
  ['default', '../dist/wa-sqlite.mjs'],
  // ['default', '../debug/wa-sqlite.mjs'],
]);

/**
 * @typedef Config
 * @property {string} name
 * @property {string} vfsModule path of the VFS module
 * @property {string} [vfsClassName] name of the VFS class
 * @property {string} [vfsName] name of the VFS instance
 * @property {object} [vfsOptions] VFS constructor arguments
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
    name: 'MemoryDelayedOPFSVFS',
    vfsModule: '../src/examples/MemoryDelayedOPFSVFS.js',
  },
].map(config => [config.name, config]));

// SQLite instance and database connection
let sqlite3;
let db;

// Initialize SQLite with the selected VFS
async function initSQLite() {
  try {
    const searchParams = new URLSearchParams(location.search);
    await maybeReset(searchParams);
    
    const buildName = searchParams.get('build') || BUILDS.keys().next().value;
    const configName = searchParams.get('config') || VFS_CONFIGS.keys().next().value;
    const config = VFS_CONFIGS.get(configName);

    const dbName = searchParams.get('dbName') ?? 'hello';
    const vfsName = searchParams.get('vfsName') ?? config.vfsName ?? 'demo';

    // Instantiate SQLite
    const { default: moduleFactory } = await import(BUILDS.get(buildName));
    const module = await moduleFactory();
    sqlite3 = SQLite.Factory(module);

    if (config.vfsModule) {
      // Create the VFS and register it as the default file system
      const namespace = await import(config.vfsModule);
      const className = config.vfsClassName ?? config.vfsModule.match(/([^/]+)\.js$/)[1];
      const vfs = await namespace[className].create(vfsName, module, config.vfsOptions);
      sqlite3.vfs_register(vfs, true);
    }

    // Open the database
    db = await sqlite3.open_v2(dbName);

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
async function executeSQL(query) {
  try {
    const start = performance.now();
    const results = [];
    
    for await (const stmt of sqlite3.statements(db, query)) {
      const rows = [];
      while (await sqlite3.step(stmt) === SQLite.SQLITE_ROW) {
        const row = sqlite3.row(stmt);
        rows.push(row);
      }

      const columns = sqlite3.column_names(stmt)
      if (columns.length) {
        results.push({ columns, rows });
      }
    }
    
    const end = performance.now();
    const elapsed = Math.trunc(end - start) / 1000;
    
    return { results, elapsed };
  } catch (e) {
    console.error(e);
    return { error: cvtErrorToCloneable(e) };
  }
}

window.addEventListener('DOMContentLoaded', async function() {
  // Load the Monaco editor
  const button = /** @type {HTMLButtonElement} */(document.getElementById('execute'));
  const editorReady = createMonacoEditor().then(editor => {
    // Change the button text with selection
    editor.onDidChangeCursorSelection(({selection}) => {
      button.textContent = selection.isEmpty() ?
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
  
  // Wait for both editor and SQLite to be ready
  const [editor, sqliteInitialized] = await Promise.all([editorReady, sqliteReady]);
  
  if (sqliteInitialized) {
    button.disabled = false;
  }

  // Execute SQL on button click
  button.addEventListener('click', async function() {
    button.disabled = true;

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
    
    button.disabled = false;
  });
});

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
    const outerLockReleaser = await new Promise(resolve => {
      navigator.locks.request('demo-worker-outer', lock => {
        return new Promise(release => {
          resolve(release);
        });
      });
    });

    await navigator.locks.request('demo-worker-inner', { ifAvailable: true }, async lock => {
      if (lock) {
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
          return new Promise((resolve, reject) => {
            const request = indexedDB.deleteDatabase(name);
            request.onsuccess = resolve;
            request.onerror = reject;
          });
        }));
      } else {
        console.warn('reset skipped because another instance already holds the lock');
      }
    });
    
    await new Promise((resolve, reject) => {
      const mode = searchParams.has('exclusive') ? 'exclusive' : 'shared';
      navigator.locks.request('demo-worker-inner', { mode, ifAvailable: true }, lock => {
        if (lock) {
          resolve();
          return new Promise(() => {});
        } else {
          reject(new Error('failed to acquire inner lock'));
        }
      });
    });

    outerLockReleaser();
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
