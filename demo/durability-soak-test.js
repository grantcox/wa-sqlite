// Copyright 2024 Roy T. Hashimoto. All Rights Reserved.

// Durability soak test main controller
// This file manages the iframe-based test iterations to ensure proper memory cleanup

const searchParams = new URLSearchParams(location.search);

let selectedFileBlobUrl = null;
let parsedQueries = [];
let testIterationCount = 0;
let maxIterationCount = 5000;
let totalQueriesRun = 0;
let currentTestRunning = false;
let testStartTime = 0;

// UI Elements
const output = document.getElementById('output');
const timestamp = document.getElementById('timestamp');
const iterationCountElement = document.getElementById('iteration-count');
const queryCountElement = document.getElementById('query-count');
const memoryUsageElement = document.getElementById('memory-usage');
const iframeContainer = document.getElementById('iframe-container');

/**
 * Starts a new iteration of the soak test in an iframe
 */
async function runSoakTestIteration() {
  if (currentTestRunning || testIterationCount >= maxIterationCount) {
    return;
  }
  
  currentTestRunning = true;
  testIterationCount++;
  
  // Create query parameter string for the iframe (without the large queries)
  const dbName = searchParams.get('dbName') ?? 'hello';
  const password = searchParams.get('password') || 'abcd123';
  
  const params = new URLSearchParams({
    dbName,
    password,
    iteration: testIterationCount,
    maxIterations: maxIterationCount,
    totalQueries: totalQueriesRun
  });
  
  // Update UI
  output.innerHTML += `<div>Running soak test iteration ${testIterationCount}...</div>`;
  output.scrollTop = output.scrollHeight;
  
  // Update statistics
  iterationCountElement.textContent = testIterationCount.toString();
  queryCountElement.textContent = totalQueriesRun.toString();
  updateMemoryUsage();
  
  // Create and load iframe
  const iframe = document.createElement('iframe');
  iframe.style.width = '0';
  iframe.style.height = '0';
  iframe.style.border = 'none';
  
  // Create a promise for when the iframe completes
  const iframeComplete = new Promise((resolve) => {
    window.addEventListener('message', function onMessage(event) {
      if (event.data.type === 'iterationComplete' && event.data.iteration === testIterationCount) {
        window.removeEventListener('message', onMessage);
        resolve(event.data);
      } else if (event.data.type === 'integrityCheck') {
        // Update UI with integrity check results
        if (event.data.passed) {
          output.innerHTML += `<div>Integrity check passed for iteration ${event.data.iteration}.</div>`;
          if (event.data.tableSizes) {
            const tableCount = Object.keys(event.data.tableSizes).length;
            output.innerHTML += `<div>Tables found: ${tableCount}</div>`;
          }
        } else {
          output.innerHTML += `<div style="color: red">Integrity check FAILED: <pre>${JSON.stringify(event.data.integrityResult, null, 2)}</pre></div>`;
        }
        output.scrollTop = output.scrollHeight;
      }
    });
  });
  
  // Create a reference to the iframe for later messaging
  const iframeId = `iframe-${testIterationCount}`;
  iframe.id = iframeId;
  
  // Set up a message listener for when the iframe is ready to receive queries
  window.addEventListener('message', function onIframeReady(event) {
    if (event.data.type === 'iframeReady' && event.data.iteration === testIterationCount) {
      // Remove this specific event listener once it's fired
      window.removeEventListener('message', onIframeReady);
      
      // share a reference to the query file so the iframe can load it
      
      if (selectedFileBlobUrl) {
        iframe.contentWindow.postMessage({
          type: 'queryFileReference',
          fileName: selectedFileBlobUrl
        }, '*');
      } else {
        console.error('No blob URL available for file reference');
        iframe.contentWindow.postMessage({
          type: 'queries',
          queries: [] // Empty array as fallback
        }, '*');
      }
    }
  });
  
  // Load the iframe
  iframe.src = `durability-soak-test-runner.html?${params.toString()}`;
  iframeContainer.appendChild(iframe);
  
  // Wait for iteration to complete
  const result = await iframeComplete;
  
  // Handle iteration results
  if (result.passed) {
    totalQueriesRun += result.queriesRun;
    queryCountElement.textContent = totalQueriesRun.toString();
    
    // Calculate and display time metrics 
    const elapsedSeconds = Math.floor((Date.now() - testStartTime) / 1000);
    const queriesPerSecond = totalQueriesRun / elapsedSeconds;
    
    output.innerHTML += `<div>Iteration ${testIterationCount} completed. (${result.queriesRun} queries, ${queriesPerSecond.toFixed(1)} q/s)</div>`;
    output.scrollTop = output.scrollHeight;
    
    // Remove iframe after a short delay to ensure proper cleanup
    setTimeout(() => {
      iframeContainer.removeChild(iframe);
      
      // Force garbage collection if available
      if (window.gc) {
        window.gc();
      }
      
      updateMemoryUsage();
      
      // Continue with next iteration if not at max
      currentTestRunning = false;
      if (testIterationCount < maxIterationCount) {
        setTimeout(() => {
          runSoakTestIteration();
        }, 100);
      } else {
        output.innerHTML += `<div><strong>Soak test successfully completed after ${maxIterationCount} iterations, ${totalQueriesRun} queries.</strong></div>`;
        output.scrollTop = output.scrollHeight;
        document.getElementById('execute-soak-test').disabled = false;
      }
    }, 500);
  } else {
    output.innerHTML += `<div style="color: red"><strong>Iteration ${testIterationCount} failed, stopping test.</strong></div>`;
    output.scrollTop = output.scrollHeight;
    
    // Keep the iframe around for debugging if it failed
    currentTestRunning = false;
    document.getElementById('execute-soak-test').disabled = false;
  }
}

/**
 * Update the memory usage display
 */
function updateMemoryUsage() {
  if (window.performance && window.performance.memory) {
    const memoryInfo = window.performance.memory;
    const usedHeapSize = memoryInfo.usedJSHeapSize / (1024 * 1024);
    const totalHeapSize = memoryInfo.totalJSHeapSize / (1024 * 1024);
    memoryUsageElement.textContent = `${usedHeapSize.toFixed(1)} / ${totalHeapSize.toFixed(1)} MB`;
  } else {
    memoryUsageElement.textContent = 'Not available';
  }
}

/**
 * Initialize the application
 */
async function init() {
  const executeTestButton = /** @type {HTMLButtonElement} */(document.getElementById('execute-soak-test'));
  const fileInput = /** @type {HTMLInputElement} */(document.getElementById('sql-file'));
  const fileInfo = document.getElementById('sql-file-info');
  const maxIterationsSelect = /** @type {HTMLSelectElement} */(document.getElementById('max-iterations'));
  
  // Reset if requested
  await maybeReset(searchParams);
  
  // Set up iteration selection
  maxIterationsSelect.addEventListener('change', function() {
    maxIterationCount = parseInt(maxIterationsSelect.value, 10);
  });
  
  // Create a blob URL for direct file access if needed
  let selectedFileBlob = null;
  
  // Handle file selection
  fileInput.addEventListener('change', async function() {
    if (fileInput.files && fileInput.files.length > 0) {
      const file = fileInput.files[0];
      fileInfo.textContent = `Selected: ${file.name} (${formatFileSize(file.size)})`;

      try {
        // Get the file content
        const fileContent = await file.text();
        // Create a blob for sharing with the iframe
        selectedFileBlob = new Blob([fileContent], { type: 'application/json' });
        
        // Revoke any existing blob URL
        if (selectedFileBlobUrl) {
          URL.revokeObjectURL(selectedFileBlobUrl);
        }
        
        // Create a blob URL
        selectedFileBlobUrl = URL.createObjectURL(selectedFileBlob);
      
        executeTestButton.disabled = false;
      } catch (e) {
        output.innerHTML = `<pre>Error parsing JSON: ${e.message}</pre>`;
      }
    } else {
      fileInfo.textContent = '';
      
      // Revoke blob URL if it exists
      if (selectedFileBlobUrl) {
        URL.revokeObjectURL(selectedFileBlobUrl);
        selectedFileBlobUrl = null;
      }
      
      selectedFileBlob = null;
    }
  });

  // Start soak test on button click
  executeTestButton.addEventListener('click', async function() {
    if (!selectedFileBlobUrl) {
      output.innerHTML = '<div>No queries loaded. Please select a JSON file with SQL queries.</div>';
      return;
    }
    
    executeTestButton.disabled = true;
    
    // Reset counters
    testIterationCount = 0;
    totalQueriesRun = 0;
    testStartTime = Date.now();
    
    // Update UI
    output.innerHTML = '<div>Starting soak test...</div>';
    iterationCountElement.textContent = '0';
    queryCountElement.textContent = '0';
    updateMemoryUsage();
    
    // Start the first iteration after a short delay
    setTimeout(() => {
      runSoakTestIteration();
    }, 100);
  });
  
  // Clean up resources when the page is unloaded
  window.addEventListener('beforeunload', () => {
    if (selectedFileBlobUrl) {
      URL.revokeObjectURL(selectedFileBlobUrl);
    }
  });
  
  // Set up periodic memory usage updates
  setInterval(updateMemoryUsage, 2000);
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

/**
 * Reset OPFS and IndexedDB if requested
 */
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

if (document.readyState !== 'loading') {
  init();
} else {
  document.addEventListener('DOMContentLoaded', init);
}
