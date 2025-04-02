// Import fake-indexeddb for tests
import { indexedDB, IDBKeyRange } from 'fake-indexeddb';

// Enable silent mode for tests
const SILENT_SETUP = true;

function log(...args) {
  if (!SILENT_SETUP) {
    console.log(...args);
  }
}

// Make indexedDB globally available in test contexts
if (typeof window === 'undefined') {
  global.indexedDB = indexedDB;
  global.IDBKeyRange = IDBKeyRange;
} else {
  window.indexedDB = indexedDB;
  window.IDBKeyRange = IDBKeyRange;
}

// Export a function to create and initialize a database
export async function createTestDatabase(name, ...storeNames) {
  return new Promise((resolve, reject) => {
    const request = indexedDB.deleteDatabase(name);
    
    request.onsuccess = () => {
      const request = indexedDB.open(name);
      
      request.onupgradeneeded = () => {
        const db = request.result;
        
        // Create object stores
        for (const storeName of storeNames) {
          const store = db.createObjectStore(storeName);
          log(`Creating object store '${storeName}' in database '${name}'`);
        }
      };
      
      request.onsuccess = () => {
        log(`Successfully set up database '${name}'`);
        resolve(request.result);
      };
      
      request.onerror = () => {
        reject(new Error(`Failed to create database '${name}'`));
      };
    };
    
    request.onerror = () => {
      reject(new Error(`Failed to delete database '${name}'`));
    };
  });
}

// Create a default database for tests
export const defaultDb = createTestDatabase('test_db', 'default');

// Initialize a value in the database
export function initValue(db, objectStore, key, value) {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction([objectStore], 'readwrite');
    const store = transaction.objectStore(objectStore);
    const request = store.put(value, key);
    
    request.onsuccess = () => {
      log(`Set key '${key}' in '${objectStore}'`);
      resolve();
    };
    
    request.onerror = () => {
      reject(new Error(`Failed to set key '${key}' in '${objectStore}'`));
    };
  });
}

// Export to make available
export { indexedDB, IDBKeyRange };

// Define test path databases that tests will use
const TEST_PATHS = Array.from({ length: 50 }, (_, i) => `test_db_${i}`);

// Define column families that will be used in tests
const COLUMN_FAMILIES = [
  'default',
  'a',
  'b',
  'c',
  'd'
];

// Pre-create databases and object stores for testing
setupTestDatabases(TEST_PATHS, COLUMN_FAMILIES);

/**
 * Helper function to set up test databases with required object stores
 * This ensures that all object stores are created before any tests run
 * 
 * @param {string[]} paths - Database paths to create
 * @param {string[]} columnFamilies - Column families (object stores) to create
 */
async function setupTestDatabases(paths, columnFamilies) {
  // Setup each database path
  for (const path of paths) {
    // Convert path to valid database name
    const dbName = path.replace(/[^a-zA-Z0-9]/g, '_');

    log(`Pre-creating database '${dbName}' with all required object stores`);

    try {
      // Open the database with a version high enough for all needed object stores
      await new Promise((resolve, reject) => {
        const request = indexedDB.open(dbName, 1);

        request.onerror = (event) => {
          const error = event.target.error || new Error('Unknown error');
          if (!SILENT_SETUP) {
            console.error(`Error creating test database '${dbName}':`, error);
          }
          reject(error);
        };

        request.onupgradeneeded = (event) => {
          const db = event.target.result;

          // Create all object stores
          for (const cfName of columnFamilies) {
            if (!db.objectStoreNames.contains(cfName)) {
              log(`Creating object store '${cfName}' in database '${dbName}'`);
              db.createObjectStore(cfName);
            }
          }
        };

        request.onsuccess = (event) => {
          const db = event.target.result;
          log(`Successfully set up database '${dbName}'`);
          db.close();
          resolve();
        };
      });
    } catch (err) {
      if (!SILENT_SETUP) {
        console.error(`Failed to set up database '${dbName}':`, err);
      }
    }
  }
}

// Export for direct use in tests
export { setupTestDatabases }; 