// Import fake-indexeddb to simulate IndexedDB in Node.js environment
import 'fake-indexeddb/auto';
import { IDBFactory } from 'fake-indexeddb';

// Ensure fake-indexeddb is globally available
global.indexedDB = global.indexedDB || new IDBFactory();
console.log('Fake IndexedDB initialized globally for tests');

// Helper function to pre-create test databases with required object stores
async function setupTestDatabases() {
  // Test paths that might be used
  const testPaths = [
    'test_path',
    'test-db',
    'test-db-0',
    'test-db-1',
    'test-db-2',
    'test-db-3'
  ];
  
  // Column families that might be used
  const columnFamilies = ['default', 'a', 'b', 'c', 'd'];
  
  for (const path of testPaths) {
    // Convert path to valid database name
    const dbName = path.replace(/[^a-zA-Z0-9]/g, '_');
    console.log(`Pre-creating database '${dbName}' with all required object stores`);
    
    try {
      // Open the database and create all possible stores
      const dbOpenRequest = global.indexedDB.open(dbName, 1);
      
      await new Promise((resolve, reject) => {
        dbOpenRequest.onupgradeneeded = (event) => {
          const db = event.target.result;
          
          // Create all column family stores
          for (const cfName of columnFamilies) {
            if (!db.objectStoreNames.contains(cfName)) {
              console.log(`Creating object store '${cfName}' in database '${dbName}'`);
              db.createObjectStore(cfName);
            }
          }
        };
        
        dbOpenRequest.onsuccess = (event) => {
          // Close the connection when done
          const db = event.target.result;
          db.close();
          console.log(`Successfully set up database '${dbName}'`);
          resolve();
        };
        
        dbOpenRequest.onerror = (event) => {
          console.error(`Error setting up database '${dbName}':`, event.target.error);
          reject(event.target.error);
        };
      });
    } catch (err) {
      console.error(`Failed to set up database '${dbName}':`, err);
    }
  }
}

// Run setup immediately
setupTestDatabases().catch(err => {
  console.error('Failed to set up test databases:', err);
});

// Export for direct use in tests
export { setupTestDatabases }; 