// Set up fake-indexeddb for testing
import { indexedDB, IDBKeyRange } from "fake-indexeddb";
import binding from "../lib/binding.js";

// Configure the binding to use fake-indexeddb
binding._setIndexedDB(indexedDB);

// Make IDBKeyRange available globally
globalThis.IDBKeyRange = IDBKeyRange;

// Helper for directly writing test data
export async function writeTestData(dbHandle, storeName, data) {
  if (!dbHandle || !dbHandle.db) throw new Error("Database not opened");

  return new Promise((resolve, reject) => {
    const transaction = dbHandle.db.transaction([storeName], "readwrite");
    const store = transaction.objectStore(storeName);

    let completed = 0;
    const total = data.length;

    data.forEach(({ key, value }) => {
      const request = store.put(value, key);

      request.onsuccess = () => {
        completed++;
        if (completed === total) {
          resolve();
        }
      };

      request.onerror = (event) => {
        reject(new Error(event.target.error.message));
      };
    });

    if (total === 0) resolve();
  });
}

// Export for use in tests
export { indexedDB, IDBKeyRange };
