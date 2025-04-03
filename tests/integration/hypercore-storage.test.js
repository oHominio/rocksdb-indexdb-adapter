import test from 'brittle'
import Corestore from 'corestore'
import HypercoreStorage from 'hypercore-storage'
import { IndexDBStorage } from '../../index.js'
import b4a from 'b4a'

test('RocksDB IndexedDB adapter with Corestore', async t => {
  // Create our RocksDB IndexedDB adapter
  const dbPath = `test_hypercore_${Date.now()}`
  const db = new IndexDBStorage(dbPath)
  await db.open()
  
  // Create a Corestore with our adapter directly
  const store = new Corestore(db)
  await store.ready()
  
  // Create two cores to test with
  const core1 = store.get({ name: 'core-1' })
  const core2 = store.get({ name: 'core-2' })
  
  // Wait for cores to be ready
  await Promise.all([core1.ready(), core2.ready()])
  
  // Log core keys for debugging
  console.log('Core 1 key:', b4a.toString(core1.key, 'hex'))
  console.log('Core 2 key:', b4a.toString(core2.key, 'hex'))
  
  // Test appending and reading data from core1
  const testData1 = Buffer.from('Hello from core1')
  await core1.append(testData1)
  
  // Verify the data was stored correctly
  const result1 = await core1.get(0)
  t.alike(result1, testData1, 'Core 1 data was stored and retrieved correctly')
  
  // Test appending and reading data from core2
  const testData2 = Buffer.from('Hello from core2')
  await core2.append(testData2)
  
  // Verify the data was stored correctly
  const result2 = await core2.get(0)
  t.alike(result2, testData2, 'Core 2 data was stored and retrieved correctly')
  
  // Test that core length is correct
  t.is(core1.length, 1, 'Core 1 has correct length')
  t.is(core2.length, 1, 'Core 2 has correct length')
  
  // Test updating with multiple entries
  await core1.append([
    Buffer.from('Entry 1'),
    Buffer.from('Entry 2'),
    Buffer.from('Entry 3')
  ])
  
  t.is(core1.length, 4, 'Core 1 length updated correctly after batch append')
  
  // Test reading multiple entries
  const entry1 = await core1.get(1)
  const entry2 = await core1.get(2)
  const entry3 = await core1.get(3)
  
  t.alike(entry1, Buffer.from('Entry 1'), 'Retrieved correct entry 1')
  t.alike(entry2, Buffer.from('Entry 2'), 'Retrieved correct entry 2')
  t.alike(entry3, Buffer.from('Entry 3'), 'Retrieved correct entry 3')
  
  // Clean up
  await store.close()
  await db.close()
  t.pass('Closed store and database successfully')
})

test('RocksDB IndexedDB adapter with HypercoreStorage and Corestore', async t => {
  // Create our RocksDB IndexedDB adapter
  const dbPath = `test_hypercore_storage_${Date.now()}`
  const db = new IndexDBStorage(dbPath)
  await db.open()
  
  // Create a HypercoreStorage with our adapter
  const storage = new HypercoreStorage(db)
  
  // Create a Corestore with the HypercoreStorage
  const store = new Corestore(storage)
  await store.ready()
  
  // Create a core
  const core = store.get({ name: 'test-core' })
  await core.ready()
  
  console.log('HypercoreStorage Core key:', b4a.toString(core.key, 'hex'))
  
  // Append data
  const testData = Buffer.from('Hello Corestore with HypercoreStorage')
  await core.append(testData)
  
  // Read data
  const result = await core.get(0)
  t.alike(result, testData, 'Data stored through HypercoreStorage was retrieved correctly')
  
  // Test with multiple blocks
  const blocks = [
    Buffer.from('HypercoreStorage Block 1'),
    Buffer.from('HypercoreStorage Block 2'),
    Buffer.from('HypercoreStorage Block 3')
  ]
  
  await core.append(blocks)
  t.is(core.length, 4, 'Core length is correct after appending multiple blocks')
  
  // Read the blocks back
  const block1 = await core.get(1)
  const block2 = await core.get(2)
  const block3 = await core.get(3)
  
  t.alike(block1, blocks[0], 'Block 1 was stored and retrieved correctly')
  t.alike(block2, blocks[1], 'Block 2 was stored and retrieved correctly')
  t.alike(block3, blocks[2], 'Block 3 was stored and retrieved correctly')
  
  // Clean up
  await store.close()
  await db.close()
  t.pass('Closed store and database successfully')
}) 