/**
 * bun:sqlite compatibility test.
 * Run: bun test/bun-compat.ts
 *
 * Minimal set to verify sqliteq works with Bun's native SQLite binding.
 * Full behavioral coverage is in the vitest suite (better-sqlite3).
 * This file only tests things that could differ across bindings:
 * schema init, SQL execution, transactions, timers, async.
 */

import { Database } from 'bun:sqlite'
import { Queue, Processor } from '../src/index.js'

const db = new Database(':memory:')
let passed = 0
let failed = 0

function assert(condition: boolean, name: string) {
  if (condition) {
    passed++
    console.log(`  ✓ ${name}`)
  } else {
    failed++
    console.log(`  ✗ ${name}`)
  }
}

console.log('\n=== bun:sqlite compatibility ===\n')

// 1. Basic send/receive/delete
{
  const q = new Queue(db as never, 'test1', { timeout: 60_000 })
  const id = q.send('hello')
  assert(typeof id === 'string' && id.startsWith('m_'), 'send returns ID')

  const msg = q.receive()
  assert(msg !== null, 'receive returns message')
  assert(msg!.body === 'hello', 'body round-trips correctly')
  assert(msg!.received === 1, 'received count is 1')

  const deleted = q.delete(msg!.id, msg!.received)
  assert(deleted === true, 'delete returns true')
  assert(q.receive() === null, 'queue is empty after delete')
}

// 2. Visibility timeout + redelivery
{
  const q = new Queue(db as never, 'test2', { timeout: 50 })
  q.send('retry-me')
  const msg1 = q.receive()!
  await Bun.sleep(100)
  const msg2 = q.receive()!
  assert(msg2.id === msg1.id, 'same message redelivered')
  assert(msg2.received === 2, 'received count incremented')
}

// 3. Fencing (stale delete)
{
  const q = new Queue(db as never, 'test3', { timeout: 50 })
  q.send('fenced')
  const msg1 = q.receive()!
  await Bun.sleep(100)
  const msg2 = q.receive()!

  assert(q.delete(msg1.id, msg1.received) === false, 'stale delete returns false')
  assert(q.size() === 1, 'message still exists after stale delete')
  assert(q.delete(msg2.id, msg2.received) === true, 'current consumer can delete')
}

// 4. Fencing (stale extend)
{
  const q = new Queue(db as never, 'test4', { timeout: 50 })
  q.send('extend-me')
  const msg1 = q.receive()!
  await Bun.sleep(100)
  const msg2 = q.receive()!

  assert(q.extend(msg1.id, msg1.received, 1_000) === false, 'stale extend returns false')
  assert(q.extend(msg2.id, msg2.received, 1_000) === true, 'current extend returns true')
}

// 5. sendBatch (transactions)
{
  const q = new Queue(db as never, 'test5')
  const ids = q.sendBatch([{ body: 'a' }, { body: 'b' }, { body: 'c' }])
  assert(ids.length === 3, 'sendBatch returns 3 IDs')
  assert(q.size() === 3, 'size is 3')
}

// 6. JSON round-trip
{
  interface Job { task: string; n: number }
  const q = new Queue<Job>(db as never, 'test6')
  q.send({ task: 'email', n: 42 })
  const msg = q.receive()!
  assert(msg.body.task === 'email' && msg.body.n === 42, 'typed payload round-trips')
}

// 7. Processor (timers + async)
{
  const q = new Queue(db as never, 'test7', { timeout: 1_000 })
  q.send('process-me')

  const processed: string[] = []
  const p = new Processor(q, {
    handler: (msg) => { processed.push(msg.body) },
    pollInterval: 10,
    onError: () => {},
  })

  p.start()
  await Bun.sleep(200)
  await p.stop()
  assert(processed[0] === 'process-me', 'Processor processes message')
  assert(q.size() === 0, 'Processor auto-deletes on success')
}

console.log(`\n${passed} passed, ${failed} failed\n`)
process.exit(failed > 0 ? 1 : 0)
