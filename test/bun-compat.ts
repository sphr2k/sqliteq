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
import { Queue, Processor, BunSqliteDriver } from '../src/index.js'

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

const driver = new BunSqliteDriver(db)

// 1. Basic send/receive/delete
{
  const q = new Queue(driver, 'test1', { timeout: 60_000 })
  await q.init()
  const id = await q.send('hello')
  assert(typeof id === 'string' && id.startsWith('m_'), 'send returns ID')

  const msg = await q.receive()
  assert(msg !== null, 'receive returns message')
  assert(msg!.body === 'hello', 'body round-trips correctly')
  assert(msg!.received === 1, 'received count is 1')

  const deleted = await q.delete(msg!.id, msg!.received)
  assert(deleted === true, 'delete returns true')
  assert(await q.receive() === null, 'queue is empty after delete')
}

// 2. Visibility timeout + redelivery
{
  const q = new Queue(driver, 'test2', { timeout: 50 })
  await q.init()
  await q.send('retry-me')
  const msg1 = (await q.receive())!
  await Bun.sleep(100)
  const msg2 = (await q.receive())!
  assert(msg2.id === msg1.id, 'same message redelivered')
  assert(msg2.received === 2, 'received count incremented')
}

// 3. Fencing (stale delete)
{
  const q = new Queue(driver, 'test3', { timeout: 50 })
  await q.init()
  await q.send('fenced')
  const msg1 = (await q.receive())!
  await Bun.sleep(100)
  const msg2 = (await q.receive())!

  assert(await q.delete(msg1.id, msg1.received) === false, 'stale delete returns false')
  assert(await q.size() === 1, 'message still exists after stale delete')
  assert(await q.delete(msg2.id, msg2.received) === true, 'current consumer can delete')
}

// 4. Fencing (stale extend)
{
  const q = new Queue(driver, 'test4', { timeout: 50 })
  await q.init()
  await q.send('extend-me')
  const msg1 = (await q.receive())!
  await Bun.sleep(100)
  const msg2 = (await q.receive())!

  assert(await q.extend(msg1.id, msg1.received, 1_000) === false, 'stale extend returns false')
  assert(await q.extend(msg2.id, msg2.received, 1_000) === true, 'current extend returns true')
}

// 5. sendBatch (transactions)
{
  const q = new Queue(driver, 'test5')
  await q.init()
  const ids = await q.sendBatch([{ body: 'a' }, { body: 'b' }, { body: 'c' }])
  assert(ids.length === 3, 'sendBatch returns 3 IDs')
  assert(await q.size() === 3, 'size is 3')
}

// 6. JSON round-trip
{
  interface Job { task: string; n: number }
  const q = new Queue<Job>(driver, 'test6')
  await q.init()
  await q.send({ task: 'email', n: 42 })
  const msg = (await q.receive())!
  assert(msg.body.task === 'email' && msg.body.n === 42, 'typed payload round-trips')
}

// 7. Processor (timers + async)
{
  const q = new Queue(driver, 'test7', { timeout: 1_000 })
  await q.init()
  await q.send('process-me')

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
  assert(await q.size() === 0, 'Processor auto-deletes on success')
}

// 8. stats
{
  const q = new Queue(db as never, 'test8', { timeout: 50, maxReceive: 1 })
  q.send('inflight')
  const inflight = q.receive()!
  q.extend(inflight.id, inflight.received, 500)
  q.send('ready')
  q.send('delayed', { delay: 200 })

  const stats = q.stats()
  assert(stats.total === 3, 'stats.total counts all messages')
  assert(stats.ready === 1, 'stats.ready counts visible messages')
  assert(stats.delayed === 1, 'stats.delayed counts scheduled messages')
  assert(stats.inFlight === 1, 'stats.inFlight counts claimed messages')
  assert(stats.dead === 0, 'stats.dead excludes non-dead messages')
}

// 9. DLQ operations
{
  const q = new Queue(db as never, 'test9', { timeout: 50, maxReceive: 1 })
  q.send('dead-letter')
  const old = q.receive()!
  await Bun.sleep(100)

  const ids = q.requeueDeadLetters()
  assert(ids.length === 1, 'requeueDeadLetters returns new IDs')
  assert(ids[0] !== old.id, 'requeued dead letter gets a new ID')
  assert(q.deadLetters().length === 0, 'requeued dead letters are removed from DLQ')

  q.send('purge-me')
  q.receive()
  await Bun.sleep(100)
  assert(q.purgeDeadLetters() === 1, 'purgeDeadLetters removes dead letters')
}

// 10. receiveBatch
{
  const q = new Queue(db as never, 'test10', { timeout: 1_000 })
  q.send('a')
  q.send('b')
  q.send('c')

  const batch = q.receiveBatch(2)
  assert(batch.length === 2, 'receiveBatch returns up to the requested limit')
  assert(batch[0]!.body === 'a' && batch[1]!.body === 'b', 'receiveBatch preserves queue order')
  assert(q.receive()!.body === 'c', 'receiveBatch leaves remaining messages available')
}

console.log(`\n${passed} passed, ${failed} failed\n`)
process.exit(failed > 0 ? 1 : 0)
