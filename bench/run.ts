/**
 * sqliteq benchmark
 *
 * Run: npx tsx bench/run.ts
 *
 * Results depend heavily on the SQLite binding used.
 * better-sqlite3 (synchronous C++ addon) is recommended for best performance.
 * Also verified with bun:sqlite (25/25 tests passing).
 */

import Database from 'better-sqlite3'
import { Queue } from '../src/index.js'
import { unlinkSync, existsSync } from 'fs'

function freshDB(name: string) {
  const path = `/tmp/sqliteq-bench-${name}.db`
  for (const suffix of ['', '-wal', '-shm']) {
    if (existsSync(path + suffix)) unlinkSync(path + suffix)
  }
  return { db: new Database(path), path }
}

function cleanup(path: string) {
  for (const suffix of ['', '-wal', '-shm']) {
    try { unlinkSync(path + suffix) } catch {}
  }
}

function fmt(n: number): string {
  return n.toLocaleString()
}

console.log('\n=== sqliteq benchmark (better-sqlite3) ===\n')

// 1. send → receive → delete cycle
{
  const { db, path } = freshDB('cycle')
  const q = new Queue(db, 'bench', { timeout: 60_000 })

  const iterations = 10_000
  const start = performance.now()

  for (let i = 0; i < iterations; i++) {
    q.send(`message-${i}`)
    const msg = q.receive()
    if (msg) q.delete(msg.id, msg.received)
  }

  const elapsed = performance.now() - start
  const opsPerSec = Math.round(iterations / (elapsed / 1000))
  const usPerOp = Math.round(elapsed / iterations * 1000)

  console.log(`send+receive+delete:  ${fmt(opsPerSec)} ops/sec  (${usPerOp} μs/op)`)

  db.close()
  cleanup(path)
}

// 2. send-only throughput
{
  const { db, path } = freshDB('send')
  const q = new Queue(db, 'bench', { timeout: 60_000 })

  const iterations = 50_000
  const start = performance.now()

  for (let i = 0; i < iterations; i++) {
    q.send(`message-${i}`)
  }

  const elapsed = performance.now() - start
  const opsPerSec = Math.round(iterations / (elapsed / 1000))

  console.log(`send only:            ${fmt(opsPerSec)} ops/sec`)

  db.close()
  cleanup(path)
}

// 3. receive+delete on big table (100K messages, 10 queues)
{
  const { db, path } = freshDB('bigtable')
  const queues: Queue[] = []
  for (let i = 0; i < 10; i++) {
    queues.push(new Queue(db, `q${i}`, { timeout: 60_000 }))
  }

  console.log(`\nPopulating 100K messages across 10 queues...`)
  const populateStart = performance.now()

  const pastTimeout = new Date(Date.now() - 1000).toISOString() // already available for receive
  const insertStmt = db.prepare(
    'insert into sqliteq (queue, body, timeout, priority) values (?, ?, ?, 0)'
  )
  db.transaction(() => {
    for (let i = 0; i < 100_000; i++) {
      insertStmt.run(`q${i % 10}`, JSON.stringify(`msg-${i}`), pastTimeout)
    }
  })()

  console.log(`  Populated in ${Math.round(performance.now() - populateStart)}ms`)

  const iterations = 10_000
  const start = performance.now()

  for (let i = 0; i < iterations; i++) {
    const q = queues[i % 10]
    const msg = q.receive()
    if (msg) q.delete(msg.id, msg.received)
  }

  const elapsed = performance.now() - start
  const opsPerSec = Math.round(iterations / (elapsed / 1000))
  const usPerOp = Math.round(elapsed / iterations * 1000)

  console.log(`big table recv+del:   ${fmt(opsPerSec)} ops/sec  (${usPerOp} μs/op)`)

  db.close()
  cleanup(path)
}

// 4. sendBatch throughput
{
  const { db, path } = freshDB('batch')
  const q = new Queue(db, 'bench', { timeout: 60_000 })

  const batchSize = 100
  const batches = 100
  const total = batchSize * batches

  const messages = Array.from({ length: batchSize }, (_, i) => ({ body: `msg-${i}` }))

  const start = performance.now()
  for (let i = 0; i < batches; i++) {
    q.sendBatch(messages)
  }
  const elapsed = performance.now() - start
  const opsPerSec = Math.round(total / (elapsed / 1000))

  console.log(`sendBatch (100/tx):   ${fmt(opsPerSec)} ops/sec`)

  db.close()
  cleanup(path)
}

console.log('\n=== done ===\n')
