/**
 * sqliteq benchmark
 *
 * Run: npx tsx bench/run.ts
 *
 * Results depend heavily on the SQLite binding used.
 * better-sqlite3 (synchronous C++ addon) is recommended for best performance.
 * Also verified with bun:sqlite (25/25 tests passing).
 */

import { Queue, BetterSqlite3Driver, BunSqliteDriver } from '../src/index.js'
import { unlinkSync, existsSync } from 'fs'

let Database: any
let driverClass: any
if (typeof process.versions.bun !== 'undefined') {
  Database = (await import('bun:sqlite')).Database
  driverClass = BunSqliteDriver
} else {
  Database = (await import('better-sqlite3')).default
  driverClass = BetterSqlite3Driver
}

function freshDB(name: string) {
  const path = `/tmp/sqliteq-bench-${name}.db`
  for (const suffix of ['', '-wal', '-shm']) {
    if (existsSync(path + suffix)) unlinkSync(path + suffix)
  }
  const db = new Database(path)
  return { db, path, driver: new driverClass(db) }
}

function cleanup(path: string) {
  for (const suffix of ['', '-wal', '-shm']) {
    try { unlinkSync(path + suffix) } catch {}
  }
}

function fmt(n: number): string {
  return n.toLocaleString()
}

const binding = typeof process.versions.bun !== 'undefined' ? 'bun:sqlite' : 'better-sqlite3'
console.log(`\n=== sqliteq benchmark (${binding}) ===\n`)

// 1. send → receive → delete cycle
{
  const { db, path, driver } = freshDB('cycle')
  const q = new Queue(driver, 'bench', { timeout: 60_000 })
  await q.init()

  const iterations = 10_000
  const start = performance.now()

  for (let i = 0; i < iterations; i++) {
    await q.send(`message-${i}`)
    const msg = await q.receive()
    if (msg) await q.delete(msg.id, msg.received)
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
  const { db, path, driver } = freshDB('send')
  const q = new Queue(driver, 'bench', { timeout: 60_000 })
  await q.init()

  const iterations = 50_000
  const start = performance.now()

  for (let i = 0; i < iterations; i++) {
    await q.send(`message-${i}`)
  }

  const elapsed = performance.now() - start
  const opsPerSec = Math.round(iterations / (elapsed / 1000))

  console.log(`send only:            ${fmt(opsPerSec)} ops/sec`)

  db.close()
  cleanup(path)
}

// 3. receive+delete on big table (100K messages, 10 queues)
{
  const { db, path, driver } = freshDB('bigtable')
  const queues: Queue[] = []
  for (let i = 0; i < 10; i++) {
    queues.push(new Queue(driver, `q${i}`, { timeout: 60_000 }))
  }
  await queues[0].init()

  console.log(`\nPopulating 100K messages across 10 queues...`)
  const populateStart = performance.now()

  const pastTimeout = new Date(Date.now() - 1000).toISOString()
  
  // High-performance direct insertion using driver.batch
  const batchSize = 1000
  for (let i = 0; i < 100_000; i += batchSize) {
    const sqls: string[] = []
    // Note: Drivers currently don't support parameterized batching well for speed, 
    // so we build raw SQL for the population phase of the benchmark.
    const batchSql = []
    for (let j = 0; j < batchSize; j++) {
        const qIdx = (i + j) % 10
        const body = JSON.stringify(`msg-${i+j}`)
        batchSql.push(`insert into sqliteq (queue, body, timeout, priority) values ('q${qIdx}', '${body}', '${pastTimeout}', 0)`)
    }
    await driver.batch(batchSql)
  }

  console.log(`  Populated in ${Math.round(performance.now() - populateStart)}ms`)

  const iterations = 10_000
  const start = performance.now()

  for (let i = 0; i < iterations; i++) {
    const q = queues[i % 10]
    const msg = await q.receive()
    if (msg) await q.delete(msg.id, msg.received)
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
  const { db, path, driver } = freshDB('batch')
  const q = new Queue(driver, 'bench', { timeout: 60_000 })
  await q.init()

  const batchSize = 100
  const batches = 100
  const total = batchSize * batches

  const messages = Array.from({ length: batchSize }, (_, i) => ({ body: `msg-${i}` }))

  const start = performance.now()
  for (let i = 0; i < batches; i++) {
    await q.sendBatch(messages)
  }
  const elapsed = performance.now() - start
  const opsPerSec = Math.round(total / (elapsed / 1000))

  console.log(`sendBatch (100/tx):   ${fmt(opsPerSec)} ops/sec`)

  db.close()
  cleanup(path)
}

console.log('\n=== done ===\n')
