/**
 * Multi-process concurrency test for sqliteq.
 *
 * Verifies that two separate Node.js processes can safely consume from the
 * same file-backed SQLite queue without duplicates or lost messages.
 *
 * Run: npx tsx test/concurrency-multi-process.ts
 */

import { fork, type ChildProcess } from 'node:child_process'
import { unlinkSync, existsSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { dirname, join } from 'node:path'

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------
const N = 100 // total messages to enqueue
const WORKER_COUNT = 2
const DB_PATH = join(dirname(fileURLToPath(import.meta.url)), `_concurrency_test_${process.pid}.db`)
const TIMEOUT_MS = 15_000 // overall test timeout

// ---------------------------------------------------------------------------
// Worker mode — when this script is forked with --worker flag
// ---------------------------------------------------------------------------
if (process.argv.includes('--worker')) {
  const Database = (await import('better-sqlite3')).default
  const { Queue } = await import('../src/index.js')

  const dbPath = process.argv[process.argv.indexOf('--db') + 1]
  const db = new Database(dbPath)
  const q = new Queue<number>(db, 'bench', {
    timeout: 30_000, // long visibility so no accidental re-delivery
    maxReceive: 5,
  })

  // Signal ready and wait for "go" from parent
  process.send!({ type: 'ready' })
  await new Promise<void>((resolve) => {
    process.on('message', (msg: any) => {
      if (msg?.type === 'go') resolve()
    })
  })

  const processed: number[] = []
  let emptyStreak = 0
  const MAX_EMPTY_STREAK = 100 // consecutive nulls before giving up

  // Helper: async sleep to yield the event loop and give other processes
  // a chance to acquire the SQLite write lock.
  const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms))

  while (emptyStreak < MAX_EMPTY_STREAK) {
    const msg = q.receive()
    if (msg === null) {
      emptyStreak++
      await sleep(1)
      continue
    }
    emptyStreak = 0
    const ok = q.delete(msg.id, msg.received)
    if (ok) {
      processed.push(msg.body)
    }
    // Yield after each message so the other process gets a fair shot at
    // acquiring the SQLite lock. Without this, one process monopolizes
    // the lock because better-sqlite3 calls are synchronous and very fast.
    await sleep(1)
  }

  db.close()

  // Report results back to parent via IPC
  process.send!({ type: 'done', processed })
  process.exit(0)
}

// ---------------------------------------------------------------------------
// Main process
// ---------------------------------------------------------------------------
async function main() {
  const Database = (await import('better-sqlite3')).default
  const { Queue } = await import('../src/index.js')

  // Cleanup any leftover DB from a previous failed run
  cleanup()

  console.log(`[main] Creating DB at ${DB_PATH}`)
  const db = new Database(DB_PATH)
  const q = new Queue<number>(db, 'bench', {
    timeout: 30_000,
    maxReceive: 5,
  })

  // Enqueue N messages (body = sequential integer for easy verification)
  console.log(`[main] Sending ${N} messages...`)
  const batch = Array.from({ length: N }, (_, i) => ({ body: i }))
  q.sendBatch(batch)
  console.log(`[main] Queue size: ${q.size()}`)
  db.close()

  // Spawn workers
  console.log(`[main] Spawning ${WORKER_COUNT} worker processes...`)
  const thisFile = fileURLToPath(import.meta.url)

  interface WorkerHandle {
    workerId: number
    child: ChildProcess
    ready: Promise<void>
    done: Promise<{ workerId: number; processed: number[] }>
  }

  const workers: WorkerHandle[] = []

  for (let w = 0; w < WORKER_COUNT; w++) {
    const workerId = w
    const child = fork(thisFile, ['--worker', '--db', DB_PATH], {
      stdio: ['pipe', 'pipe', 'pipe', 'ipc'],
    })

    // Forward stderr for debugging
    child.stderr?.on('data', (data: Buffer) => {
      process.stderr.write(`[worker-${workerId}] ${data}`)
    })

    let readyResolve: () => void
    const ready = new Promise<void>((resolve) => { readyResolve = resolve })

    let doneResolve: (v: { workerId: number; processed: number[] }) => void
    let doneReject: (err: Error) => void
    const done = new Promise<{ workerId: number; processed: number[] }>((resolve, reject) => {
      doneResolve = resolve
      doneReject = reject
    })

    child.on('message', (msg: any) => {
      if (msg?.type === 'ready') {
        readyResolve()
      } else if (msg?.type === 'done') {
        doneResolve({ workerId, processed: msg.processed })
      }
    })

    child.on('error', (err) => {
      doneReject(new Error(`Worker ${workerId} error: ${err.message}`))
    })

    child.on('exit', (code) => {
      // If done hasn't resolved yet, this is unexpected
      if (code !== 0) {
        doneReject(new Error(`Worker ${workerId} exited with code ${code}`))
      }
    })

    workers.push({ workerId, child, ready, done })
  }

  // Wait for all workers to be ready (DB opened, Queue created)
  console.log(`[main] Waiting for all workers to be ready...`)
  await Promise.all(workers.map((w) => w.ready))

  // Signal all workers to start consuming simultaneously
  console.log(`[main] All workers ready — sending "go" signal`)
  for (const w of workers) {
    w.child.send({ type: 'go' })
  }

  // Wait for all workers with timeout
  const timer = setTimeout(() => {
    console.error(`[main] TIMEOUT after ${TIMEOUT_MS}ms — killing workers`)
    for (const w of workers) {
      w.child.kill()
    }
    process.exit(1)
  }, TIMEOUT_MS)

  const results = await Promise.all(workers.map((w) => w.done))
  clearTimeout(timer)

  // ---------------------------------------------------------------------------
  // Verify
  // ---------------------------------------------------------------------------
  const allProcessed: number[] = []
  for (const r of results) {
    console.log(`[main] Worker ${r.workerId} processed ${r.processed.length} messages`)
    allProcessed.push(...r.processed)
  }

  // 1. Total count must equal N
  const totalProcessed = allProcessed.length
  console.log(`[main] Total processed: ${totalProcessed} / ${N}`)

  // 2. No duplicates
  const unique = new Set(allProcessed)
  const duplicates = totalProcessed - unique.size
  console.log(`[main] Unique: ${unique.size}, duplicates: ${duplicates}`)

  // 3. Every message [0..N-1] was processed
  const expected = new Set(Array.from({ length: N }, (_, i) => i))
  const missing = [...expected].filter((x) => !unique.has(x))
  console.log(`[main] Missing: ${missing.length}`)

  // 4. Each worker got at least 1 message (work was distributed)
  const workerCounts = results.map((r) => r.processed.length)
  const minWorkerMessages = Math.min(...workerCounts)
  console.log(`[main] Per-worker counts: ${workerCounts.join(', ')}`)

  // Assertions
  let passed = true
  function assert(condition: boolean, label: string) {
    if (!condition) {
      console.error(`  FAIL: ${label}`)
      passed = false
    } else {
      console.log(`  PASS: ${label}`)
    }
  }

  console.log('\n--- Assertions ---')
  assert(totalProcessed === N, `all ${N} messages processed (got ${totalProcessed})`)
  assert(duplicates === 0, `no duplicates (got ${duplicates})`)
  assert(missing.length === 0, `no missing messages (missing ${missing.length})`)
  assert(minWorkerMessages >= 1, `each worker got at least 1 message (min was ${minWorkerMessages})`)

  // Verify queue is empty
  const db2 = new Database(DB_PATH)
  const q2 = new Queue<number>(db2, 'bench', { timeout: 30_000, maxReceive: 5 })
  const remaining = q2.size()
  db2.close()
  assert(remaining === 0, `queue is empty after test (remaining: ${remaining})`)

  console.log('')
  if (passed) {
    console.log('All assertions passed.')
  } else {
    console.error('Some assertions FAILED.')
    process.exitCode = 1
  }

  cleanup()
}

function cleanup() {
  for (const suffix of ['', '-wal', '-shm']) {
    const f = DB_PATH + suffix
    if (existsSync(f)) {
      try { unlinkSync(f) } catch { /* ignore */ }
    }
  }
}

main().catch((err) => {
  console.error('[main] Fatal error:', err)
  cleanup()
  process.exit(1)
})
