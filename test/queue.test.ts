import { describe, it, expect, beforeEach } from 'vitest'
import Database from 'better-sqlite3'
import { Queue } from '../src/index.js'

function createDB() {
  return new Database(':memory:')
}

describe('Queue', () => {
  let db: ReturnType<typeof Database>
  beforeEach(() => { db = createDB() })

  describe('constructor', () => {
    it('creates the sqliteq table (idempotent across queues)', () => {
      new Queue(db, 'queue-a')
      new Queue(db, 'queue-b')
      const tables = db.prepare("select name from sqlite_master where type='table' and name='sqliteq'").all()
      expect(tables).toHaveLength(1)
    })

    it('sets WAL mode on file-based DB', () => {
      const path = `/tmp/sqliteq-test-${Date.now()}.db`
      const fileDb = new Database(path)
      try {
        new Queue(fileDb, 'test')
        const row = fileDb.prepare('pragma journal_mode').get() as { journal_mode: string }
        expect(row.journal_mode).toBe('wal')
      } finally {
        fileDb.close()
        require('fs').unlinkSync(path)
        try { require('fs').unlinkSync(path + '-wal') } catch {}
        try { require('fs').unlinkSync(path + '-shm') } catch {}
      }
    })

    it('validates constructor arguments', () => {
      expect(() => new Queue(null as never, 'test')).toThrow('db is required')
      expect(() => new Queue(db, '')).toThrow('name is required')
      expect(() => new Queue(db, 'x'.repeat(256))).toThrow('name too long')
      expect(() => new Queue(db, 'test', { timeout: -1 })).toThrow('timeout cannot be negative')
      expect(() => new Queue(db, 'test', { maxReceive: 0 })).toThrow('maxReceive must be at least 1')

      const q = new Queue(db, 'test', { timeout: 0 })
      expect(q.timeout).toBe(0)
    })
  })

  describe('send', () => {
    it('inserts a message and returns an ID', () => {
      const q = new Queue(db, 'test')
      const id = q.send('hello')
      expect(id).toMatch(/^m_[0-9a-f]{32}$/)
    })

    it('stores the body correctly', () => {
      const q = new Queue(db, 'test')
      q.send('hello world')
      expect(q.receive()!.body).toBe('hello world')
    })

    it('validates send arguments', () => {
      const q = new Queue(db, 'test', { maxBodyBytes: 100 })
      expect(() => q.send(undefined as never)).toThrow('body cannot be undefined')
      expect(() => q.send('hi', { delay: -1 })).toThrow('delay cannot be negative')
      expect(() => q.send('hi', { priority: NaN })).toThrow('priority must be a finite number')
      expect(() => q.send('hi', { priority: Infinity })).toThrow('priority must be a finite number')
      expect(() => q.send('x'.repeat(200))).toThrow('body exceeds max size')
    })

    it('supports delayed messages', async () => {
      const q = new Queue(db, 'test', { timeout: 100 })
      q.send('delayed', { delay: 200 })
      expect(q.receive()).toBeNull()
      await new Promise((r) => setTimeout(r, 250))
      expect(q.receive()!.body).toBe('delayed')
    })

    it('respects priority ordering (higher first, negative below default)', () => {
      const q = new Queue(db, 'test')
      q.send('negative', { priority: -5 })
      q.send('low', { priority: 1 })
      q.send('default')
      q.send('high', { priority: 10 })

      expect(q.receive()!.body).toBe('high')
      expect(q.receive()!.body).toBe('low')
      expect(q.receive()!.body).toBe('default')
      expect(q.receive()!.body).toBe('negative')
    })

    it('sendBatch: sends atomically with correct priority', () => {
      const q = new Queue(db, 'test')
      const ids = q.sendBatch([
        { body: 'low', options: { priority: 1 } },
        { body: 'high', options: { priority: 10 } },
        { body: 'mid' },
      ])
      expect(ids).toHaveLength(3)
      expect(ids.every((id) => id.startsWith('m_'))).toBe(true)
      expect(q.size()).toBe(3)
      expect(q.receive()!.body).toBe('high')
    })

    it('sendBatch: rolls back entire batch on partial failure', () => {
      const q = new Queue(db, 'test', { maxBodyBytes: 50 })
      expect(() =>
        q.sendBatch([{ body: 'ok' }, { body: 'x'.repeat(200) }])
      ).toThrow('body exceeds max size')
      expect(q.size()).toBe(0)
    })
  })

  describe('receive', () => {
    it('returns null when queue is empty', () => {
      const q = new Queue(db, 'test')
      expect(q.receive()).toBeNull()
    })

    it('does not return the same message twice (visibility timeout)', () => {
      const q = new Queue(db, 'test', { timeout: 60_000 })
      q.send('hello')
      q.receive()
      expect(q.receive()).toBeNull()
    })

    it('timeout:0 makes messages immediately re-receivable', () => {
      const q = new Queue(db, 'test', { timeout: 0 })
      q.send('hello')
      const msg1 = q.receive()!
      const msg2 = q.receive()!
      expect(msg2.id).toBe(msg1.id)
    })

    it('re-delivers after timeout expires', async () => {
      const q = new Queue(db, 'test', { timeout: 50 })
      q.send('hello')
      const msg1 = q.receive()!
      expect(msg1.received).toBe(1)
      await new Promise((r) => setTimeout(r, 100))
      const msg2 = q.receive()!
      expect(msg2.id).toBe(msg1.id)
      expect(msg2.received).toBe(2)
    })

    it('stops delivering after maxReceive', async () => {
      const q = new Queue(db, 'test', { timeout: 10, maxReceive: 2 })
      q.send('hello')
      expect(q.receive()).not.toBeNull()
      await new Promise((r) => setTimeout(r, 20))
      expect(q.receive()).not.toBeNull()
      await new Promise((r) => setTimeout(r, 20))
      expect(q.receive()).toBeNull()
    })

    it('does not return messages from a different queue', () => {
      const q1 = new Queue(db, 'queue-1')
      const q2 = new Queue(db, 'queue-2')
      q1.send('for q1')
      expect(q2.receive()).toBeNull()
      expect(q1.receive()!.body).toBe('for q1')
    })

    it('returns oldest message first (FIFO within same priority)', () => {
      const q = new Queue(db, 'test')
      q.send('first')
      q.send('second')
      q.send('third')
      expect(q.receive()!.body).toBe('first')
      expect(q.receive()!.body).toBe('second')
      expect(q.receive()!.body).toBe('third')
    })

    it('throws with context on corrupt body', () => {
      const q = new Queue(db, 'test')
      db.prepare("insert into sqliteq (queue, body, timeout, priority) values ('test', 'not-json{', datetime('now'), 0)").run()
      expect(() => q.receive()).toThrow('Failed to parse body for message')
    })
  })

  describe('receiveBatch', () => {
    it('returns up to limit messages in queue order', () => {
      const q = new Queue(db, 'test')
      q.send('low', { priority: 1 })
      q.send('high', { priority: 10 })
      q.send('mid')

      const batch = q.receiveBatch(2)
      expect(batch.map((msg) => msg.body)).toEqual(['high', 'low'])
      expect(q.receive()!.body).toBe('mid')
    })

    it('returns fewer messages when the queue runs dry', () => {
      const q = new Queue(db, 'test')
      q.send('a')
      q.send('b')

      const batch = q.receiveBatch(5)
      expect(batch).toHaveLength(2)
      expect(batch.map((msg) => msg.body)).toEqual(['a', 'b'])
    })

    it('makes claimed messages invisible until timeout expires', () => {
      const q = new Queue(db, 'test', { timeout: 60_000 })
      q.send('a')
      q.send('b')

      const batch = q.receiveBatch(2)
      expect(batch).toHaveLength(2)
      expect(q.receive()).toBeNull()
    })

    it('validates limit', () => {
      const q = new Queue(db, 'test')
      expect(() => q.receiveBatch(0)).toThrow('limit must be an integer >= 1')
      expect(() => q.receiveBatch(1.5)).toThrow('limit must be an integer >= 1')
      expect(() => q.receiveBatch(Number.POSITIVE_INFINITY)).toThrow('limit must be an integer >= 1')
    })
  })

  describe('extend', () => {
    it('prevents redelivery while extended', async () => {
      const q = new Queue(db, 'test', { timeout: 50 })
      q.send('hello')
      const msg = q.receive()!
      q.extend(msg.id, msg.received, 1_000)
      await new Promise((r) => setTimeout(r, 100))
      expect(q.receive()).toBeNull()
    })

    it('returns true when message exists', () => {
      const q = new Queue(db, 'test', { timeout: 60_000 })
      q.send('hello')
      const msg = q.receive()!
      expect(q.extend(msg.id, msg.received, 1_000)).toBe(true)
    })

    it('returns false when message is not owned (wrong id, wrong queue, stale received)', async () => {
      const q1 = new Queue(db, 'queue-a', { timeout: 10 })
      const q2 = new Queue(db, 'queue-b')
      q1.send('hello')
      const msg1 = q1.receive()!

      expect(q1.extend('m_nonexistent_id_1234567890ab', 1, 1_000)).toBe(false)
      expect(q2.extend(msg1.id, msg1.received, 1_000)).toBe(false)

      await new Promise((r) => setTimeout(r, 20))
      const msg2 = q1.receive()!
      expect(q1.extend(msg1.id, msg1.received, 1_000)).toBe(false)
      expect(q1.extend(msg2.id, msg2.received, 1_000)).toBe(true)
    })

    it('extend with delay=0 makes message re-receivable', () => {
      const q = new Queue(db, 'test', { timeout: 60_000 })
      q.send('hello')
      const msg = q.receive()!
      q.extend(msg.id, msg.received, 0)
      const msg2 = q.receive()
      expect(msg2).not.toBeNull()
      expect(msg2!.id).toBe(msg.id)
    })

    it('throws on negative delay', () => {
      const q = new Queue(db, 'test')
      expect(() => q.extend('x', 1, -1)).toThrow('delay cannot be negative')
    })
  })

  describe('delete', () => {
    it('removes the message permanently', async () => {
      const q = new Queue(db, 'test', { timeout: 10 })
      q.send('hello')
      const msg = q.receive()!
      expect(q.delete(msg.id, msg.received)).toBe(true)
      await new Promise((r) => setTimeout(r, 50))
      expect(q.receive()).toBeNull()
    })

    it('returns false for stale delete (fencing prevents message loss)', async () => {
      const q = new Queue(db, 'test', { timeout: 10 })
      q.send('hello')
      const msg1 = q.receive()!
      await new Promise((r) => setTimeout(r, 20))
      const msg2 = q.receive()!
      expect(q.delete(msg1.id, msg1.received)).toBe(false)
      expect(q.size()).toBe(1)
      expect(q.delete(msg2.id, msg2.received)).toBe(true)
      expect(q.size()).toBe(0)
    })

    it('is safe on double-delete, nonexistent ID, and wrong queue', async () => {
      const q1 = new Queue(db, 'queue-a', { timeout: 10 })
      const q2 = new Queue(db, 'queue-b')
      q1.send('hello')
      const msg = q1.receive()!

      // Wrong queue — no effect
      expect(q2.delete(msg.id, msg.received)).toBe(false)

      // Wait for timeout, re-receive with new received count
      await new Promise((r) => setTimeout(r, 20))
      const msg2 = q1.receive()!
      expect(msg2.received).toBe(2)

      // Delete, double-delete, nonexistent
      expect(q1.delete(msg2.id, msg2.received)).toBe(true)
      expect(q1.delete(msg2.id, msg2.received)).toBe(false)
      expect(q1.delete('m_does_not_exist_at_all_1234', 1)).toBe(false)
    })
  })

  describe('size + purge', () => {
    it('size: counts all messages in this queue regardless of state', () => {
      const q1 = new Queue(db, 'queue-a', { timeout: 60_000 })
      const q2 = new Queue(db, 'queue-b')

      expect(q1.size()).toBe(0)
      q1.send('a')
      q1.send('b')
      q2.send('c')
      expect(q1.size()).toBe(2)
      expect(q2.size()).toBe(1)

      q1.receive()
      expect(q1.size()).toBe(2) // claimed, not deleted
    })

    it('purge: removes all messages from this queue only', () => {
      const q1 = new Queue(db, 'queue-a')
      const q2 = new Queue(db, 'queue-b')
      q1.send('a')
      q1.send('b')
      q1.send('c')
      q2.send('d')

      expect(q1.purge()).toBe(3)
      expect(q1.size()).toBe(0)
      expect(q2.size()).toBe(1)
    })
  })

  describe('stats', () => {
    it('reports ready, delayed, in-flight, and dead counts', async () => {
      const q = new Queue(db, 'test', { timeout: 20, maxReceive: 1 })

      q.send('inflight')
      const inflight = q.receive()!
      expect(q.extend(inflight.id, inflight.received, 1_000)).toBe(true)

      q.send('dead')
      const dead = q.receive()!
      expect(dead.body).toBe('dead')

      await new Promise((r) => setTimeout(r, 30))

      q.send('ready')
      q.send('delayed', { delay: 100 })

      expect(q.stats()).toEqual({
        total: 4,
        ready: 1,
        delayed: 1,
        inFlight: 1,
        dead: 1,
      })
    })

    it('returns zeroes for an empty queue', () => {
      const q = new Queue(db, 'test')
      expect(q.stats()).toEqual({
        total: 0,
        ready: 0,
        delayed: 0,
        inFlight: 0,
        dead: 0,
      })
    })
  })

  describe('deadLetters', () => {
    it('returns messages that exceeded maxReceive (and only those)', async () => {
      const q = new Queue(db, 'test', { timeout: 10, maxReceive: 1 })
      q.send('will-die')
      q.send('will-live')

      expect(q.deadLetters()).toHaveLength(0)

      q.receive() // claims will-die (received=1, dead after timeout)
      await new Promise((r) => setTimeout(r, 20))

      const dead = q.deadLetters()
      expect(dead).toHaveLength(1)
      expect(dead[0].body).toBe('will-die')
      expect(dead[0].received).toBe(1)
    })

    it('requeues dead letters as fresh messages with new IDs', async () => {
      const q = new Queue(db, 'test', { timeout: 10, maxReceive: 1 })
      q.send('will-die')

      const old = q.receive()!
      await new Promise((r) => setTimeout(r, 20))

      const ids = q.requeueDeadLetters()
      expect(ids).toHaveLength(1)
      expect(ids[0]).not.toBe(old.id)
      expect(q.deadLetters()).toHaveLength(0)

      const retried = q.receive()!
      expect(retried.id).toBe(ids[0])
      expect(retried.body).toBe('will-die')
      expect(retried.received).toBe(1)
      expect(q.delete(old.id, old.received)).toBe(false)
    })

    it('supports delayed requeue', async () => {
      const q = new Queue(db, 'test', { timeout: 10, maxReceive: 1 })
      q.send('later')
      q.receive()
      await new Promise((r) => setTimeout(r, 20))

      expect(q.requeueDeadLetters({ delay: 50 })).toHaveLength(1)
      expect(q.receive()).toBeNull()
      await new Promise((r) => setTimeout(r, 70))
      expect(q.receive()!.body).toBe('later')
    })

    it('purges dead letters only', async () => {
      const q = new Queue(db, 'test', { timeout: 10, maxReceive: 1 })
      q.send('dead')
      q.send('ready')
      q.send('delayed', { delay: 100 })

      q.receive()
      await new Promise((r) => setTimeout(r, 20))

      expect(q.purgeDeadLetters()).toBe(1)
      expect(q.stats()).toEqual({
        total: 2,
        ready: 1,
        delayed: 1,
        inFlight: 0,
        dead: 0,
      })
      expect(q.receive()!.body).toBe('ready')
    })
  })

  describe('JSON serialization', () => {
    it('round-trips typed objects', () => {
      interface Job { task: string; count: number }
      const q = new Queue<Job>(db, 'typed')
      q.send({ task: 'email', count: 42 })
      expect(q.receive()!.body).toEqual({ task: 'email', count: 42 })
    })

    it('round-trips edge-case string payloads without type corruption', () => {
      const q = new Queue<string>(db, 'strings')
      for (const input of ['42', 'true', '{"a":1}', '', '你好世界 🚀']) {
        q.send(input)
        const msg = q.receive()!
        expect(msg.body).toBe(input)
        expect(typeof msg.body).toBe('string')
        q.delete(msg.id, msg.received)
      }
    })

    it('round-trips arrays, null, and deeply nested objects', () => {
      const qArr = new Queue<number[]>(db, 'arrays')
      qArr.send([1, 2, 3])
      expect(qArr.receive()!.body).toEqual([1, 2, 3])

      const qNull = new Queue<null>(db, 'nulls')
      qNull.send(null)
      expect(qNull.receive()!.body).toBeNull()

      const qDeep = new Queue<{ a: { b: { c: number } } }>(db, 'deep')
      qDeep.send({ a: { b: { c: 42 } } })
      expect(qDeep.receive()!.body).toEqual({ a: { b: { c: 42 } } })
    })
  })
})
