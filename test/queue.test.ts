import { describe, it, expect, beforeEach } from 'vitest'
import Database from 'better-sqlite3'
import { Queue, BetterSqlite3Driver } from '../src/index.js'
import { unlinkSync, existsSync } from 'node:fs'

function createDB() {
  return new Database(':memory:')
}

describe('Queue', () => {
  let db: Database.Database
  beforeEach(() => { db = createDB() })

  describe('constructor', () => {
    it('creates the sqliteq table (idempotent across queues)', async () => {
      const q1 = new Queue(new BetterSqlite3Driver(db), 'queue-a')
      await q1.init()
      const q2 = new Queue(new BetterSqlite3Driver(db), 'queue-b')
      await q2.init()
      const tables = db.prepare("select name from sqlite_master where type='table' and name='sqliteq'").all()
      expect(tables).toHaveLength(1)
    })

    it('sets WAL mode on file-based DB', async () => {
      const path = `./sqliteq-test-${Date.now()}.db`
      const fileDb = new Database(path)
      try {
        const q = new Queue(new BetterSqlite3Driver(fileDb), 'test')
        await q.init()
        const row = fileDb.prepare('pragma journal_mode').get() as { journal_mode: string }
        expect(row.journal_mode).toBe('wal')
      } finally {
        fileDb.close()
        if (existsSync(path)) unlinkSync(path)
        if (existsSync(path + '-wal')) unlinkSync(path + '-wal')
        if (existsSync(path + '-shm')) unlinkSync(path + '-shm')
      }
    })

    it('validates constructor arguments', () => {
      expect(() => new Queue(null as never, 'test')).toThrow('driver is required')
      expect(() => new Queue(new BetterSqlite3Driver(db), '')).toThrow('name is required')
      expect(() => new Queue(new BetterSqlite3Driver(db), 'x'.repeat(256))).toThrow('name too long')
      expect(() => new Queue(new BetterSqlite3Driver(db), 'test', { timeout: -1 })).toThrow('timeout cannot be negative')
      expect(() => new Queue(new BetterSqlite3Driver(db), 'test', { maxReceive: 0 })).toThrow('maxReceive must be at least 1')

      const q = new Queue(new BetterSqlite3Driver(db), 'test', { timeout: 0 })
      expect(q.timeout).toBe(0)
    })
  })

  describe('send', () => {
    it('inserts a message and returns an ID', async () => {
      const q = new Queue(new BetterSqlite3Driver(db), 'test')
      await q.init()
      const id = await q.send('hello')
      expect(id).toMatch(/^m_[0-9a-f]{32}$/)
    })

    it('stores the body correctly', async () => {
      const q = new Queue(new BetterSqlite3Driver(db), 'test')
      await q.init()
      await q.send('hello world')
      const msg = await q.receive()
      expect(msg!.body).toBe('hello world')
    })

    it('validates send arguments', async () => {
      const q = new Queue(new BetterSqlite3Driver(db), 'test', { maxBodyBytes: 100 })
      await q.init()
      await expect(q.send(undefined as never)).rejects.toThrow('body cannot be undefined')
      await expect(q.send('hi', { delay: -1 })).rejects.toThrow('delay cannot be negative')
      await expect(q.send('hi', { priority: NaN })).rejects.toThrow('priority must be a finite number')
      await expect(q.send('hi', { priority: Infinity })).rejects.toThrow('priority must be a finite number')
      await expect(q.send('x'.repeat(200))).rejects.toThrow('body exceeds max size')
    })

    it('supports delayed messages', async () => {
      const q = new Queue(new BetterSqlite3Driver(db), 'test', { timeout: 100 })
      await q.init()
      await q.send('delayed', { delay: 200 })
      expect(await q.receive()).toBeNull()
      await new Promise((r) => setTimeout(r, 250))
      const msg = await q.receive()
      expect(msg!.body).toBe('delayed')
    })

    it('respects priority ordering (higher first, negative below default)', async () => {
      const q = new Queue(new BetterSqlite3Driver(db), 'test')
      await q.init()
      await q.send('negative', { priority: -5 })
      await q.send('low', { priority: 1 })
      await q.send('default')
      await q.send('high', { priority: 10 })

      expect((await q.receive())!.body).toBe('high')
      expect((await q.receive())!.body).toBe('low')
      expect((await q.receive())!.body).toBe('default')
      expect((await q.receive())!.body).toBe('negative')
    })

    it('sendBatch: sends atomically with correct priority', async () => {
      const q = new Queue(new BetterSqlite3Driver(db), 'test')
      await q.init()
      const ids = await q.sendBatch([
        { body: 'low', options: { priority: 1 } },
        { body: 'high', options: { priority: 10 } },
        { body: 'mid' },
      ])
      expect(ids).toHaveLength(3)
      expect(ids.every((id) => id.startsWith('m_'))).toBe(true)
      expect(await q.size()).toBe(3)
      expect((await q.receive())!.body).toBe('high')
    })

    it('sendBatch: rolls back entire batch on partial failure', async () => {
      const q = new Queue(new BetterSqlite3Driver(db), 'test', { maxBodyBytes: 50 })
      await q.init()
      await expect(
        q.sendBatch([{ body: 'ok' }, { body: 'x'.repeat(200) }])
      ).rejects.toThrow('body exceeds max size')
      expect(await q.size()).toBe(0)
    })
  })

  describe('receive', () => {
    it('returns null when queue is empty', async () => {
      const q = new Queue(new BetterSqlite3Driver(db), 'test')
      await q.init()
      expect(await q.receive()).toBeNull()
    })

    it('does not return the same message twice (visibility timeout)', async () => {
      const q = new Queue(new BetterSqlite3Driver(db), 'test', { timeout: 60_000 })
      await q.init()
      await q.send('hello')
      await q.receive()
      expect(await q.receive()).toBeNull()
    })

    it('timeout:0 makes messages immediately re-receivable', async () => {
      const q = new Queue(new BetterSqlite3Driver(db), 'test', { timeout: 0 })
      await q.init()
      await q.send('hello')
      const msg1 = await q.receive()
      const msg2 = await q.receive()
      expect(msg2!.id).toBe(msg1!.id)
    })

    it('re-delivers after timeout expires', async () => {
      const q = new Queue(new BetterSqlite3Driver(db), 'test', { timeout: 50 })
      await q.init()
      await q.send('hello')
      const msg1 = await q.receive()
      expect(msg1!.received).toBe(1)
      await new Promise((r) => setTimeout(r, 100))
      const msg2 = await q.receive()
      expect(msg2!.id).toBe(msg1!.id)
      expect(msg2!.received).toBe(2)
    })

    it('stops delivering after maxReceive', async () => {
      const q = new Queue(new BetterSqlite3Driver(db), 'test', { timeout: 10, maxReceive: 2 })
      await q.init()
      await q.send('hello')
      expect(await q.receive()).not.toBeNull()
      await new Promise((r) => setTimeout(r, 20))
      expect(await q.receive()).not.toBeNull()
      await new Promise((r) => setTimeout(r, 20))
      expect(await q.receive()).toBeNull()
    })

    it('does not return messages from a different queue', async () => {
      const q1 = new Queue(new BetterSqlite3Driver(db), 'queue-1')
      await q1.init()
      const q2 = new Queue(new BetterSqlite3Driver(db), 'queue-2')
      await q2.init()
      await q1.send('for q1')
      expect(await q2.receive()).toBeNull()
      expect((await q1.receive())!.body).toBe('for q1')
    })

    it('returns oldest message first (FIFO within same priority)', async () => {
      const q = new Queue(new BetterSqlite3Driver(db), 'test')
      await q.init()
      await q.send('first')
      await q.send('second')
      await q.send('third')
      expect((await q.receive())!.body).toBe('first')
      expect((await q.receive())!.body).toBe('second')
      expect((await q.receive())!.body).toBe('third')
    })

    it('throws with context on corrupt body', async () => {
      const q = new Queue(new BetterSqlite3Driver(db), 'test')
      await q.init()
      db.prepare("insert into sqliteq (queue, body, timeout, priority) values ('test', 'not-json{', datetime('now'), 0)").run()
      await expect(q.receive()).rejects.toThrow('Failed to parse body for message')
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
      const q = new Queue(new BetterSqlite3Driver(db), 'test', { timeout: 50 })
      await q.init()
      await q.send('hello')
      const msg = await q.receive()
      await q.extend(msg!.id, msg!.received, 1_000)
      await new Promise((r) => setTimeout(r, 100))
      expect(await q.receive()).toBeNull()
    })

    it('returns true when message exists', async () => {
      const q = new Queue(new BetterSqlite3Driver(db), 'test', { timeout: 60_000 })
      await q.init()
      await q.send('hello')
      const msg = await q.receive()
      expect(await q.extend(msg!.id, msg!.received, 1_000)).toBe(true)
    })

    it('returns false when message is not owned (wrong id, wrong queue, stale received)', async () => {
      const q1 = new Queue(new BetterSqlite3Driver(db), 'queue-a', { timeout: 10 })
      await q1.init()
      const q2 = new Queue(new BetterSqlite3Driver(db), 'queue-b')
      await q2.init()
      await q1.send('hello')
      const msg1 = await q1.receive()

      expect(await q1.extend('m_nonexistent_id_1234567890ab', 1, 1_000)).toBe(false)
      expect(await q2.extend(msg1!.id, msg1!.received, 1_000)).toBe(false)

      await new Promise((r) => setTimeout(r, 20))
      const msg2 = await q1.receive()
      expect(await q1.extend(msg1!.id, msg1!.received, 1_000)).toBe(false)
      expect(await q1.extend(msg2!.id, msg2!.received, 1_000)).toBe(true)
    })

    it('extend with delay=0 makes message re-receivable', async () => {
      const q = new Queue(new BetterSqlite3Driver(db), 'test', { timeout: 60_000 })
      await q.init()
      await q.send('hello')
      const msg = await q.receive()
      await q.extend(msg!.id, msg!.received, 0)
      const msg2 = await q.receive()
      expect(msg2).not.toBeNull()
      expect(msg2!.id).toBe(msg!.id)
    })

    it('throws on negative delay', async () => {
      const q = new Queue(new BetterSqlite3Driver(db), 'test')
      await q.init()
      await expect(q.extend('x', 1, -1)).rejects.toThrow('delay cannot be negative')
    })
  })

  describe('delete', () => {
    it('removes the message permanently', async () => {
      const q = new Queue(new BetterSqlite3Driver(db), 'test', { timeout: 10 })
      await q.init()
      await q.send('hello')
      const msg = await q.receive()
      expect(await q.delete(msg!.id, msg!.received)).toBe(true)
      await new Promise((r) => setTimeout(r, 50))
      expect(await q.receive()).toBeNull()
    })

    it('returns false for stale delete (fencing prevents message loss)', async () => {
      const q = new Queue(new BetterSqlite3Driver(db), 'test', { timeout: 10 })
      await q.init()
      await q.send('hello')
      const msg1 = await q.receive()
      await new Promise((r) => setTimeout(r, 20))
      const msg2 = await q.receive()
      expect(await q.delete(msg1!.id, msg1!.received)).toBe(false)
      expect(await q.size()).toBe(1)
      expect(await q.delete(msg2!.id, msg2!.received)).toBe(true)
      expect(await q.size()).toBe(0)
    })

    it('is safe on double-delete, nonexistent ID, and wrong queue', async () => {
      const q1 = new Queue(new BetterSqlite3Driver(db), 'queue-a', { timeout: 10 })
      await q1.init()
      const q2 = new Queue(new BetterSqlite3Driver(db), 'queue-b')
      await q2.init()
      await q1.send('hello')
      const msg = await q1.receive()

      // Wrong queue — no effect
      expect(await q2.delete(msg!.id, msg!.received)).toBe(false)

      // Wait for timeout, re-receive with new received count
      await new Promise((r) => setTimeout(r, 20))
      const msg2 = await q1.receive()
      expect(msg2!.received).toBe(2)

      // Delete, double-delete, nonexistent
      expect(await q1.delete(msg2!.id, msg2!.received)).toBe(true)
      expect(await q1.delete(msg2!.id, msg2!.received)).toBe(false)
      expect(await q1.delete('m_does_not_exist_at_all_1234', 1)).toBe(false)
    })
  })

  describe('size + purge', () => {
    it('size: counts all messages in this queue regardless of state', async () => {
      const q1 = new Queue(new BetterSqlite3Driver(db), 'queue-a', { timeout: 60_000 })
      await q1.init()
      const q2 = new Queue(new BetterSqlite3Driver(db), 'queue-b')
      await q2.init()

      expect(await q1.size()).toBe(0)
      await q1.send('a')
      await q1.send('b')
      await q2.send('c')
      expect(await q1.size()).toBe(2)
      expect(await q2.size()).toBe(1)

      await q1.receive()
      expect(await q1.size()).toBe(2) // claimed, not deleted
    })

    it('purge: removes all messages from this queue only', async () => {
      const q1 = new Queue(new BetterSqlite3Driver(db), 'queue-a')
      await q1.init()
      const q2 = new Queue(new BetterSqlite3Driver(db), 'queue-b')
      await q2.init()
      await q1.send('a')
      await q1.send('b')
      await q1.send('c')
      await q2.send('d')

      expect(await q1.purge()).toBe(3)
      expect(await q1.size()).toBe(0)
      expect(await q2.size()).toBe(1)
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
      const q = new Queue(new BetterSqlite3Driver(db), 'test', { timeout: 10, maxReceive: 1 })
      await q.init()
      await q.send('will-die')
      await q.send('will-live')

      expect(await q.deadLetters()).toHaveLength(0)

      await q.receive() // claims will-die (received=1, dead after timeout)
      await new Promise((r) => setTimeout(r, 20))

      const dead = await q.deadLetters()
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
    it('round-trips typed objects', async () => {
      interface Job { task: string; count: number }
      const q = new Queue<Job>(new BetterSqlite3Driver(db), 'typed')
      await q.init()
      await q.send({ task: 'email', count: 42 })
      const msg = await q.receive()
      expect(msg!.body).toEqual({ task: 'email', count: 42 })
    })

    it('round-trips edge-case string payloads without type corruption', async () => {
      const q = new Queue<string>(new BetterSqlite3Driver(db), 'strings')
      await q.init()
      for (const input of ['42', 'true', '{"a":1}', '', '你好世界 🚀']) {
        await q.send(input)
        const msg = await q.receive()
        expect(msg!.body).toBe(input)
        expect(typeof msg!.body).toBe('string')
        await q.delete(msg!.id, msg!.received)
      }
    })

    it('round-trips arrays, null, and deeply nested objects', async () => {
      const qArr = new Queue<number[]>(new BetterSqlite3Driver(db), 'arrays')
      await qArr.init()
      await qArr.send([1, 2, 3])
      const msg1 = await qArr.receive()
      expect(msg1!.body).toEqual([1, 2, 3])

      const qNull = new Queue<null>(new BetterSqlite3Driver(db), 'nulls')
      await qNull.init()
      await qNull.send(null)
      const msg2 = await qNull.receive()
      expect(msg2!.body).toBeNull()

      const qDeep = new Queue<{ a: { b: { c: number } } }>(new BetterSqlite3Driver(db), 'deep')
      await qDeep.init()
      await qDeep.send({ a: { b: { c: 42 } } })
      const msg3 = await qDeep.receive()
      expect(msg3!.body).toEqual({ a: { b: { c: 42 } } })
    })
  })
})
