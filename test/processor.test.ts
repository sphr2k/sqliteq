import { describe, it, expect, beforeEach, vi } from 'vitest'
import Database from 'better-sqlite3'
import { Queue, Processor } from '../src/index.js'

function createDB() {
  return new Database(':memory:')
}

function wait(ms: number) {
  return new Promise((r) => setTimeout(r, ms))
}

describe('Processor', () => {
  let db: ReturnType<typeof Database>
  beforeEach(() => { db = createDB() })

  describe('constructor', () => {
    it('validates constructor options', () => {
      const q = new Queue(db, 'test')
      expect(() => new Processor(q, { handler: () => {}, concurrency: 0 })).toThrow('concurrency')
      expect(() => new Processor(q, { handler: () => {}, pollInterval: 0 })).toThrow('pollInterval')
      expect(() => new Processor(q, { handler: () => {}, extendInterval: NaN })).toThrow('extendInterval')
      expect(() => new Processor(q, { handler: () => {}, extendInterval: -1 })).toThrow('extendInterval')
    })
  })

  describe('basic processing', () => {
    it('processes messages and auto-deletes on success', async () => {
      const q = new Queue(db, 'test', { timeout: 1_000 })
      q.send('hello')

      const processed: string[] = []
      const p = new Processor(q, {
        handler: (msg) => { processed.push(msg.body) },
        pollInterval: 10,
        onError: () => {},
      })

      p.start()
      await wait(100)
      await p.stop()

      expect(processed).toEqual(['hello'])
      expect(q.size()).toBe(0)
    })

    it('processes multiple messages in order', async () => {
      const q = new Queue(db, 'test')
      q.send('a')
      q.send('b')
      q.send('c')

      const order: string[] = []
      const p = new Processor(q, {
        handler: (msg) => { order.push(msg.body) },
        pollInterval: 10,
        onError: () => {},
      })

      p.start()
      await wait(200)
      await p.stop()
      expect(order).toEqual(['a', 'b', 'c'])
    })

    it('picks up messages sent after start', async () => {
      const q = new Queue(db, 'test', { timeout: 1_000 })
      const processed: string[] = []

      const p = new Processor(q, {
        handler: (msg) => { processed.push(msg.body) },
        pollInterval: 10,
        onError: () => {},
      })

      p.start()
      await wait(50)
      q.send('late')
      await wait(100)
      await p.stop()
      expect(processed).toContain('late')
    })

    it('idles without error on empty queue', async () => {
      const q = new Queue(db, 'test')
      const errors: unknown[] = []

      const p = new Processor(q, {
        handler: () => {},
        pollInterval: 10,
        onError: (err) => { errors.push(err) },
      })

      p.start()
      await wait(100)
      await p.stop()
      expect(errors).toHaveLength(0)
    })
  })

  describe('error handling', () => {
    it('leaves message for retry on handler throw', async () => {
      const q = new Queue(db, 'test', { timeout: 50, maxReceive: 5 })
      q.send('important')

      let callCount = 0
      const p = new Processor(q, {
        handler: () => { callCount++; throw new Error('fail') },
        pollInterval: 10,
        onError: () => {},
      })

      p.start()
      await wait(200)
      await p.stop()

      expect(callCount).toBeGreaterThan(1)
      expect(q.size()).toBe(1)
    })

    it('reports errors with correct phase context (handler, delete, extend)', async () => {
      const phases: string[] = []
      const onError = (_err: unknown, ctx: { phase: string }) => { phases.push(ctx.phase) }

      // handler phase
      const q1 = new Queue(db, 'test-h', { timeout: 1_000 })
      q1.send('boom')
      const p1 = new Processor(q1, {
        handler: () => { throw new Error('fail') },
        pollInterval: 10, onError,
      })
      p1.start(); await wait(100); await p1.stop()

      // delete phase
      const q2 = new Queue(db, 'test-d', { timeout: 1_000 })
      q2.send('hello')
      const origDelete = q2.delete.bind(q2)
      q2.delete = () => { throw new Error('delete failed') }
      const p2 = new Processor(q2, {
        handler: () => {},
        pollInterval: 10, onError,
      })
      p2.start(); await wait(100); await p2.stop()
      q2.delete = origDelete

      // extend phase
      const q3 = new Queue(db, 'test-e', { timeout: 1_000 })
      q3.send('hello')
      const origExtend = q3.extend.bind(q3)
      q3.extend = () => { throw new Error('extend failed') }
      const p3 = new Processor(q3, {
        handler: async () => { await wait(200) },
        pollInterval: 10, extendInterval: 50, onError,
      })
      p3.start(); await wait(300); await p3.stop()
      q3.extend = origExtend

      expect(phases).toContain('handler')
      expect(phases).toContain('delete')
      expect(phases).toContain('extend')
    })

    it('survives both sync and async handler failures', async () => {
      const q = new Queue(db, 'test', { timeout: 50 })
      q.send('sync-crash')
      q.send('async-crash')
      q.send('ok')

      const processed: string[] = []
      const errors: unknown[] = []

      const p = new Processor(q, {
        handler: async (msg) => {
          if (msg.body === 'sync-crash' && msg.received === 1) throw new Error('sync boom')
          if (msg.body === 'async-crash' && msg.received === 1) { await wait(1); throw new Error('async boom') }
          processed.push(msg.body)
        },
        pollInterval: 10,
        onError: (err) => { errors.push(err) },
      })

      p.start()
      await wait(300)
      await p.stop()

      expect(errors.length).toBeGreaterThanOrEqual(2)
      expect(processed).toContain('ok')
    })

    it('defaults to console.error when onError not provided', async () => {
      const spy = vi.spyOn(console, 'error').mockImplementation(() => {})
      const q = new Queue(db, 'test', { timeout: 1_000 })
      q.send('crash')

      const p = new Processor(q, {
        handler: () => { throw new Error('boom') },
        pollInterval: 10,
      })

      p.start()
      await wait(100)
      await p.stop()

      expect(spy).toHaveBeenCalled()
      expect(spy.mock.calls[0][0]).toContain('[sqliteq]')
      spy.mockRestore()
    })

    it('onError throwing does not crash processor or hang stop()', async () => {
      const q = new Queue(db, 'test', { timeout: 50 })
      q.send('a')
      q.send('b')

      const processed: string[] = []
      const p = new Processor(q, {
        handler: (msg) => {
          if (msg.body === 'a' && msg.received === 1) throw new Error('fail')
          processed.push(msg.body)
        },
        pollInterval: 10,
        onError: () => { throw new Error('onError itself throws!') },
      })

      p.start()
      await wait(200)
      await p.stop()
      expect(processed).toContain('b')
    })

    it('receive exception in poll does not crash processor', async () => {
      const q = new Queue(db, 'test', { timeout: 1_000 })
      q.send('before')

      const processed: string[] = []
      let callCount = 0
      const origReceive = q.receive.bind(q)
      q.receive = () => {
        callCount++
        if (callCount === 2) throw new Error('db error')
        return origReceive()
      }

      const errors: string[] = []
      const p = new Processor(q, {
        handler: (msg) => { processed.push(msg.body) },
        pollInterval: 10,
        onError: (_err, ctx) => { errors.push(ctx.phase) },
      })

      p.start()
      await wait(200)
      await p.stop()
      q.receive = origReceive

      expect(processed).toContain('before')
      expect(errors).toContain('receive')
    })
  })

  describe('auto-extend', () => {
    it('actually calls extend during long processing', async () => {
      const q = new Queue(db, 'test', { timeout: 100, maxReceive: 1 })
      q.send('long')

      let extendCalls = 0
      const origExtend = q.extend.bind(q)
      q.extend = (id: string, received: number, delay: number) => {
        extendCalls++
        return origExtend(id, received, delay)
      }

      const p = new Processor(q, {
        handler: async () => { await wait(300) },
        pollInterval: 10,
        extendInterval: 100,
        onError: () => {},
      })

      p.start()
      await wait(400)
      await p.stop()
      q.extend = origExtend

      expect(extendCalls).toBeGreaterThanOrEqual(2)
    })

    it('stops extending after handler completes', async () => {
      const q = new Queue(db, 'test', { timeout: 200 })
      q.send('medium-task')

      let extendCalls = 0
      const origExtend = q.extend.bind(q)
      q.extend = (id: string, received: number, delay: number) => {
        extendCalls++
        return origExtend(id, received, delay)
      }

      const p = new Processor(q, {
        handler: async () => { await wait(80) },
        pollInterval: 10,
        extendInterval: 50,
        onError: () => {},
      })

      p.start()
      await wait(200)
      await p.stop()
      expect(extendCalls).toBeGreaterThanOrEqual(1)
      const countAtStop = extendCalls
      await wait(200)
      q.extend = origExtend
      expect(extendCalls).toBe(countAtStop)
    })

    it('keeps message alive for handler longer than timeout', async () => {
      const q = new Queue(db, 'test', { timeout: 100, maxReceive: 1 })
      q.send('long-task')

      const p = new Processor(q, {
        handler: async () => { await wait(300) },
        pollInterval: 10,
        extendInterval: 100,
        onError: () => {},
      })

      p.start()
      await wait(400)
      await p.stop()
      expect(q.size()).toBe(0)
    })
  })

  describe('concurrency', () => {
    it('concurrency=1 processes one at a time', async () => {
      const q = new Queue(db, 'test', { timeout: 5_000 })
      q.send('a')
      q.send('b')
      q.send('c')

      let maxConcurrent = 0
      let current = 0

      const p = new Processor(q, {
        handler: async () => {
          current++
          maxConcurrent = Math.max(maxConcurrent, current)
          await wait(50)
          current--
        },
        pollInterval: 10,
        concurrency: 1,
        onError: () => {},
      })

      p.start()
      await wait(500)
      await p.stop()
      expect(maxConcurrent).toBe(1)
    })

    it('respects concurrency limits', async () => {
      const q = new Queue(db, 'test', { timeout: 5_000 })
      for (let i = 0; i < 10; i++) q.send(`msg-${i}`)

      let maxConcurrent = 0
      let current = 0

      const p = new Processor(q, {
        handler: async () => {
          current++
          maxConcurrent = Math.max(maxConcurrent, current)
          await wait(50)
          current--
        },
        pollInterval: 10,
        concurrency: 3,
        onError: () => {},
      })

      p.start()
      await wait(800)
      await p.stop()
      expect(maxConcurrent).toBeGreaterThanOrEqual(2)
      expect(maxConcurrent).toBeLessThanOrEqual(3)
    })
  })

  describe('start/stop lifecycle', () => {
    it('graceful shutdown waits for in-flight jobs', async () => {
      const q = new Queue(db, 'test', { timeout: 5_000 })
      q.send('slow')

      let finished = false
      const p = new Processor(q, {
        handler: async () => { await wait(200); finished = true },
        pollInterval: 10,
        onError: () => {},
      })

      p.start()
      await wait(50)
      await p.stop()
      expect(finished).toBe(true)
    })

    it('start() is idempotent and blocked during stop', async () => {
      const q = new Queue(db, 'test', { timeout: 5_000 })
      q.send('msg')

      let callCount = 0
      const p = new Processor(q, {
        handler: async () => { callCount++; await wait(200) },
        pollInterval: 10,
        onError: () => {},
      })

      p.start()
      p.start() // no-op
      await wait(50)
      const stopPromise = p.stop()
      p.start() // no-op while stopping
      await stopPromise
      expect(callCount).toBe(1)

      q.send('after')
      p.start()
      await wait(100)
      await p.stop()
      expect(callCount).toBe(2)
    })

    it('stop() resolves immediately when not started', async () => {
      const q = new Queue(db, 'test')
      const p = new Processor(q, { handler: () => {}, pollInterval: 10, onError: () => {} })
      const before = Date.now()
      await p.stop()
      expect(Date.now() - before).toBeLessThan(50)
    })

    it('can restart after stop', async () => {
      const q = new Queue(db, 'test', { timeout: 1_000 })
      const processed: string[] = []

      const p = new Processor(q, {
        handler: (msg) => { processed.push(msg.body) },
        pollInterval: 10,
        onError: () => {},
      })

      q.send('first')
      p.start()
      await wait(100)
      await p.stop()

      q.send('second')
      p.start()
      await wait(100)
      await p.stop()

      expect(processed).toEqual(['first', 'second'])
    })
  })
})
