import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { createClient, type Client } from '@libsql/client'
import { Queue, LibsqlDriver } from '../src/index.js'
import { unlinkSync, existsSync } from 'node:fs'

describe('LibsqlDriver', () => {
  const dbPath = 'test-libsql.db'
  let client: Client
  let q: Queue<string>

  beforeEach(async () => {
    if (existsSync(dbPath)) unlinkSync(dbPath)
    client = createClient({ url: `file:${dbPath}` })
    q = new Queue(new LibsqlDriver(client), 'test-queue')
    await q.init()
  })

  afterEach(() => {
    client.close()
    if (existsSync(dbPath)) {
      try { unlinkSync(dbPath) } catch {}
    }
  })

  it('should send and receive a message', async () => {
    const id = await q.send('hello libsql')
    expect(id).toBeDefined()
    expect(id.startsWith('m_')).toBe(true)

    const msg = await q.receive()
    expect(msg).not.toBeNull()
    expect(msg?.body).toBe('hello libsql')
    expect(msg?.received).toBe(1)
  })

  it('should respect visibility timeout', async () => {
    const shortQ = new Queue(new LibsqlDriver(client), 'short-queue', { timeout: 100 })
    await shortQ.init()
    
    await shortQ.send('timed message')
    const msg1 = await shortQ.receive()
    expect(msg1).not.toBeNull()

    // Second receive should be null immediately
    const msg2 = await shortQ.receive()
    expect(msg2).toBeNull()

    // Wait for timeout
    await new Promise(r => setTimeout(r, 150))

    const msg3 = await shortQ.receive()
    expect(msg3).not.toBeNull()
    expect(msg3?.id).toBe(msg1?.id)
    expect(msg3?.received).toBe(2)
  })

  it('should delete a message', async () => {
    await q.send('to be deleted')
    const msg = await q.receive()
    expect(msg).not.toBeNull()

    const deleted = await q.delete(msg!.id, msg!.received)
    expect(deleted).toBe(true)

    const msg2 = await q.receive()
    expect(msg2).toBeNull()
  })

  it('should prevent stale deletes (fencing)', async () => {
    const shortQ = new Queue(new LibsqlDriver(client), 'fence-queue', { timeout: 50 })
    await shortQ.init()

    await shortQ.send('fenced message')
    const msg1 = await shortQ.receive()
    
    await new Promise(r => setTimeout(r, 100))
    const msg2 = await shortQ.receive()
    expect(msg2?.id).toBe(msg1?.id)

    // msg1 is now stale
    const deleted1 = await shortQ.delete(msg1!.id, msg1!.received)
    expect(deleted1).toBe(false)

    const deleted2 = await shortQ.delete(msg2!.id, msg2!.received)
    expect(deleted2).toBe(true)
  })

  it('should return queue size', async () => {
    await q.send('msg1')
    await q.send('msg2')
    expect(await q.size()).toBe(2)
  })

  it('should purge the queue', async () => {
    await q.send('msg1')
    await q.send('msg2')
    const count = await q.purge()
    expect(count).toBe(2)
    expect(await q.size()).toBe(0)
  })
})
