import { beforeEach, describe, expect, it } from 'vitest'
import Database from 'better-sqlite3'

import { BetterSqlite3Driver, RunStore } from '../src/index.js'

function createDB() {
  return new Database(':memory:')
}

describe('RunStore', () => {
  let db: Database.Database

  beforeEach(() => {
    db = createDB()
  })

  it('creates and reads a run snapshot with counters', async () => {
    const store = new RunStore(new BetterSqlite3Driver(db))
    await store.init()

    const createdAt = new Date('2026-03-24T10:00:00.000Z')
    const expiresAt = new Date('2026-03-24T11:00:00.000Z')

    await store.createRun({
      runId: 'run-1',
      queue: 'telegram-invites',
      createdAt,
      expiresAt,
      scope: { groupBuyId: 'gb-1' },
      counters: {
        queued: 3,
        processing: 0,
        sent: 0,
        failed: 0,
      },
      status: 'queued',
    })

    const snapshot = await store.getRunSnapshot('run-1')

    expect(snapshot).toMatchObject({
      runId: 'run-1',
      queue: 'telegram-invites',
      status: 'queued',
      scope: { groupBuyId: 'gb-1' },
      counters: {
        queued: 3,
        processing: 0,
        sent: 0,
        failed: 0,
      },
    })
    expect(snapshot?.createdAt.toISOString()).toBe(createdAt.toISOString())
    expect(snapshot?.expiresAt.toISOString()).toBe(expiresAt.toISOString())
  })

  it('applies delta and appends an event only once for the same idempotency key', async () => {
    const store = new RunStore(new BetterSqlite3Driver(db))
    await store.init()
    await store.createRun({
      runId: 'run-2',
      queue: 'telegram-invites',
      createdAt: new Date('2026-03-24T10:00:00.000Z'),
      expiresAt: new Date('2026-03-24T11:00:00.000Z'),
    })

    await store.record({
      runId: 'run-2',
      event: {
        kind: 'recipient_sent',
        level: 'ok',
        message: 'Invite delivered',
        subjectKey: 'user-1',
        payload: { authUserId: 'user-1' },
      },
      idempotencyKey: 'delivery:gb-1:user-1',
      now: new Date('2026-03-24T10:05:00.000Z'),
      status: 'running',
      deltas: {
        queued: -1,
        processing: -1,
        sent: 1,
      },
    })

    await store.record({
      runId: 'run-2',
      event: {
        kind: 'recipient_sent',
        level: 'ok',
        message: 'Invite delivered',
        subjectKey: 'user-1',
        payload: { authUserId: 'user-1' },
      },
      idempotencyKey: 'delivery:gb-1:user-1',
      now: new Date('2026-03-24T10:05:05.000Z'),
      status: 'running',
      deltas: {
        queued: -1,
        processing: -1,
        sent: 1,
      },
    })

    const snapshot = await store.getRunSnapshot('run-2')
    const events = await store.listRunEvents('run-2')

    expect(snapshot?.status).toBe('running')
    expect(snapshot?.counters).toMatchObject({
      queued: -1,
      processing: -1,
      sent: 1,
    })
    expect(events).toHaveLength(1)
    expect(events[0]).toMatchObject({
      kind: 'recipient_sent',
      level: 'ok',
      subjectKey: 'user-1',
    })
  })

  it('prunes expired runs together with their events', async () => {
    const store = new RunStore(new BetterSqlite3Driver(db))
    await store.init()
    await store.createRun({
      runId: 'run-3',
      queue: 'telegram-invites',
      createdAt: new Date('2026-03-24T08:00:00.000Z'),
      expiresAt: new Date('2026-03-24T08:30:00.000Z'),
    })
    await store.record({
      runId: 'run-3',
      event: {
        kind: 'batch_started',
        level: 'info',
        message: 'Batch started',
      },
      idempotencyKey: 'batch:start',
      now: new Date('2026-03-24T08:01:00.000Z'),
    })

    const pruned = await store.pruneExpiredRuns(
      new Date('2026-03-24T09:00:00.000Z'),
    )

    expect(pruned.runs).toBe(1)
    expect(pruned.events).toBe(1)
    expect(await store.getRunSnapshot('run-3')).toBeNull()
    expect(await store.listRunEvents('run-3')).toEqual([])
  })
})
