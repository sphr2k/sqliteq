import { describe, it, expect, beforeEach } from 'vitest'
import Database from 'better-sqlite3'
import { ScheduleStore, BetterSqlite3Driver } from '../src/index.js'

function makeStore() {
  const db = new Database(':memory:')
  const store = new ScheduleStore(new BetterSqlite3Driver(db))
  return store
}

describe('ScheduleStore', () => {
  let store: ScheduleStore

  beforeEach(async () => {
    store = makeStore()
    await store.init()
  })

  describe('constructor', () => {
    it('throws when driver is missing', () => {
      expect(() => new ScheduleStore(null as never)).toThrow('driver is required')
    })
  })

  describe('init', () => {
    it('is idempotent', async () => {
      await expect(store.init()).resolves.toBeUndefined()
      await expect(store.init()).resolves.toBeUndefined()
    })
  })

  describe('claimWindow', () => {
    it('returns true on first claim', async () => {
      expect(await store.claimWindow('job:daily', '2024-03-15')).toBe(true)
    })

    it('returns false on duplicate claim (dedupe)', async () => {
      await store.claimWindow('job:daily', '2024-03-15')
      expect(await store.claimWindow('job:daily', '2024-03-15')).toBe(false)
    })

    it('different windowKey on same scheduleKey is independent', async () => {
      expect(await store.claimWindow('job:daily', '2024-03-15')).toBe(true)
      expect(await store.claimWindow('job:daily', '2024-03-16')).toBe(true)
    })

    it('same windowKey on different scheduleKey is independent', async () => {
      expect(await store.claimWindow('job:a', '2024-W12')).toBe(true)
      expect(await store.claimWindow('job:b', '2024-W12')).toBe(true)
    })

    it('stores optional runId and summary at claim time', async () => {
      await store.claimWindow('job:daily', '2024-03-15', {
        runId: 'run_abc',
        summary: { triggeredBy: 'cron' },
      })
      const w = await store.getWindow('job:daily', '2024-03-15')
      expect(w?.runId).toBe('run_abc')
      expect(w?.summary).toEqual({ triggeredBy: 'cron' })
    })

    it('sets status to queued and records enqueuedAt', async () => {
      const before = new Date()
      await store.claimWindow('job:daily', '2024-03-15')
      const after = new Date()
      const w = await store.getWindow('job:daily', '2024-03-15')
      expect(w?.status).toBe('queued')
      expect(w?.enqueuedAt.getTime()).toBeGreaterThanOrEqual(before.getTime())
      expect(w?.enqueuedAt.getTime()).toBeLessThanOrEqual(after.getTime())
    })

    it('throws when scheduleKey is empty', async () => {
      await expect(store.claimWindow('', 'w1')).rejects.toThrow('scheduleKey is required')
    })

    it('throws when windowKey is empty', async () => {
      await expect(store.claimWindow('job:daily', '')).rejects.toThrow('windowKey is required')
    })
  })

  describe('getWindow', () => {
    it('returns null for unclaimed window', async () => {
      expect(await store.getWindow('job:daily', 'never')).toBeNull()
    })

    it('returns the window after claim', async () => {
      await store.claimWindow('job:daily', '2024-03-15')
      const w = await store.getWindow('job:daily', '2024-03-15')
      expect(w).not.toBeNull()
      expect(w?.scheduleKey).toBe('job:daily')
      expect(w?.windowKey).toBe('2024-03-15')
      expect(w?.startedAt).toBeNull()
      expect(w?.finishedAt).toBeNull()
    })
  })

  describe('listWindows', () => {
    it('returns empty array for unknown schedule', async () => {
      expect(await store.listWindows('job:unknown')).toEqual([])
    })

    it('returns windows newest first', async () => {
      await store.claimWindow('job:daily', '2024-03-13')
      await store.claimWindow('job:daily', '2024-03-15')
      await store.claimWindow('job:daily', '2024-03-14')
      const windows = await store.listWindows('job:daily')
      expect(windows).toHaveLength(3)
      // All three keys are present
      expect(windows.map((w) => w.windowKey).sort()).toEqual(['2024-03-13', '2024-03-14', '2024-03-15'])
      // enqueuedAt is non-increasing (newest first)
      for (let i = 1; i < windows.length; i++) {
        expect(windows[i - 1].enqueuedAt.getTime()).toBeGreaterThanOrEqual(windows[i].enqueuedAt.getTime())
      }
    })

    it('respects the limit parameter', async () => {
      for (let i = 1; i <= 5; i++) {
        await store.claimWindow('job:hourly', `2024-03-15T0${i}:00Z`)
      }
      const windows = await store.listWindows('job:hourly', 3)
      expect(windows).toHaveLength(3)
    })

    it('does not return windows for other schedule keys', async () => {
      await store.claimWindow('job:a', 'w1')
      await store.claimWindow('job:b', 'w1')
      expect(await store.listWindows('job:a')).toHaveLength(1)
    })

    it('throws when scheduleKey is empty', async () => {
      await expect(store.listWindows('')).rejects.toThrow('scheduleKey is required')
    })
  })

  describe('markWindowRunning', () => {
    it('transitions queued → running and sets runId + startedAt', async () => {
      await store.claimWindow('job:daily', '2024-03-15')
      const before = new Date()
      const ok = await store.markWindowRunning('job:daily', '2024-03-15', 'run_xyz')
      const after = new Date()
      expect(ok).toBe(true)
      const w = await store.getWindow('job:daily', '2024-03-15')
      expect(w?.status).toBe('running')
      expect(w?.runId).toBe('run_xyz')
      expect(w?.startedAt!.getTime()).toBeGreaterThanOrEqual(before.getTime())
      expect(w?.startedAt!.getTime()).toBeLessThanOrEqual(after.getTime())
    })

    it('returns false when window does not exist', async () => {
      expect(await store.markWindowRunning('job:daily', 'ghost', 'run_1')).toBe(false)
    })

    it('returns false when already running (not queued)', async () => {
      await store.claimWindow('job:daily', '2024-03-15')
      await store.markWindowRunning('job:daily', '2024-03-15', 'run_1')
      expect(await store.markWindowRunning('job:daily', '2024-03-15', 'run_2')).toBe(false)
    })

    it('returns false when already finished', async () => {
      await store.claimWindow('job:daily', '2024-03-15')
      await store.markWindowRunning('job:daily', '2024-03-15', 'run_1')
      await store.markWindowFinished('job:daily', '2024-03-15', 'completed')
      expect(await store.markWindowRunning('job:daily', '2024-03-15', 'run_2')).toBe(false)
    })
  })

  describe('markWindowFinished', () => {
    async function runThrough(windowKey: string, status: 'completed' | 'failed', summary?: unknown) {
      await store.claimWindow('job:daily', windowKey)
      await store.markWindowRunning('job:daily', windowKey, 'run_1')
      return store.markWindowFinished('job:daily', windowKey, status, summary != null ? { summary } : undefined)
    }

    it('transitions running → completed', async () => {
      expect(await runThrough('2024-03-15', 'completed')).toBe(true)
      const w = await store.getWindow('job:daily', '2024-03-15')
      expect(w?.status).toBe('completed')
      expect(w?.finishedAt).not.toBeNull()
    })

    it('transitions running → failed', async () => {
      expect(await runThrough('2024-03-15', 'failed')).toBe(true)
      const w = await store.getWindow('job:daily', '2024-03-15')
      expect(w?.status).toBe('failed')
    })

    it('stores optional summary', async () => {
      await runThrough('2024-03-15', 'completed', { processed: 42, errors: 0 })
      const w = await store.getWindow('job:daily', '2024-03-15')
      expect(w?.summary).toEqual({ processed: 42, errors: 0 })
    })

    it('returns false when not in running state', async () => {
      await store.claimWindow('job:daily', '2024-03-15')
      expect(await store.markWindowFinished('job:daily', '2024-03-15', 'completed')).toBe(false)
    })

    it('returns false for non-existent window', async () => {
      expect(await store.markWindowFinished('job:daily', 'ghost', 'completed')).toBe(false)
    })

    it('records finishedAt timestamp', async () => {
      const before = new Date()
      await runThrough('2024-03-15', 'completed')
      const after = new Date()
      const w = await store.getWindow('job:daily', '2024-03-15')
      expect(w?.finishedAt!.getTime()).toBeGreaterThanOrEqual(before.getTime())
      expect(w?.finishedAt!.getTime()).toBeLessThanOrEqual(after.getTime())
    })
  })

  describe('full lifecycle', () => {
    it('queued → running → completed with summary', async () => {
      const claimed = await store.claimWindow('reports:weekly', '2024-W12', {
        summary: { trigger: 'scheduled' },
      })
      expect(claimed).toBe(true)
      expect((await store.getWindow('reports:weekly', '2024-W12'))?.status).toBe('queued')

      await store.markWindowRunning('reports:weekly', '2024-W12', 'run_001')
      expect((await store.getWindow('reports:weekly', '2024-W12'))?.status).toBe('running')

      await store.markWindowFinished('reports:weekly', '2024-W12', 'completed', {
        summary: { rowsProcessed: 500 },
      })

      const w = await store.getWindow('reports:weekly', '2024-W12')
      expect(w?.status).toBe('completed')
      expect(w?.runId).toBe('run_001')
      expect(w?.summary).toEqual({ rowsProcessed: 500 })
      expect(w?.startedAt).not.toBeNull()
      expect(w?.finishedAt).not.toBeNull()
      expect(w?.startedAt!.getTime()).toBeLessThanOrEqual(w?.finishedAt!.getTime())
    })

    it('duplicate claim after completion is still rejected', async () => {
      await store.claimWindow('reports:weekly', '2024-W12')
      await store.markWindowRunning('reports:weekly', '2024-W12', 'run_1')
      await store.markWindowFinished('reports:weekly', '2024-W12', 'completed')

      expect(await store.claimWindow('reports:weekly', '2024-W12')).toBe(false)
    })
  })
})
