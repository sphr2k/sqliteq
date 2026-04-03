const SCHEMA = `
create table if not exists sqliteq_schedule_windows (
  schedule_key  text    not null,
  window_key    text    not null,
  status        text    not null default 'queued',
  run_id        text,
  enqueued_at   text    not null,
  started_at    text,
  finished_at   text,
  summary_json  text,
  primary key (schedule_key, window_key)
) strict;
`;
const INDEX_LIST = `
create index if not exists sqliteq_schedule_windows_schedule_enqueued_idx
on sqliteq_schedule_windows (schedule_key, enqueued_at desc);
`;
const SQL_CLAIM = `
insert or ignore into sqliteq_schedule_windows
  (schedule_key, window_key, status, run_id, enqueued_at, summary_json)
values (?, ?, 'queued', ?, ?, ?)
`;
const SQL_GET = `
select schedule_key, window_key, status, run_id, enqueued_at, started_at, finished_at, summary_json
from sqliteq_schedule_windows
where schedule_key = ? and window_key = ?
`;
const SQL_LIST = `
select schedule_key, window_key, status, run_id, enqueued_at, started_at, finished_at, summary_json
from sqliteq_schedule_windows
where schedule_key = ?
order by enqueued_at desc
limit ?
`;
const SQL_MARK_RUNNING = `
update sqliteq_schedule_windows
set status = 'running', run_id = ?, started_at = ?
where schedule_key = ? and window_key = ? and status = 'queued'
`;
const SQL_MARK_FINISHED = `
update sqliteq_schedule_windows
set status = ?, finished_at = ?, summary_json = ?
where schedule_key = ? and window_key = ? and status = 'running'
`;
function toIso(d) {
    return d.toISOString();
}
function parseDate(v) {
    return v ? new Date(v) : null;
}
function toJson(v) {
    return v == null ? null : JSON.stringify(v);
}
function parseJson(v) {
    if (!v)
        return null;
    try {
        return JSON.parse(v);
    }
    catch {
        return null;
    }
}
function rowToWindow(row) {
    return {
        scheduleKey: row.schedule_key,
        windowKey: row.window_key,
        status: row.status,
        runId: row.run_id ?? null,
        enqueuedAt: new Date(row.enqueued_at),
        startedAt: parseDate(row.started_at),
        finishedAt: parseDate(row.finished_at),
        summary: parseJson(row.summary_json),
    };
}
/**
 * Generic schedule-window ledger.
 *
 * Tracks one row per (scheduleKey, windowKey) pair. The composite primary key
 * makes `claimWindow` a safe, atomic dedupe operation — calling it twice for
 * the same window is a no-op and returns `false` on the second call.
 *
 * Intended use-cases:
 *  - Periodic job scheduler: scheduleKey = job name, windowKey = ISO period
 *    (e.g. "2024-W12", "2024-03-15T00:00Z").
 *  - Any system that needs to track "has this window been processed?" without
 *    coupling to queue message IDs.
 */
export class ScheduleStore {
    driver;
    constructor(driver) {
        this.driver = driver;
        if (!driver)
            throw new Error('driver is required');
    }
    async init() {
        try {
            await this.driver.execute('pragma foreign_keys = on', []);
            await this.driver.execute('pragma busy_timeout = 5000', []);
        }
        catch {
            // Some drivers may not support pragmas — non-fatal.
        }
        await this.driver.batch([SCHEMA, INDEX_LIST]);
    }
    /**
     * Atomically claim a schedule window. Safe to call concurrently — only the
     * first caller succeeds; subsequent calls for the same (scheduleKey, windowKey)
     * are no-ops and return `false`.
     *
     * @returns `true` if the window was newly created, `false` if it already existed.
     */
    async claimWindow(scheduleKey, windowKey, options) {
        if (!scheduleKey)
            throw new Error('scheduleKey is required');
        if (!windowKey)
            throw new Error('windowKey is required');
        const now = toIso(new Date());
        const result = await this.driver.execute(SQL_CLAIM, [
            scheduleKey,
            windowKey,
            options?.runId ?? null,
            now,
            toJson(options?.summary ?? null),
        ]);
        return result.rowsAffected > 0;
    }
    /**
     * Retrieve a single window entry.
     * @returns The window, or `null` if it has never been claimed.
     */
    async getWindow(scheduleKey, windowKey) {
        const result = await this.driver.execute(SQL_GET, [scheduleKey, windowKey]);
        const row = result.rows[0];
        return row ? rowToWindow(row) : null;
    }
    /**
     * List windows for a schedule key, newest first.
     * @param limit Max rows to return (default: 50).
     */
    async listWindows(scheduleKey, limit = 50) {
        if (!scheduleKey)
            throw new Error('scheduleKey is required');
        const result = await this.driver.execute(SQL_LIST, [scheduleKey, limit]);
        return result.rows.map(rowToWindow);
    }
    /**
     * Transition a window from `queued` → `running`.
     * Associates a runId and records the startedAt timestamp.
     *
     * @returns `true` if updated, `false` if the window was not in `queued` state
     *          (already running, finished, or non-existent).
     */
    async markWindowRunning(scheduleKey, windowKey, runId) {
        const now = toIso(new Date());
        const result = await this.driver.execute(SQL_MARK_RUNNING, [
            runId,
            now,
            scheduleKey,
            windowKey,
        ]);
        return result.rowsAffected > 0;
    }
    /**
     * Transition a window from `running` → `completed` or `failed`.
     * Records the finishedAt timestamp and optional summary.
     *
     * @returns `true` if updated, `false` if the window was not in `running` state.
     */
    async markWindowFinished(scheduleKey, windowKey, status, options) {
        const now = toIso(new Date());
        const result = await this.driver.execute(SQL_MARK_FINISHED, [
            status,
            now,
            toJson(options?.summary ?? null),
            scheduleKey,
            windowKey,
        ]);
        return result.rowsAffected > 0;
    }
}
//# sourceMappingURL=schedule-store.js.map