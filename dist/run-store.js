const RUN_SCHEMA = `
create table if not exists sqliteq_runs (
  run_id text primary key,
  queue text not null,
  status text not null default 'queued',
  scope_json text,
  counters_json text not null default '{}',
  created_at text not null,
  updated_at text not null,
  expires_at text not null
) strict;
`;
const EVENT_SCHEMA = `
create table if not exists sqliteq_run_events (
  id text primary key default ('e_' || lower(hex(randomblob(16)))),
  run_id text not null,
  kind text not null,
  level text not null,
  message text not null,
  subject_key text,
  payload_json text,
  created_at text not null,
  foreign key (run_id) references sqliteq_runs(run_id) on delete cascade
) strict;
`;
const TOKEN_SCHEMA = `
create table if not exists sqliteq_run_idempotency (
  run_id text not null,
  token text not null,
  applied_at text not null,
  primary key (run_id, token),
  foreign key (run_id) references sqliteq_runs(run_id) on delete cascade
) strict;
`;
const EVENT_INDEX = `
create index if not exists sqliteq_run_events_run_created_idx
on sqliteq_run_events (run_id, created_at, id);
`;
const RUN_EXPIRY_INDEX = `
create index if not exists sqliteq_runs_expires_at_idx
on sqliteq_runs (expires_at);
`;
const SQL_INSERT_RUN = `
insert into sqliteq_runs (
  run_id, queue, status, scope_json, counters_json, created_at, updated_at, expires_at
) values (?, ?, ?, ?, ?, ?, ?, ?)
`;
const SQL_GET_RUN = `
select run_id, queue, status, scope_json, counters_json, created_at, updated_at, expires_at
from sqliteq_runs
where run_id = ?
`;
const SQL_INSERT_TOKEN = `
insert or ignore into sqliteq_run_idempotency (run_id, token, applied_at)
values (?, ?, ?)
`;
const SQL_UPDATE_RUN = `
update sqliteq_runs
set status = ?, counters_json = ?, updated_at = ?
where run_id = ?
`;
const SQL_INSERT_EVENT = `
insert into sqliteq_run_events (
  run_id, kind, level, message, subject_key, payload_json, created_at
) values (?, ?, ?, ?, ?, ?, ?)
`;
const SQL_LIST_EVENTS = `
select id, run_id, kind, level, message, subject_key, payload_json, created_at
from sqliteq_run_events
where run_id = ?
order by created_at asc, id asc
`;
const SQL_SELECT_EXPIRED_RUNS = `
select run_id from sqliteq_runs
where expires_at <= ?
`;
const SQL_COUNT_EVENTS_FOR_RUNS = `
select count(*) as count
from sqliteq_run_events
where run_id in (%RUN_IDS%)
`;
const SQL_DELETE_RUNS = `
delete from sqliteq_runs
where run_id in (%RUN_IDS%)
`;
function parseJson(value, fallback) {
    if (!value) {
        return fallback;
    }
    try {
        return JSON.parse(value);
    }
    catch {
        return fallback;
    }
}
function toJson(value) {
    if (value == null) {
        return null;
    }
    return JSON.stringify(value);
}
function applyDeltas(current, deltas) {
    if (!deltas) {
        return current;
    }
    const next = { ...current };
    for (const [key, value] of Object.entries(deltas)) {
        next[key] = (next[key] ?? 0) + value;
    }
    return next;
}
function placeholders(count) {
    return new Array(count).fill('?').join(', ');
}
export class RunStore {
    driver;
    constructor(driver) {
        this.driver = driver;
        if (!driver) {
            throw new Error('driver is required');
        }
    }
    async init() {
        try {
            await this.driver.execute('pragma foreign_keys = on', []);
            await this.driver.execute('pragma busy_timeout = 5000', []);
        }
        catch {
            // Some drivers may not support pragmas.
        }
        await this.driver.batch([
            RUN_SCHEMA,
            EVENT_SCHEMA,
            TOKEN_SCHEMA,
            EVENT_INDEX,
            RUN_EXPIRY_INDEX,
        ]);
    }
    async createRun(input) {
        await this.driver.execute(SQL_INSERT_RUN, [
            input.runId,
            input.queue,
            input.status ?? 'queued',
            toJson(input.scope ?? null),
            JSON.stringify(input.counters ?? {}),
            input.createdAt.toISOString(),
            input.createdAt.toISOString(),
            input.expiresAt.toISOString(),
        ]);
    }
    async getRunSnapshot(runId) {
        const result = await this.driver.execute(SQL_GET_RUN, [runId]);
        const row = result.rows[0];
        if (!row) {
            return null;
        }
        return {
            runId: row.run_id,
            queue: row.queue,
            status: row.status,
            scope: parseJson(row.scope_json, null),
            counters: parseJson(row.counters_json, {}),
            createdAt: new Date(row.created_at),
            updatedAt: new Date(row.updated_at),
            expiresAt: new Date(row.expires_at),
        };
    }
    async record(input) {
        const tokenResult = await this.driver.execute(SQL_INSERT_TOKEN, [
            input.runId,
            input.idempotencyKey,
            input.now.toISOString(),
        ]);
        if (tokenResult.rowsAffected === 0) {
            return false;
        }
        const snapshot = await this.getRunSnapshot(input.runId);
        if (!snapshot) {
            throw new Error(`run not found: ${input.runId}`);
        }
        const counters = applyDeltas(snapshot.counters, input.deltas);
        await this.driver.execute(SQL_UPDATE_RUN, [
            input.status ?? snapshot.status,
            JSON.stringify(counters),
            input.now.toISOString(),
            input.runId,
        ]);
        if (input.event) {
            await this.driver.execute(SQL_INSERT_EVENT, [
                input.runId,
                input.event.kind,
                input.event.level,
                input.event.message,
                input.event.subjectKey ?? null,
                toJson(input.event.payload ?? null),
                input.now.toISOString(),
            ]);
        }
        return true;
    }
    async listRunEvents(runId) {
        const result = await this.driver.execute(SQL_LIST_EVENTS, [runId]);
        return result.rows.map((row) => ({
            id: row.id,
            runId: row.run_id,
            kind: row.kind,
            level: row.level,
            message: row.message,
            subjectKey: row.subject_key,
            payload: parseJson(row.payload_json, null),
            createdAt: new Date(row.created_at),
        }));
    }
    async pruneExpiredRuns(now) {
        const expiredRunsResult = await this.driver.execute(SQL_SELECT_EXPIRED_RUNS, [
            now.toISOString(),
        ]);
        const runIds = expiredRunsResult.rows.map((row) => row.run_id);
        if (runIds.length === 0) {
            return { runs: 0, events: 0 };
        }
        const eventCountResult = await this.driver.execute(SQL_COUNT_EVENTS_FOR_RUNS.replace('%RUN_IDS%', placeholders(runIds.length)), runIds);
        const events = Number(eventCountResult.rows[0]?.count ?? 0);
        const deleteResult = await this.driver.execute(SQL_DELETE_RUNS.replace('%RUN_IDS%', placeholders(runIds.length)), runIds);
        return {
            runs: deleteResult.rowsAffected,
            events,
        };
    }
}
//# sourceMappingURL=run-store.js.map