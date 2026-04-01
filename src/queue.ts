import type {
  Database,
  ID,
  Message,
  QueueOptions,
  QueueStats,
  RequeueDeadLettersOptions,
  SendOptions,
} from './types.js'

const SCHEMA = `
create table if not exists sqliteq (
  id       text    primary key default ('m_' || lower(hex(randomblob(16)))),
  created  text    not null default (strftime('%Y-%m-%dT%H:%M:%fZ')),
  queue    text    not null,
  body     text    not null,
  timeout  text    not null default (strftime('%Y-%m-%dT%H:%M:%fZ')),
  received integer not null default 0,
  priority integer not null default 0
) strict;

create index if not exists sqliteq_queue_priority_created_idx on sqliteq (queue, priority desc, created);
`

const SQL_SEND = `
  insert into sqliteq (queue, body, timeout, priority)
  values (?, ?, ?, ?)
  returning id
`

// Atomic claim: the subquery finds the next eligible message, the outer update
// bumps its timeout + receive count in a single statement — no lock needed.
const SQL_RECEIVE = `
  update sqliteq
  set
    timeout = ?,
    received = received + 1
  where id = (
    select id from sqliteq
    where
      queue = ? and
      ? >= timeout and
      received < ?
    order by priority desc, created
    limit 1
  )
  returning id, body, received
`

const SQL_EXTEND = `update sqliteq set timeout = ? where queue = ? and id = ? and received = ?`

const SQL_DELETE = `delete from sqliteq where queue = ? and id = ? and received = ?`

const SQL_SIZE = `select count(*) as c from sqliteq where queue = ?`

const SQL_STATS = `
  select
    count(*) as total,
    sum(case when received < ? and ? >= timeout then 1 else 0 end) as ready,
    sum(case when received = 0 and ? < timeout then 1 else 0 end) as delayed,
    sum(case when received > 0 and ? < timeout then 1 else 0 end) as in_flight,
    sum(case when received >= ? and ? >= timeout then 1 else 0 end) as dead
  from sqliteq
  where queue = ?
`

const SQL_PURGE = `delete from sqliteq where queue = ?`

const SQL_DEAD_LETTERS = `
  select id, body, received
  from sqliteq
  where queue = ? and received >= ? and ? >= timeout
  order by created
`

const SQL_DEAD_LETTER_ROWS = `
  select id, body, received, priority
  from sqliteq
  where queue = ? and received >= ? and ? >= timeout
  order by created
`

const SQL_PURGE_DEAD_LETTERS = `
  delete from sqliteq
  where queue = ? and received >= ? and ? >= timeout
`

function nowPlusMs(ms: number): string {
  return new Date(Date.now() + ms).toISOString()
}

function validateFinite(value: number, name: string): void {
  if (!Number.isFinite(value)) throw new Error(`${name} must be a finite number`)
}

function validatePositiveInteger(value: number, name: string): void {
  if (!Number.isInteger(value) || value < 1) {
    throw new Error(`${name} must be an integer >= 1`)
  }
}

function parseMessageBody<T>(body: string, messageId: string): T {
  try {
    return JSON.parse(body) as T
  } catch (err) {
    throw new Error(`Failed to parse body for message ${messageId}: ${err}`)
  }
}

const DEFAULT_MAX_BODY_BYTES = 1_048_576 // 1MB

/**
 * A durable message queue backed by a single SQLite table.
 *
 * **`T` must be JSON-serializable.** The body is stored via `JSON.stringify`
 * and restored via `JSON.parse`, so types that do not survive a JSON round-trip
 * (Date, Map, Set, RegExp, functions, symbols, `undefined` values, class
 * instances with prototype methods, BigInt, etc.) will be silently converted or
 * will throw at send time. Stick to plain objects, arrays, strings, numbers,
 * booleans, and null.
 *
 * There is no built-in TypeScript `JsonSerializable` constraint, so this is
 * enforced by convention rather than at the type level.
 */
export class Queue<T = string> {
  private db: Database
  private name: string
  /** Visibility timeout in ms. Exposed for Processor to read. */
  readonly timeout: number
  private maxReceive: number
  private maxBodyBytes: number

  // Cached prepared statements — avoids re-parsing SQL on every call
  private stmts: {
    send: ReturnType<Database['prepare']>
    receive: ReturnType<Database['prepare']>
    extend: ReturnType<Database['prepare']>
    delete: ReturnType<Database['prepare']>
    size: ReturnType<Database['prepare']>
    stats: ReturnType<Database['prepare']>
    purge: ReturnType<Database['prepare']>
    deadLetters: ReturnType<Database['prepare']>
    deadLetterRows: ReturnType<Database['prepare']>
    purgeDeadLetters: ReturnType<Database['prepare']>
  }

  constructor(db: Database, name: string, options?: QueueOptions) {
    if (!db) throw new Error('db is required')
    if (!name) throw new Error('name is required')
    if (name.length > 255) throw new Error('name too long (max 255)')

    this.db = db
    this.name = name
    this.timeout = options?.timeout ?? 5_000
    this.maxReceive = options?.maxReceive ?? 3
    this.maxBodyBytes = options?.maxBodyBytes ?? DEFAULT_MAX_BODY_BYTES

    validateFinite(this.timeout, 'timeout')
    validateFinite(this.maxReceive, 'maxReceive')
    validateFinite(this.maxBodyBytes, 'maxBodyBytes')
    if (this.timeout < 0) throw new Error('timeout cannot be negative')
    if (this.maxReceive < 1) throw new Error('maxReceive must be at least 1')
    if (this.maxBodyBytes < 1) throw new Error('maxBodyBytes must be at least 1')

    try {
      db.exec('pragma journal_mode = WAL')
      db.exec('pragma busy_timeout = 5000')
    } catch {
      // Some DB wrappers may not support exec for pragmas — non-fatal
    }

    db.exec(SCHEMA)

    this.stmts = {
      send: db.prepare(SQL_SEND),
      receive: db.prepare(SQL_RECEIVE),
      extend: db.prepare(SQL_EXTEND),
      delete: db.prepare(SQL_DELETE),
      size: db.prepare(SQL_SIZE),
      stats: db.prepare(SQL_STATS),
      purge: db.prepare(SQL_PURGE),
      deadLetters: db.prepare(SQL_DEAD_LETTERS),
      deadLetterRows: db.prepare(SQL_DEAD_LETTER_ROWS),
      purgeDeadLetters: db.prepare(SQL_PURGE_DEAD_LETTERS),
    }
  }

  /**
   * Send a message to the queue. Returns the message ID.
   *
   * Body is JSON-serialized. T must be JSON-round-trippable.
   * Date, Map, Set, and other non-POJO types will not survive serialization.
   */
  send(body: T, options?: SendOptions): ID {
    if (body === undefined) throw new Error('body cannot be undefined')

    const delay = options?.delay ?? 0
    const priority = options?.priority ?? 0

    validateFinite(delay, 'delay')
    validateFinite(priority, 'priority')
    if (delay < 0) throw new Error('delay cannot be negative')

    const timeout = nowPlusMs(delay)
    const serialized = JSON.stringify(body)

    if (serialized === undefined) {
      throw new Error('body is not JSON-serializable (functions, symbols, etc. cannot be serialized)')
    }

    if (Buffer.byteLength(serialized, 'utf8') > this.maxBodyBytes) {
      throw new Error(`body exceeds max size (${this.maxBodyBytes} bytes)`)
    }

    const row = this.stmts.send.get(this.name, serialized, timeout, priority) as { id: string } | undefined
    if (!row) throw new Error('failed to insert message')
    return row.id
  }

  /**
   * Send multiple messages in a single transaction.
   * If any message fails validation, the entire batch is rolled back.
   * @returns An array of message IDs in the same order as the input.
   */
  sendBatch(messages: Array<{ body: T; options?: SendOptions }>): ID[] {
    return this.db.transaction(() =>
      messages.map((m) => this.send(m.body, m.options))
    )()
  }

  /**
   * Atomically claim the next available message.
   * The message becomes invisible to other consumers for `timeout` ms.
   * @returns The message, or `null` if the queue is empty or all messages are in-flight.
   */
  receive(): Message<T> | null {
    const now = new Date().toISOString()
    const timeout = nowPlusMs(this.timeout)

    const row = this.stmts.receive.get(timeout, this.name, now, this.maxReceive) as
      | { id: string; body: string; received: number }
      | undefined

    if (!row) return null

    return { id: row.id, body: parseMessageBody<T>(row.body, row.id), received: row.received }
  }

  /**
   * Atomically claim up to `limit` available messages.
   * Returns fewer than requested when the queue runs dry.
   */
  receiveBatch(limit: number): Message<T>[] {
    validatePositiveInteger(limit, 'limit')

    return this.db.transaction(() => {
      const messages: Message<T>[] = []
      for (let i = 0; i < limit; i++) {
        const msg = this.receive()
        if (!msg) break
        messages.push(msg)
      }
      return messages
    })()
  }

  /**
   * Extend a message's visibility timeout by `delay` ms from now.
   * @param id - Message ID from {@link Message.id}.
   * @param received - Receive count from {@link Message.received} (used as a fencing token).
   * @param delay - Additional visibility time in ms.
   * @returns `true` if extended, `false` if the message was already re-delivered (stale handle).
   */
  extend(id: ID, received: number, delay: number): boolean {
    validateFinite(delay, 'delay')
    if (delay < 0) throw new Error('delay cannot be negative')
    const timeout = nowPlusMs(delay)
    const result = this.stmts.extend.run(timeout, this.name, id, received)
    return result.changes > 0
  }

  /**
   * Acknowledge and remove a message from the queue.
   * @param id - Message ID from {@link Message.id}.
   * @param received - Receive count from {@link Message.received} (used as a fencing token).
   * @returns `true` if deleted, `false` if the message was already re-delivered (stale handle — safe no-op).
   */
  delete(id: ID, received: number): boolean {
    const result = this.stmts.delete.run(this.name, id, received)
    return result.changes > 0
  }

  /** Number of messages in this queue (all states). */
  size(): number {
    const row = this.stmts.size.get(this.name) as { c: number }
    return row.c
  }

  /** Queue counts grouped by operational state. */
  stats(): QueueStats {
    const now = new Date().toISOString()
    const row = this.stmts.stats.get(
      this.maxReceive,
      now,
      now,
      now,
      this.maxReceive,
      now,
      this.name,
    ) as
      | { total: number; ready: number | null; delayed: number | null; in_flight: number | null; dead: number | null }
      | undefined

    return {
      total: row?.total ?? 0,
      ready: row?.ready ?? 0,
      delayed: row?.delayed ?? 0,
      inFlight: row?.in_flight ?? 0,
      dead: row?.dead ?? 0,
    }
  }

  /** Remove all messages from this queue. @returns Count of messages deleted. */
  purge(): number {
    return this.stmts.purge.run(this.name).changes
  }

  /**
   * Requeue all currently dead-lettered messages as fresh messages.
   * Requeued messages get new IDs so stale handles from previous deliveries remain invalid.
   * @returns The new message IDs in FIFO order.
   */
  requeueDeadLetters(options?: RequeueDeadLettersOptions): ID[] {
    const delay = options?.delay ?? 0
    validateFinite(delay, 'delay')
    if (delay < 0) throw new Error('delay cannot be negative')

    return this.db.transaction(() => {
      const now = new Date().toISOString()
      const timeout = nowPlusMs(delay)
      const rows = this.stmts.deadLetterRows.all(this.name, this.maxReceive, now) as
        Array<{ id: string; body: string; received: number; priority: number }>

      const ids: ID[] = []
      for (const row of rows) {
        const inserted = this.stmts.send.get(this.name, row.body, timeout, row.priority) as { id: string } | undefined
        if (!inserted) throw new Error(`failed to requeue dead letter ${row.id}`)
        ids.push(inserted.id)

        const deleted = this.stmts.delete.run(this.name, row.id, row.received)
        if (deleted.changes !== 1) throw new Error(`failed to delete dead letter ${row.id} after requeue`)
      }

      return ids
    })()
  }

  /** Remove all currently dead-lettered messages. @returns Count of messages deleted. */
  purgeDeadLetters(): number {
    const now = new Date().toISOString()
    return this.stmts.purgeDeadLetters.run(this.name, this.maxReceive, now).changes
  }

  /** Get messages that exceeded `maxReceive` and will never be delivered again. Inspect or purge these periodically. */
  deadLetters(): Message<T>[] {
    const now = new Date().toISOString()
    const rows = this.stmts.deadLetters.all(this.name, this.maxReceive, now) as
      Array<{ id: string; body: string; received: number }>

    return rows.map((r) => {
      let body: T
      try {
        body = parseMessageBody<T>(r.body, r.id)
      } catch {
        body = r.body as T
      }
      return { id: r.id, body, received: r.received }
    })
  }
}
