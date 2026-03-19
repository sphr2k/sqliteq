/** Minimal SQLite statement interface */
export interface Statement {
  run(...params: unknown[]): { changes: number }
  get(...params: unknown[]): unknown
  all(...params: unknown[]): unknown[]
}

/** Minimal SQLite database interface. Tested with better-sqlite3 and bun:sqlite. */
export interface Database {
  prepare(sql: string): Statement
  exec(sql: string): void
  transaction<R>(fn: (...args: unknown[]) => R): (...args: unknown[]) => R
}

/** Opaque message identifier (format: `m_` + 32 hex chars). */
export type ID = string

/** A message returned by {@link Queue.receive}. */
export interface Message<T = string> {
  /** Unique message identifier. */
  id: ID
  /** Deserialized message body. */
  body: T
  /** How many times this message has been received (1 on first delivery). Pass to `delete`/`extend` for fencing. */
  received: number
}

export interface SendOptions {
  /** Delivery delay in ms; message is invisible until elapsed (default: 0) */
  delay?: number
  /** Higher-priority messages are received first (default: 0) */
  priority?: number
}

export interface QueueOptions {
  /** Default visibility timeout in ms (default: 5000) */
  timeout?: number
  /** Max receive count before message becomes dead-lettered (default: 3) */
  maxReceive?: number
  /** Max body size in bytes after JSON serialization (default: 1MB) */
  maxBodyBytes?: number
}
