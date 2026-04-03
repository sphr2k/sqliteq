/** Minimal SQLite statement interface */
export interface Statement {
    run(...params: unknown[]): {
        changes: number;
    };
    get(...params: unknown[]): unknown;
    all(...params: unknown[]): unknown[];
}
/** Minimal SQLite database interface. Tested with better-sqlite3 and bun:sqlite. */
export interface Database {
    prepare(sql: string): Statement;
    exec(sql: string): void;
    transaction<R>(fn: (...args: unknown[]) => R): (...args: unknown[]) => R;
}
/** Result of a driver execution */
export interface DriverResultSet {
    rows: any[];
    rowsAffected: number;
    lastInsertRowid?: string | number;
}
/**
 * Unified driver interface for different SQLite backends.
 * Implementation can be synchronous or asynchronous.
 */
export interface QueueDriver {
    execute(sql: string, args: any[]): Promise<DriverResultSet>;
    batch(sqls: string[]): Promise<void>;
    close?(): void;
}
/** Opaque message identifier (format: `m_` + 32 hex chars). */
export type ID = string;
/** A message returned by {@link Queue.receive}. */
export interface Message<T = string> {
    /** Unique message identifier. */
    id: ID;
    /** Deserialized message body. */
    body: T;
    /** How many times this message has been received (1 on first delivery). Pass to `delete`/`extend` for fencing. */
    received: number;
}
export interface SendOptions {
    /** Delivery delay in ms; message is invisible until elapsed (default: 0) */
    delay?: number;
    /** Higher-priority messages are received first (default: 0) */
    priority?: number;
}
export interface QueueOptions {
    /** Default visibility timeout in ms (default: 5000) */
    timeout?: number;
    /** Max receive count before message becomes dead-lettered (default: 3) */
    maxReceive?: number;
    /** Max body size in bytes after JSON serialization (default: 1MB) */
    maxBodyBytes?: number;
}
export interface QueueStats {
    /** Total messages in the queue across all states. */
    total: number;
    /** Messages available to receive now. */
    ready: number;
    /** Delayed messages that have never been received yet. */
    delayed: number;
    /** Messages currently claimed by a consumer (including the final allowed attempt). */
    inFlight: number;
    /** Messages that exceeded maxReceive and whose visibility timeout has expired. */
    dead: number;
}
export interface RequeueDeadLettersOptions {
    /** Optional delay in ms before the requeued messages become visible. */
    delay?: number;
}
//# sourceMappingURL=types.d.ts.map