import type { Database, QueueDriver, DriverResultSet } from './types.js';
/**
 * Adapter for better-sqlite3 (synchronous Node.js).
 * Wraps synchronous calls in a Promise for a unified async API.
 */
export declare class BetterSqlite3Driver implements QueueDriver {
    private db;
    constructor(db: Database);
    execute(sql: string, args: any[]): Promise<DriverResultSet>;
    batch(sqls: string[]): Promise<void>;
}
/**
 * Adapter for bun:sqlite (synchronous Bun).
 */
export declare class BunSqliteDriver implements QueueDriver {
    private db;
    constructor(db: any);
    execute(sql: string, args: any[]): Promise<DriverResultSet>;
    batch(sqls: string[]): Promise<void>;
}
/**
 * Adapter for @libsql/client (asynchronous Turso).
 */
export declare class LibsqlDriver implements QueueDriver {
    private client;
    constructor(client: any);
    execute(sql: string, args: any[]): Promise<DriverResultSet>;
    batch(sqls: string[]): Promise<void>;
    close(): void;
}
//# sourceMappingURL=drivers.d.ts.map