/**
 * Adapter for better-sqlite3 (synchronous Node.js).
 * Wraps synchronous calls in a Promise for a unified async API.
 */
export class BetterSqlite3Driver {
    db;
    constructor(db) {
        this.db = db;
    }
    async execute(sql, args) {
        const stmt = this.db.prepare(sql);
        // Check if it's a query that returns rows (RETURNING, SELECT) or a mutation
        if (sql.trim().toLowerCase().includes('returning') || sql.trim().toLowerCase().startsWith('select')) {
            const rows = stmt.all(...args);
            return { rows, rowsAffected: 0 }; // better-sqlite3 .all doesn't return changes
        }
        else {
            const info = stmt.run(...args);
            return { rows: [], rowsAffected: info.changes };
        }
    }
    async batch(sqls) {
        this.db.transaction(() => {
            for (const sql of sqls) {
                this.db.exec(sql);
            }
        })();
    }
}
/**
 * Adapter for bun:sqlite (synchronous Bun).
 */
export class BunSqliteDriver {
    db;
    constructor(db) {
        this.db = db;
    }
    async execute(sql, args) {
        const query = this.db.query(sql);
        if (sql.trim().toLowerCase().includes('returning') || sql.trim().toLowerCase().startsWith('select')) {
            const rows = query.all(...args);
            return { rows, rowsAffected: 0 };
        }
        else {
            const info = query.run(...args);
            return { rows: [], rowsAffected: info.changes };
        }
    }
    async batch(sqls) {
        this.db.transaction(() => {
            for (const sql of sqls) {
                this.db.run(sql);
            }
        })();
    }
}
/**
 * Adapter for @libsql/client (asynchronous Turso).
 */
export class LibsqlDriver {
    client;
    constructor(client) {
        this.client = client;
    }
    async execute(sql, args) {
        const res = await this.client.execute({ sql, args });
        return {
            rows: res.rows,
            rowsAffected: res.rowsAffected,
            lastInsertRowid: res.lastInsertRowid
        };
    }
    async batch(sqls) {
        await this.client.batch(sqls, 'write');
    }
    close() {
        this.client.close();
    }
}
//# sourceMappingURL=drivers.js.map