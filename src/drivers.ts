import type { Database, QueueDriver, DriverResultSet } from './types.js'

/** 
 * Adapter for better-sqlite3 (synchronous Node.js).
 * Wraps synchronous calls in a Promise for a unified async API.
 */
export class BetterSqlite3Driver implements QueueDriver {
  constructor(private db: Database) {}

  async execute(sql: string, args: any[]): Promise<DriverResultSet> {
    const stmt = this.db.prepare(sql)
    
    // Check if it's a query that returns rows (RETURNING, SELECT) or a mutation
    if (sql.trim().toLowerCase().includes('returning') || sql.trim().toLowerCase().startsWith('select')) {
      const rows = stmt.all(...args)
      return { rows, rowsAffected: 0 } // better-sqlite3 .all doesn't return changes
    } else {
      const info = stmt.run(...args)
      return { rows: [], rowsAffected: info.changes }
    }
  }

  async batch(sqls: string[]): Promise<void> {
    this.db.transaction(() => {
      for (const sql of sqls) {
        this.db.exec(sql)
      }
    })()
  }
}

/** 
 * Adapter for bun:sqlite (synchronous Bun).
 */
export class BunSqliteDriver implements QueueDriver {
  constructor(private db: any) {}

  async execute(sql: string, args: any[]): Promise<DriverResultSet> {
    const query = this.db.query(sql)
    
    if (sql.trim().toLowerCase().includes('returning') || sql.trim().toLowerCase().startsWith('select')) {
      const rows = query.all(...args)
      return { rows, rowsAffected: 0 }
    } else {
      const info = query.run(...args)
      return { rows: [], rowsAffected: info.changes }
    }
  }

  async batch(sqls: string[]): Promise<void> {
    this.db.transaction(() => {
      for (const sql of sqls) {
        this.db.run(sql)
      }
    })()
  }
}

/** 
 * Adapter for @libsql/client (asynchronous Turso).
 */
export class LibsqlDriver implements QueueDriver {
  constructor(private client: any) {}

  async execute(sql: string, args: any[]): Promise<DriverResultSet> {
    const res = await this.client.execute({ sql, args })
    return {
      rows: res.rows,
      rowsAffected: res.rowsAffected,
      lastInsertRowid: res.lastInsertRowid
    }
  }

  async batch(sqls: string[]): Promise<void> {
    await this.client.batch(sqls, 'write')
  }

  close(): void {
    this.client.close()
  }
}
