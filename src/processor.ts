import type { Message, ID } from './types.js'

/** 
 * Interface for both synchronous and asynchronous queue implementations.
 * This allows the Processor to work with better-sqlite3, bun:sqlite, and LibSQL/Turso.
 */
export interface IQueue<T> {
  readonly timeout: number
  receive(): Promise<Message<T> | null>
  extend(id: ID, received: number, delay: number): Promise<boolean>
  delete(id: ID, received: number): Promise<boolean>
}

export interface ProcessorOptions<T> {
  /** Handler function. Return normally = auto-delete. Throw = leave for retry. */
  handler: (msg: Message<T>) => void | Promise<void>
  /** Poll interval in ms (default: 100) */
  pollInterval?: number
  /** Max simultaneous handlers (default: 1) */
  concurrency?: number
  /** Auto-extend interval in ms (default: queue's timeout value). Set to 0 to disable auto-extend. */
  extendInterval?: number
  /** Called when an error occurs. Defaults to console.error. Pass () => {} to silence. */
  onError?: (error: unknown, context: { phase: 'handler' | 'delete' | 'extend' | 'receive'; messageId: string }) => void
}

function defaultOnError(error: unknown, context: { phase: string; messageId: string }): void {
  console.error(`[sqliteq] ${context.phase} error for message ${context.messageId}:`, error)
}

/** Call onError safely — never let a user callback crash the processor. */
function safeOnError(
  fn: NonNullable<ProcessorOptions<unknown>['onError']>,
  error: unknown,
  context: { phase: 'handler' | 'delete' | 'extend' | 'receive'; messageId: string },
): void {
  try {
    fn(error, context)
  } catch {
    // onError itself threw — nothing we can do, swallow to protect the processor
  }
}

/**
 * Long-running consumer that polls, processes, and auto-deletes messages.
 */
export class Processor<T = string> {
  private queue: IQueue<T>
  private handler: (msg: Message<T>) => void | Promise<void>
  private pollInterval: number
  private concurrency: number
  private extendMs: number
  private onError: NonNullable<ProcessorOptions<T>['onError']>

  private running = false
  private stopping = false
  private activeCount = 0
  private pollTimer: ReturnType<typeof setTimeout> | null = null
  private stopPromise: Promise<void> | null = null
  private stopResolve: (() => void) | null = null

  constructor(queue: IQueue<T>, options: ProcessorOptions<T>) {
    this.queue = queue
    this.handler = options.handler
    this.pollInterval = options.pollInterval ?? 100
    this.concurrency = options.concurrency ?? 1
    this.extendMs = options.extendInterval ?? queue.timeout
    this.onError = options.onError ?? defaultOnError

    if (!Number.isFinite(this.pollInterval) || this.pollInterval < 1) {
      throw new Error('pollInterval must be a finite number >= 1')
    }
    if (!Number.isFinite(this.concurrency) || this.concurrency < 1) {
      throw new Error('concurrency must be a finite number >= 1')
    }
    if (!Number.isFinite(this.extendMs) || this.extendMs < 0) {
      throw new Error('extendInterval must be a finite number >= 0')
    }
  }

  /** Start polling for messages. Non-blocking — returns immediately. */
  start(): void {
    if (this.running || this.stopping) return
    this.running = true
    this.scheduleNext()
  }

  /** Stop polling and wait for in-flight handlers to finish. Resolves when all handlers have returned. */
  stop(): Promise<void> {
    // If already stopping, return the same promise
    if (this.stopping && this.stopPromise) return this.stopPromise
    if (!this.running) return Promise.resolve()

    this.running = false
    this.stopping = true
    if (this.pollTimer) {
      clearTimeout(this.pollTimer)
      this.pollTimer = null
    }
    if (this.activeCount === 0) {
      this.stopping = false
      return Promise.resolve()
    }
    this.stopPromise = new Promise<void>((resolve) => {
      this.stopResolve = resolve
    })
    return this.stopPromise
  }

  private async poll(): Promise<void> {
    if (!this.running) return

    // Iterative loop to fill concurrency slots (avoids recursive stack overflow)
    while (this.running && this.activeCount < this.concurrency) {
      let msg: Message<T> | null
      try {
        msg = await this.queue.receive()
      } catch (err) {
        safeOnError(this.onError, err, { phase: 'receive', messageId: '' })
        break
      }

      if (!msg) break

      this.activeCount++
      this.processMessage(msg)
    }

    this.scheduleNext()
  }

  private scheduleNext(): void {
    if (!this.running) return
    this.pollTimer = setTimeout(() => this.poll(), this.pollInterval)
  }

  private async processMessage(msg: Message<T>): Promise<void> {
    // Auto-extend timer (fires at 4/5 of extend interval, matching goqite)
    let extendTimer: ReturnType<typeof setInterval> | null = null
    if (this.extendMs > 0) {
      const extendDelay = Math.max(1, this.extendMs - Math.floor(this.extendMs / 5))
      extendTimer = setInterval(async () => {
        try {
          const ok = await this.queue.extend(msg.id, msg.received, this.extendMs)
          if (!ok) {
            // Message was redelivered to another consumer — stop extending
            if (extendTimer) {
              clearInterval(extendTimer)
              extendTimer = null
            }
            safeOnError(this.onError, new Error('extend failed: message was redelivered (stale handle)'), { phase: 'extend', messageId: msg.id })
          }
        } catch (err) {
          safeOnError(this.onError, err, { phase: 'extend', messageId: msg.id })
        }
      }, extendDelay)
    }

    let handlerSucceeded = false
    try {
      await this.handler(msg)
      handlerSucceeded = true
    } catch (err) {
      safeOnError(this.onError, err, { phase: 'handler', messageId: msg.id })
    } finally {
      if (extendTimer) {
        clearInterval(extendTimer)
        extendTimer = null
      }
    }

    // Only delete on success — a failed handler leaves the message for retry after timeout
    if (handlerSucceeded) {
      try {
        const ok = await this.queue.delete(msg.id, msg.received)
        if (!ok) {
          safeOnError(this.onError, new Error('delete failed: message was redelivered (stale handle)'), { phase: 'delete', messageId: msg.id })
        }
      } catch (err) {
        safeOnError(this.onError, err, { phase: 'delete', messageId: msg.id })
      }
    }

    this.onComplete()
  }

  private onComplete(): void {
    this.activeCount--

    if (this.stopping && this.activeCount === 0 && this.stopResolve) {
      this.stopResolve()
      this.stopResolve = null
      this.stopPromise = null
      this.stopping = false
    }
    // Don't call poll() here — scheduleNext() in poll() handles the next cycle
  }
}
