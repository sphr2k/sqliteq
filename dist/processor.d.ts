import type { Message, ID } from './types.js';
/**
 * Interface for both synchronous and asynchronous queue implementations.
 * This allows the Processor to work with better-sqlite3, bun:sqlite, and LibSQL/Turso.
 */
export interface IQueue<T> {
    readonly timeout: number;
    receive(): Promise<Message<T> | null>;
    extend(id: ID, received: number, delay: number): Promise<boolean>;
    delete(id: ID, received: number): Promise<boolean>;
}
export interface ProcessorOptions<T> {
    /** Handler function. Return normally = auto-delete. Throw = leave for retry. */
    handler: (msg: Message<T>) => void | Promise<void>;
    /** Poll interval in ms (default: 100) */
    pollInterval?: number;
    /** Max simultaneous handlers (default: 1) */
    concurrency?: number;
    /** Auto-extend interval in ms (default: queue's timeout value). Set to 0 to disable auto-extend. */
    extendInterval?: number;
    /** Called when an error occurs. Defaults to console.error. Pass () => {} to silence. */
    onError?: (error: unknown, context: {
        phase: 'handler' | 'delete' | 'extend' | 'receive';
        messageId: string;
    }) => void;
}
/**
 * Long-running consumer that polls, processes, and auto-deletes messages.
 */
export declare class Processor<T = string> {
    private queue;
    private handler;
    private pollInterval;
    private concurrency;
    private extendMs;
    private onError;
    private running;
    private stopping;
    private activeCount;
    private pollTimer;
    private stopPromise;
    private stopResolve;
    constructor(queue: IQueue<T>, options: ProcessorOptions<T>);
    /** Start polling for messages. Non-blocking — returns immediately. */
    start(): void;
    /** Stop polling and wait for in-flight handlers to finish. Resolves when all handlers have returned. */
    stop(): Promise<void>;
    private poll;
    private scheduleNext;
    private processMessage;
    private onComplete;
}
//# sourceMappingURL=processor.d.ts.map