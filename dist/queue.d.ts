import type { ID, Message, SendOptions, QueueOptions, QueueDriver, QueueStats, RequeueDeadLettersOptions } from './types.js';
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
export declare class Queue<T = string> {
    private driver;
    private name;
    /** Visibility timeout in ms. Exposed for Processor to read. */
    readonly timeout: number;
    private maxReceive;
    private maxBodyBytes;
    constructor(driver: QueueDriver, name: string, options?: QueueOptions);
    /**
     * Initialize the queue schema. This is idempotent and can be called safely
     * multiple times or across different queues sharing the same database.
     */
    init(): Promise<void>;
    /**
     * Send a message to the queue. Returns the message ID.
     *
     * Body is JSON-serialized. T must be JSON-round-trippable.
     * Date, Map, Set, and other non-POJO types will not survive serialization.
     */
    send(body: T, options?: SendOptions): Promise<ID>;
    /**
     * Send multiple messages in a single transaction.
     * If any message fails validation, the entire batch is rolled back.
     * @returns An array of message IDs in the same order as the input.
     */
    sendBatch(messages: Array<{
        body: T;
        options?: SendOptions;
    }>): Promise<ID[]>;
    /**
     * Atomically claim the next available message.
     * The message becomes invisible to other consumers for `timeout` ms.
     * @returns The message, or `null` if the queue is empty or all messages are in-flight.
     */
    receive(): Promise<Message<T> | null>;
    /**
     * Atomically claim up to `limit` available messages.
     * Returns fewer than requested when the queue runs dry.
     */
    receiveBatch(limit: number): Promise<Message<T>[]>;
    /**
     * Extend a message's visibility timeout by `delay` ms from now.
     * @param id - Message ID from {@link Message.id}.
     * @param received - Receive count from {@link Message.received} (used as a fencing token).
     * @param delay - Additional visibility time in ms.
     * @returns `true` if extended, `false` if the message was already re-delivered (stale handle).
     */
    extend(id: ID, received: number, delay: number): Promise<boolean>;
    /**
     * Acknowledge and remove a message from the queue.
     * @param id - Message ID from {@link Message.id}.
     * @param received - Receive count from {@link Message.received} (used as a fencing token).
     * @returns `true` if deleted, `false` if the message was already re-delivered (stale handle — safe no-op).
     */
    delete(id: ID, received: number): Promise<boolean>;
    /** Number of messages in this queue (all states). */
    size(): Promise<number>;
    /** Queue counts grouped by operational state. */
    stats(): Promise<QueueStats>;
    /** Remove all messages from this queue. @returns Count of messages deleted. */
    purge(): Promise<number>;
    /**
     * Requeue all currently dead-lettered messages as fresh messages.
     * Requeued messages get new IDs so stale handles from previous deliveries remain invalid.
     * @returns The new message IDs in FIFO order.
     */
    requeueDeadLetters(options?: RequeueDeadLettersOptions): Promise<ID[]>;
    /** Remove all currently dead-lettered messages. @returns Count of messages deleted. */
    purgeDeadLetters(): Promise<number>;
    /** Get messages that exceeded `maxReceive` and will never be delivered again. Inspect or purge these periodically. */
    deadLetters(): Promise<Message<T>[]>;
}
//# sourceMappingURL=queue.d.ts.map