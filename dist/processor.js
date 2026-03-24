function defaultOnError(error, context) {
    console.error(`[sqliteq] ${context.phase} error for message ${context.messageId}:`, error);
}
/** Call onError safely — never let a user callback crash the processor. */
function safeOnError(fn, error, context) {
    try {
        fn(error, context);
    }
    catch {
        // onError itself threw — nothing we can do, swallow to protect the processor
    }
}
/**
 * Long-running consumer that polls, processes, and auto-deletes messages.
 */
export class Processor {
    queue;
    handler;
    pollInterval;
    concurrency;
    extendMs;
    onError;
    running = false;
    stopping = false;
    activeCount = 0;
    pollTimer = null;
    stopPromise = null;
    stopResolve = null;
    constructor(queue, options) {
        this.queue = queue;
        this.handler = options.handler;
        this.pollInterval = options.pollInterval ?? 100;
        this.concurrency = options.concurrency ?? 1;
        this.extendMs = options.extendInterval ?? queue.timeout;
        this.onError = options.onError ?? defaultOnError;
        if (!Number.isFinite(this.pollInterval) || this.pollInterval < 1) {
            throw new Error('pollInterval must be a finite number >= 1');
        }
        if (!Number.isFinite(this.concurrency) || this.concurrency < 1) {
            throw new Error('concurrency must be a finite number >= 1');
        }
        if (!Number.isFinite(this.extendMs) || this.extendMs < 0) {
            throw new Error('extendInterval must be a finite number >= 0');
        }
    }
    /** Start polling for messages. Non-blocking — returns immediately. */
    start() {
        if (this.running || this.stopping)
            return;
        this.running = true;
        this.scheduleNext();
    }
    /** Stop polling and wait for in-flight handlers to finish. Resolves when all handlers have returned. */
    stop() {
        // If already stopping, return the same promise
        if (this.stopping && this.stopPromise)
            return this.stopPromise;
        if (!this.running)
            return Promise.resolve();
        this.running = false;
        this.stopping = true;
        if (this.pollTimer) {
            clearTimeout(this.pollTimer);
            this.pollTimer = null;
        }
        if (this.activeCount === 0) {
            this.stopping = false;
            return Promise.resolve();
        }
        this.stopPromise = new Promise((resolve) => {
            this.stopResolve = resolve;
        });
        return this.stopPromise;
    }
    async poll() {
        if (!this.running)
            return;
        // Iterative loop to fill concurrency slots (avoids recursive stack overflow)
        while (this.running && this.activeCount < this.concurrency) {
            let msg;
            try {
                msg = await this.queue.receive();
            }
            catch (err) {
                safeOnError(this.onError, err, { phase: 'receive', messageId: '' });
                break;
            }
            if (!msg)
                break;
            this.activeCount++;
            this.processMessage(msg);
        }
        this.scheduleNext();
    }
    scheduleNext() {
        if (!this.running)
            return;
        this.pollTimer = setTimeout(() => this.poll(), this.pollInterval);
    }
    async processMessage(msg) {
        // Auto-extend timer (fires at 4/5 of extend interval, matching goqite)
        let extendTimer = null;
        if (this.extendMs > 0) {
            const extendDelay = Math.max(1, this.extendMs - Math.floor(this.extendMs / 5));
            extendTimer = setInterval(async () => {
                try {
                    const ok = await this.queue.extend(msg.id, msg.received, this.extendMs);
                    if (!ok) {
                        // Message was redelivered to another consumer — stop extending
                        if (extendTimer) {
                            clearInterval(extendTimer);
                            extendTimer = null;
                        }
                        safeOnError(this.onError, new Error('extend failed: message was redelivered (stale handle)'), { phase: 'extend', messageId: msg.id });
                    }
                }
                catch (err) {
                    safeOnError(this.onError, err, { phase: 'extend', messageId: msg.id });
                }
            }, extendDelay);
        }
        let handlerSucceeded = false;
        try {
            await this.handler(msg);
            handlerSucceeded = true;
        }
        catch (err) {
            safeOnError(this.onError, err, { phase: 'handler', messageId: msg.id });
        }
        finally {
            if (extendTimer) {
                clearInterval(extendTimer);
                extendTimer = null;
            }
        }
        // Only delete on success — a failed handler leaves the message for retry after timeout
        if (handlerSucceeded) {
            try {
                const ok = await this.queue.delete(msg.id, msg.received);
                if (!ok) {
                    safeOnError(this.onError, new Error('delete failed: message was redelivered (stale handle)'), { phase: 'delete', messageId: msg.id });
                }
            }
            catch (err) {
                safeOnError(this.onError, err, { phase: 'delete', messageId: msg.id });
            }
        }
        this.onComplete();
    }
    onComplete() {
        this.activeCount--;
        if (this.stopping && this.activeCount === 0 && this.stopResolve) {
            this.stopResolve();
            this.stopResolve = null;
            this.stopPromise = null;
            this.stopping = false;
        }
        // Don't call poll() here — scheduleNext() in poll() handles the next cycle
    }
}
//# sourceMappingURL=processor.js.map