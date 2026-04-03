import type { QueueDriver } from './types.js';
export type ScheduleWindowStatus = 'queued' | 'running' | 'completed' | 'failed';
export type ScheduleWindow = {
    scheduleKey: string;
    windowKey: string;
    status: ScheduleWindowStatus;
    runId: string | null;
    enqueuedAt: Date;
    startedAt: Date | null;
    finishedAt: Date | null;
    summary: unknown;
};
export type ClaimWindowOptions = {
    /** Arbitrary metadata stored at claim time (JSON-serialised). */
    summary?: unknown;
    /** Associate a runId at claim time (can also be set later via markWindowRunning). */
    runId?: string;
};
export type MarkWindowFinishedOptions = {
    /** Arbitrary result/summary to store (JSON-serialised). */
    summary?: unknown;
};
/**
 * Generic schedule-window ledger.
 *
 * Tracks one row per (scheduleKey, windowKey) pair. The composite primary key
 * makes `claimWindow` a safe, atomic dedupe operation — calling it twice for
 * the same window is a no-op and returns `false` on the second call.
 *
 * Intended use-cases:
 *  - Periodic job scheduler: scheduleKey = job name, windowKey = ISO period
 *    (e.g. "2024-W12", "2024-03-15T00:00Z").
 *  - Any system that needs to track "has this window been processed?" without
 *    coupling to queue message IDs.
 */
export declare class ScheduleStore {
    private readonly driver;
    constructor(driver: QueueDriver);
    init(): Promise<void>;
    /**
     * Atomically claim a schedule window. Safe to call concurrently — only the
     * first caller succeeds; subsequent calls for the same (scheduleKey, windowKey)
     * are no-ops and return `false`.
     *
     * @returns `true` if the window was newly created, `false` if it already existed.
     */
    claimWindow(scheduleKey: string, windowKey: string, options?: ClaimWindowOptions): Promise<boolean>;
    /**
     * Retrieve a single window entry.
     * @returns The window, or `null` if it has never been claimed.
     */
    getWindow(scheduleKey: string, windowKey: string): Promise<ScheduleWindow | null>;
    /**
     * List windows for a schedule key, newest first.
     * @param limit Max rows to return (default: 50).
     */
    listWindows(scheduleKey: string, limit?: number): Promise<ScheduleWindow[]>;
    /**
     * Transition a window from `queued` → `running`.
     * Associates a runId and records the startedAt timestamp.
     *
     * @returns `true` if updated, `false` if the window was not in `queued` state
     *          (already running, finished, or non-existent).
     */
    markWindowRunning(scheduleKey: string, windowKey: string, runId: string): Promise<boolean>;
    /**
     * Transition a window from `running` → `completed` or `failed`.
     * Records the finishedAt timestamp and optional summary.
     *
     * @returns `true` if updated, `false` if the window was not in `running` state.
     */
    markWindowFinished(scheduleKey: string, windowKey: string, status: 'completed' | 'failed', options?: MarkWindowFinishedOptions): Promise<boolean>;
}
//# sourceMappingURL=schedule-store.d.ts.map