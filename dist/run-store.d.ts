import type { QueueDriver } from './types.js';
type JsonRecord = Record<string, unknown>;
type CounterMap = Record<string, number>;
export type RunStatus = 'completed' | 'completed_with_failures' | 'failed' | 'queued' | 'running';
export type RunEventLevel = 'error' | 'info' | 'ok';
export type RunSnapshot = {
    runId: string;
    queue: string;
    status: RunStatus;
    scope: JsonRecord | null;
    counters: CounterMap;
    createdAt: Date;
    updatedAt: Date;
    expiresAt: Date;
};
export type RunEvent = {
    id: string;
    runId: string;
    kind: string;
    level: RunEventLevel;
    message: string;
    subjectKey: string | null;
    payload: unknown;
    createdAt: Date;
};
export type CreateRunInput = {
    runId: string;
    queue: string;
    createdAt: Date;
    expiresAt: Date;
    counters?: CounterMap;
    scope?: JsonRecord;
    status?: RunStatus;
};
export type RecordRunUpdateInput = {
    runId: string;
    event?: {
        kind: string;
        level: RunEventLevel;
        message: string;
        payload?: unknown;
        subjectKey?: string;
    };
    idempotencyKey: string;
    now: Date;
    deltas?: CounterMap;
    status?: RunStatus;
};
export declare class RunStore {
    private readonly driver;
    constructor(driver: QueueDriver);
    init(): Promise<void>;
    createRun(input: CreateRunInput): Promise<void>;
    getRunSnapshot(runId: string): Promise<RunSnapshot | null>;
    record(input: RecordRunUpdateInput): Promise<boolean>;
    listRunEvents(runId: string): Promise<RunEvent[]>;
    pruneExpiredRuns(now: Date): Promise<{
        events: number;
        runs: number;
    }>;
}
export {};
//# sourceMappingURL=run-store.d.ts.map