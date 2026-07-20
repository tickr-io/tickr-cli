import createClient from 'openapi-fetch';
import type { paths, components } from './types.gen';

export const api = createClient<paths>({
  baseUrl: '',
});

export type Workflow = components['schemas']['Workflow'];
export type WorkflowDetail = components['schemas']['WorkflowDetail'];
export type AvailableVersion = components['schemas']['AvailableVersion'];
export type Trigger = components['schemas']['Trigger'];
export type WorkflowInstance = components['schemas']['WorkflowInstance'];
export type InstanceSnapshot = components['schemas']['InstanceSnapshot'];
export type AppliedPatchView = components['schemas']['AppliedPatchView'];
export type PatchOpView = components['schemas']['PatchOpView'];
export type PatchSource = components['schemas']['PatchSource'];
export type SnapshotTaskDef = components['schemas']['SnapshotTaskDef'];
export type SnapshotTaskInstance = components['schemas']['SnapshotTaskInstance'];
export type SnapshotGraph = components['schemas']['SnapshotGraph'];
export type GateView = components['schemas']['GateView'];
export type InstanceContext = components['schemas']['InstanceContext'];
export type CtxEntry = components['schemas']['CtxEntry'];
export type RoutingValueView = components['schemas']['RoutingValueView'];
export type TaskInstance = components['schemas']['TaskInstance'];
export type ClockResponse = components['schemas']['ClockResponse'];
export type ClockInstance = components['schemas']['ClockInstance'];
export type CalendarResponse = components['schemas']['CalendarResponse'];
export type DayCounts = components['schemas']['DayCounts'];
export type UpcomingInstance = components['schemas']['UpcomingInstance'];
export type TaskLogs = components['schemas']['TaskLogs'];
export type TaskLogPage = components['schemas']['TaskLogBatchResponse'];
export type Event = components['schemas']['Event'];
export type TriggerProvenanceView = components['schemas']['TriggerProvenanceView'];
export type IdentityRef = components['schemas']['IdentityRef'];
export type ReplayResult = components['schemas']['ReplayResult'];
export type ReplayRow = components['schemas']['ReplayRow'];
export type TenantInfo = components['schemas']['TenantInfo'];

export class ApiError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
    this.name = 'ApiError';
  }
}

export async function unwrap<T>(p: Promise<{ data?: T; error?: unknown; response: Response }>): Promise<T> {
  const { data, error, response } = await p;
  if (error !== undefined || !response.ok) {
    const msg = typeof error === 'string' ? error : `HTTP ${response.status} ${response.statusText}`;
    throw new ApiError(response.status, msg);
  }
  return data as T;
}
