import type { WorkflowDetail } from './client';

type JsonObject = Record<string, unknown>;

const EDGE_KIND: Record<number, string> = {
  0: 'Control',
  1: 'Data',
  2: 'Loop',
};

const TASK_TYPE: Record<number, string> = {
  0: 'RegularTask',
  1: 'SensorTask',
  2: 'ShadowTask',
};

const WORKFLOW_STATUS: Record<number, string> = {
  0: 'Inactive',
  1: 'Active',
};

function object(value: unknown): JsonObject | undefined {
  return value != null && typeof value === 'object' && !Array.isArray(value)
    ? (value as JsonObject)
    : undefined;
}

function keyedById(value: unknown): Record<string, JsonObject> {
  if (Array.isArray(value)) {
    return Object.fromEntries(
      value.flatMap((entry) => {
        const item = object(entry);
        return item && typeof item.id === 'string' ? [[item.id, item] as const] : [];
      }),
    );
  }
  return object(value) as Record<string, JsonObject> | undefined ?? {};
}

function variantName(name: string): string {
  const names: Record<string, string> = {
    fire_now: 'FireNow',
    waits_on_signal: 'WaitsOnSignal',
    signal_received: 'SignalReceived',
    predicate_holds: 'PredicateHolds',
    timer_elapsed: 'TimerElapsed',
    on_success: 'OnSuccess',
    on_failure: 'OnFailure',
  };
  return names[name] ?? name;
}

function unwrapOneof(value: unknown): JsonObject | undefined {
  const wrapper = object(value);
  const oneof = object(wrapper?.kind) ?? object(wrapper?.emit);
  if (!oneof) return wrapper;
  const entry = Object.entries(oneof)[0];
  return entry ? { [variantName(entry[0])]: entry[1] } : undefined;
}

function normalizeTrigger(value: unknown): unknown {
  if (typeof value === 'string') return value;
  const trigger = unwrapOneof(value);
  if (!trigger) return value;
  if ('FireNow' in trigger) return 'FireNow';
  if ('Cron' in trigger) return { Cron: trigger.Cron };
  if ('cron' in trigger) return { Cron: trigger.cron };
  if ('WaitsOnSignal' in trigger) return { WaitsOnSignal: trigger.WaitsOnSignal };
  return trigger;
}

function normalizeGate(value: unknown): unknown {
  return unwrapOneof(value) ?? value;
}

function normalizeEmit(value: unknown): unknown {
  const emit = unwrapOneof(value);
  if (!emit) return value;
  const success = object(emit.OnSuccess);
  if (success) {
    return { signal: success.signal_name ?? 'emit', kind: 'on-success' };
  }
  const failure = object(emit.OnFailure);
  if (failure) {
    return { signal: failure.signal_name ?? 'emit', kind: 'on-failure' };
  }
  return emit;
}

/**
 * Convert the canonical protobuf JSON representation returned by the API into
 * the id-keyed, named-enum view consumed by the Console's definition and graph
 * renderers. The function is deliberately idempotent so retained historical
 * definitions in the older map/string representation keep rendering.
 */
export function normalizeWorkflowDefinition(value: unknown): JsonObject {
  const definition = object(value) ?? {};

  const tasks = Object.fromEntries(
    Object.entries(keyedById(definition.tasks)).map(([id, task]) => [
      id,
      {
        ...task,
        ...(typeof task.task_type === 'number'
          ? { task_type: TASK_TYPE[task.task_type] ?? task.task_type }
          : {}),
        ...(Array.isArray(task.emits) ? { emits: task.emits.map(normalizeEmit) } : {}),
      },
    ]),
  );

  const graph = object(definition.task_graph) ?? {};
  const edges = Object.fromEntries(
    Object.entries(keyedById(graph.edges)).map(([id, edge]) => [
      id,
      {
        ...edge,
        ...(typeof edge.kind === 'number'
          ? { kind: EDGE_KIND[edge.kind] ?? edge.kind }
          : {}),
        ...(Array.isArray(edge.gates) ? { gates: edge.gates.map(normalizeGate) } : {}),
      },
    ]),
  );

  return {
    ...definition,
    status:
      typeof definition.status === 'number'
        ? WORKFLOW_STATUS[definition.status] ?? definition.status
        : definition.status,
    trigger: normalizeTrigger(definition.trigger),
    tasks,
    task_graph: { ...graph, edges },
  };
}

export function normalizeWorkflowDetail(detail: WorkflowDetail): WorkflowDetail {
  return {
    ...detail,
    workflow_definition: normalizeWorkflowDefinition(detail.workflow_definition),
  };
}
