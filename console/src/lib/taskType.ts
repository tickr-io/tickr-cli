/**
 * Single source of truth for how the three task kinds render (DC-0019).
 *
 * The wire carries the true `task_type` enum variant name — `RegularTask` /
 * `SensorTask` / `ShadowTask` — so the kind is matched *exactly*, never guessed
 * from a substring. A `ShadowTask` (externally asserted, never dispatched) must
 * read visibly as its own kind, not silently collapse into the default. An
 * unknown or absent value reads as the default `RegularTask`.
 */

export type TaskTypeName = 'RegularTask' | 'SensorTask' | 'ShadowTask';

export interface TaskTypeView {
  /** Pill token class — resolves to a `.tt-*` rule in `index.css`. */
  cls: string;
  /** Short pill label. */
  label: string;
  /** Hover tooltip spelling out what the kind means. */
  title: string;
}

const VIEWS: Record<TaskTypeName, TaskTypeView> = {
  RegularTask: {
    cls: 'tt-regular',
    label: 'Regular',
    title: 'Regular task — dispatched to an executor.',
  },
  SensorTask: {
    cls: 'tt-sensor',
    label: 'Sensor',
    title: 'Sensor task — continuous polling.',
  },
  ShadowTask: {
    cls: 'tt-shadow',
    label: 'Shadow',
    title: 'Externally asserted — grounded by an outside assertion, never dispatched to an executor.',
  },
};

/** Map a wire `task_type` enum value onto its kit visual. Unknown → Regular. */
export function taskTypeView(raw?: string): TaskTypeView {
  if (raw && Object.prototype.hasOwnProperty.call(VIEWS, raw)) {
    return VIEWS[raw as TaskTypeName];
  }
  return VIEWS.RegularTask;
}
