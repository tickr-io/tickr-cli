import { describe, it, expect } from 'vitest';
import { taskTypeView } from './taskType';

describe('taskTypeView', () => {
  it('maps each true enum variant to its own visual', () => {
    expect(taskTypeView('RegularTask')).toMatchObject({ cls: 'tt-regular', label: 'Regular' });
    expect(taskTypeView('SensorTask')).toMatchObject({ cls: 'tt-sensor', label: 'Sensor' });
    expect(taskTypeView('ShadowTask')).toMatchObject({ cls: 'tt-shadow', label: 'Shadow' });
  });

  it('a ShadowTask reads visibly as an externally-asserted node', () => {
    const view = taskTypeView('ShadowTask');
    expect(view.label).toBe('Shadow');
    expect(view.title.toLowerCase()).toContain('externally asserted');
  });

  it('an unknown or absent value defaults to Regular', () => {
    expect(taskTypeView(undefined)).toMatchObject({ label: 'Regular' });
    expect(taskTypeView('')).toMatchObject({ label: 'Regular' });
  });

  it('the corrupted legacy "AddTask" string no longer resolves to a real kind', () => {
    // Regression: the pre-de-slop wire carried task_type = "AddTask" and the
    // view coerced it to the default; RegularTask is now read from the true
    // enum, and "AddTask" is not a variant, so it is not a recognised kind.
    expect(taskTypeView('AddTask')).toMatchObject({ label: 'Regular' });
  });
});
