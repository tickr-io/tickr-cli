-- Preserve migration 1 as deployed while upgrading calendar and replay lookups.
DROP INDEX IF EXISTS public.workflow_instances_workflow_id_idx;
DROP INDEX IF EXISTS public.workflow_instances_workflow_scheduled_idx;
CREATE INDEX workflow_instances_workflow_scheduled_idx
    ON public.workflow_instances USING btree (workflow_id, scheduled_at);

DROP INDEX IF EXISTS public.workflow_replays_source_idx;
CREATE INDEX workflow_replays_source_idx
    ON public.workflow_replays USING btree (source_instance_id, created_at DESC, replay_instance_id DESC);

DROP INDEX IF EXISTS public.workflow_replays_unsettled_idx;
CREATE INDEX workflow_replays_unsettled_idx
    ON public.workflow_replays USING btree (updated_at, replay_instance_id)
    WHERE (status = 'Materializing'::text);
