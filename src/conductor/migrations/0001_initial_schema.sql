-- Initial Tickr data-plane schema.
-- New installations apply this baseline atomically through sqlx.

CREATE TABLE public.events (
    seq bigint NOT NULL,
    id uuid NOT NULL,
    ts timestamp with time zone NOT NULL,
    event_type text NOT NULL,
    payload jsonb NOT NULL,
    archived_at timestamp with time zone NOT NULL
);

CREATE SEQUENCE public.events_seq_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.events_seq_seq OWNED BY public.events.seq;

CREATE TABLE public.signal_cancels (
    signal_id uuid NOT NULL,
    applied_count integer NOT NULL,
    target jsonb NOT NULL,
    note text,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.signal_captures (
    signal_id uuid NOT NULL,
    workflow_id uuid NOT NULL,
    captures jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    materialized_run_id uuid,
    terminal_at timestamp with time zone,
    workflow_version bigint
);

CREATE TABLE public.signal_wakeups (
    signal_id uuid NOT NULL,
    name text NOT NULL,
    matched_workflows integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.task_instances (
    id uuid NOT NULL,
    workflow_instance_id uuid NOT NULL,
    workflow_id uuid NOT NULL,
    task_id uuid NOT NULL,
    name text NOT NULL,
    state text NOT NULL,
    archived_at timestamp with time zone DEFAULT now() NOT NULL,
    task_instance jsonb NOT NULL,
    attempt integer DEFAULT 0 NOT NULL
);

CREATE TABLE public.task_specs (
    task_id uuid NOT NULL,
    routing_vars jsonb DEFAULT '[]'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.workflow_instances (
    id uuid NOT NULL,
    workflow_id uuid NOT NULL,
    name text NOT NULL,
    state text NOT NULL,
    scheduled_at timestamp with time zone,
    archived_at timestamp with time zone DEFAULT now() NOT NULL,
    instance jsonb NOT NULL
);

CREATE TABLE public.workflow_patch_discrepancies (
    workflow_instance_id uuid NOT NULL,
    patch_key uuid NOT NULL,
    ledger_status text NOT NULL,
    detail text NOT NULL,
    detected_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.workflow_patch_task_builds (
    patch_key uuid NOT NULL,
    task_id uuid NOT NULL,
    status text DEFAULT 'pending'::text NOT NULL,
    error text,
    pending_since timestamp with time zone DEFAULT now() NOT NULL,
    built_at timestamp with time zone,
    CONSTRAINT workflow_patch_task_builds_status_check CHECK ((status = ANY (ARRAY['pending'::text, 'success'::text, 'failure'::text])))
);

CREATE TABLE public.workflow_patches (
    patch_key uuid NOT NULL,
    patch_id uuid NOT NULL,
    workflow_instance_id uuid NOT NULL,
    status text NOT NULL,
    ops jsonb NOT NULL,
    reason text,
    outcome text,
    applied_version bigint,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    provenance text DEFAULT 'external'::text NOT NULL,
    source text,
    source_format text,
    operation jsonb,
    CONSTRAINT workflow_patches_provenance_check CHECK ((provenance = ANY (ARRAY['self'::text, 'external'::text]))),
    CONSTRAINT workflow_patches_source_format_check CHECK ((source_format = ANY (ARRAY['nickel'::text, 'json'::text]))),
    CONSTRAINT workflow_patches_status_check CHECK ((status = ANY (ARRAY['Validating'::text, 'Building'::text, 'Submitted'::text, 'Applied'::text, 'Rejected'::text, 'BuildFailed'::text])))
);

CREATE TABLE public.workflow_replays (
    replay_instance_id uuid NOT NULL,
    source_instance_id uuid NOT NULL,
    signal_id uuid NOT NULL,
    idempotency_key text,
    status text NOT NULL,
    resume_from jsonb DEFAULT '[]'::jsonb NOT NULL,
    pre_grounded jsonb DEFAULT '[]'::jsonb NOT NULL,
    name text,
    seed_sha256 text,
    outcome text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    shadowed_keys jsonb DEFAULT '[]'::jsonb NOT NULL,
    CONSTRAINT workflow_replays_status_check CHECK ((status = ANY (ARRAY['Materializing'::text, 'Released'::text, 'VersionUnresolvable'::text])))
);

CREATE TABLE public.workflow_run_info (
    workflow_instance_id uuid NOT NULL,
    ctx_envelope jsonb DEFAULT '[]'::jsonb NOT NULL,
    runtime_params jsonb DEFAULT '{}'::jsonb NOT NULL,
    log_uris jsonb DEFAULT '{}'::jsonb NOT NULL,
    enriched_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.workflow_task_builds (
    workflow_id uuid NOT NULL,
    workflow_version bigint NOT NULL,
    task_id uuid NOT NULL,
    status text DEFAULT 'pending'::text NOT NULL,
    error text,
    pending_since timestamp with time zone DEFAULT now() NOT NULL,
    built_at timestamp with time zone
);

CREATE TABLE public.workflows (
    id uuid NOT NULL,
    name text NOT NULL,
    definition jsonb NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    version bigint NOT NULL,
    status text DEFAULT 'Building'::text NOT NULL,
    nickel_source text NOT NULL,
    namespace text NOT NULL,
    slug text NOT NULL,
    content_hash text NOT NULL,
    cosmetic_hash text NOT NULL
);

COMMENT ON COLUMN public.workflows.definition IS 'Published tickr.workflow protobuf workflow-definition contract, JSON-encoded.';

ALTER TABLE ONLY public.events ALTER COLUMN seq SET DEFAULT nextval('public.events_seq_seq'::regclass);

ALTER TABLE ONLY public.events
    ADD CONSTRAINT events_id_key UNIQUE (id);

ALTER TABLE ONLY public.events
    ADD CONSTRAINT events_pkey PRIMARY KEY (seq);

ALTER TABLE ONLY public.signal_cancels
    ADD CONSTRAINT signal_cancels_pkey PRIMARY KEY (signal_id);

ALTER TABLE ONLY public.signal_captures
    ADD CONSTRAINT signal_captures_pkey PRIMARY KEY (signal_id);

ALTER TABLE ONLY public.signal_wakeups
    ADD CONSTRAINT signal_wakeups_pkey PRIMARY KEY (signal_id);

ALTER TABLE ONLY public.task_instances
    ADD CONSTRAINT task_instances_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.task_specs
    ADD CONSTRAINT task_specs_pkey PRIMARY KEY (task_id);

ALTER TABLE ONLY public.workflow_instances
    ADD CONSTRAINT workflow_instances_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.workflow_patch_discrepancies
    ADD CONSTRAINT workflow_patch_discrepancies_pkey PRIMARY KEY (workflow_instance_id, patch_key);

ALTER TABLE ONLY public.workflow_patch_task_builds
    ADD CONSTRAINT workflow_patch_task_builds_pkey PRIMARY KEY (patch_key, task_id);

ALTER TABLE ONLY public.workflow_patches
    ADD CONSTRAINT workflow_patches_pkey PRIMARY KEY (patch_key);

ALTER TABLE ONLY public.workflow_replays
    ADD CONSTRAINT workflow_replays_pkey PRIMARY KEY (replay_instance_id);

ALTER TABLE ONLY public.workflow_run_info
    ADD CONSTRAINT workflow_run_info_pkey PRIMARY KEY (workflow_instance_id);

ALTER TABLE ONLY public.workflow_task_builds
    ADD CONSTRAINT workflow_task_builds_pkey PRIMARY KEY (workflow_id, workflow_version, task_id);

ALTER TABLE ONLY public.workflows
    ADD CONSTRAINT workflows_pkey PRIMARY KEY (id, version);

CREATE INDEX events_archived_at_id_idx ON public.events USING btree (archived_at, id);

CREATE INDEX signal_captures_materialized_run_id_idx ON public.signal_captures USING btree (materialized_run_id) WHERE (materialized_run_id IS NOT NULL);

CREATE INDEX signal_captures_terminal_at_idx ON public.signal_captures USING btree (terminal_at) WHERE (terminal_at IS NOT NULL);

CREATE INDEX task_instances_wf_inst_task_attempt_idx ON public.task_instances USING btree (workflow_instance_id, task_id, attempt);

CREATE INDEX task_instances_workflow_id_idx ON public.task_instances USING btree (workflow_id);

CREATE INDEX task_instances_workflow_instance_id_idx ON public.task_instances USING btree (workflow_instance_id);

CREATE INDEX workflow_instances_state_archived_at_idx ON public.workflow_instances USING btree (state, archived_at DESC);

CREATE INDEX workflow_instances_workflow_scheduled_idx ON public.workflow_instances USING btree (workflow_id, scheduled_at);

CREATE INDEX workflow_patch_discrepancies_detected_idx ON public.workflow_patch_discrepancies USING btree (detected_at DESC);

CREATE INDEX workflow_patches_unsettled_idx ON public.workflow_patches USING btree (workflow_instance_id) WHERE (status = ANY (ARRAY['Validating'::text, 'Building'::text, 'Submitted'::text]));

CREATE UNIQUE INDEX workflow_replays_idempotency_idx ON public.workflow_replays USING btree (source_instance_id, idempotency_key) WHERE (idempotency_key IS NOT NULL);

CREATE INDEX workflow_replays_source_idx ON public.workflow_replays USING btree (source_instance_id, created_at DESC, replay_instance_id DESC);

CREATE INDEX workflow_replays_unsettled_idx ON public.workflow_replays USING btree (updated_at, replay_instance_id) WHERE (status = 'Materializing'::text);

CREATE INDEX workflow_task_builds_workflow_idx ON public.workflow_task_builds USING btree (workflow_id, workflow_version);

CREATE INDEX workflows_definition_tasks_gin ON public.workflows USING gin (((definition -> 'tasks'::text)));

ALTER TABLE ONLY public.task_instances
    ADD CONSTRAINT task_instances_workflow_instance_id_fkey FOREIGN KEY (workflow_instance_id) REFERENCES public.workflow_instances(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.workflow_patch_task_builds
    ADD CONSTRAINT workflow_patch_task_builds_patch_key_fkey FOREIGN KEY (patch_key) REFERENCES public.workflow_patches(patch_key) ON DELETE CASCADE;

ALTER TABLE ONLY public.workflow_run_info
    ADD CONSTRAINT workflow_run_info_workflow_instance_id_fkey FOREIGN KEY (workflow_instance_id) REFERENCES public.workflow_instances(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.workflow_task_builds
    ADD CONSTRAINT workflow_task_builds_workflow_id_workflow_version_fkey FOREIGN KEY (workflow_id, workflow_version) REFERENCES public.workflows(id, version) ON DELETE CASCADE;
