//! Backend-neutral claim admission for SQL-authoritative lifecycle work.
//!
//! Reconciler scans remain active while a formation fence is closed. The
//! selected coordination adapter controls only the boundary that acquires an
//! expiring SQL lease; notification delivery never carries claim authority.

use std::{num::NonZeroUsize, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt as _;
use tokio_util::sync::CancellationToken;

use crate::{
    build_pipeline::{
        definition_build_notifications, DefinitionBuildNotificationStream, DefinitionBuildNotifier,
        BUILD_QUEUE_GROUP, BUILD_QUEUE_SUBJECT,
    },
    patch_pipeline::{
        local::{patch_work_notifications, PatchWorkNotificationStream, PatchWorkNotifier},
        PATCH_BUILD_QUEUE_GROUP, PATCH_BUILD_QUEUE_SUBJECT,
    },
    submission_consumer::{
        definition_submission_notifications, DefinitionSubmissionNotificationStream,
        DefinitionSubmissionNotifier, SUBMISSION_QUEUE_GROUP, SUBMISSION_QUEUE_SUBJECT,
    },
};

/// The three SQL-authoritative lifecycle pipelines that accept advisory wakeups.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum LifecyclePipeline {
    DefinitionBuild,
    PatchBuild,
    Submission,
}

impl LifecyclePipeline {
    pub const ALL: [Self; 3] = [Self::DefinitionBuild, Self::PatchBuild, Self::Submission];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DefinitionBuild => "definition-build",
            Self::PatchBuild => "patch-build",
            Self::Submission => "submission",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|pipeline| pipeline.as_str() == value)
    }
}

/// Formation-owned admission check immediately before an SQL lease operation.
pub trait LifecycleClaimAdmission: Send + Sync {
    fn claims_open(&self, pipeline: LifecyclePipeline) -> bool;
}

/// Local and all-NATS formations do not have the Redis capability fence.
#[derive(Clone, Copy, Debug, Default)]
pub struct OpenLifecycleClaims;

impl LifecycleClaimAdmission for OpenLifecycleClaims {
    fn claims_open(&self, _pipeline: LifecyclePipeline) -> bool {
        true
    }
}

/// Formation-selected source of advisory lifecycle wakeups.
///
/// Implementations own their substrate client and protocol resources. SQL
/// reconcilers receive only the bounded streams fed through [`LifecycleWakeups`].
#[async_trait]
pub trait LifecycleWakeupSource: Send {
    async fn run(&mut self, wakeups: LifecycleWakeups, cancel: CancellationToken) -> Result<()>;
}

/// Bounded, role-specific wakeups shared with capability reconstruction.
#[derive(Clone)]
pub struct LifecycleWakeups {
    definition_build: DefinitionBuildNotifier,
    patch: PatchWorkNotifier,
    submission: DefinitionSubmissionNotifier,
}

impl LifecycleWakeups {
    pub fn notify(&self, pipeline: LifecyclePipeline) -> bool {
        match pipeline {
            LifecyclePipeline::DefinitionBuild => self.definition_build.notify(),
            LifecyclePipeline::PatchBuild => self.patch.notify(),
            LifecyclePipeline::Submission => self.submission.notify(),
        }
    }
}

/// Notification and claim-admission interfaces consumed by SQL reconcilers.
pub struct LifecycleReconcilerInputs {
    definition_build: DefinitionBuildNotificationStream,
    patch: PatchWorkNotificationStream,
    submission: DefinitionSubmissionNotificationStream,
    claim_admission: Arc<dyn LifecycleClaimAdmission>,
}

impl LifecycleReconcilerInputs {
    pub fn into_parts(
        self,
    ) -> (
        DefinitionBuildNotificationStream,
        PatchWorkNotificationStream,
        DefinitionSubmissionNotificationStream,
        Arc<dyn LifecycleClaimAdmission>,
    ) {
        (
            self.definition_build,
            self.patch,
            self.submission,
            self.claim_admission,
        )
    }
}

/// One admitted LifecycleWork role ready for Conductor composition.
pub struct LifecycleWork {
    source: Box<dyn LifecycleWakeupSource>,
    wakeups: LifecycleWakeups,
    reconciler_inputs: LifecycleReconcilerInputs,
}

impl LifecycleWork {
    pub fn new(
        source: Box<dyn LifecycleWakeupSource>,
        claim_admission: Arc<dyn LifecycleClaimAdmission>,
        capacity: NonZeroUsize,
    ) -> Self {
        let (definition_build, definition_build_stream) = definition_build_notifications(capacity);
        let (patch, patch_stream) = patch_work_notifications(capacity);
        let (submission, submission_stream) = definition_submission_notifications(capacity);
        let wakeups = LifecycleWakeups {
            definition_build,
            patch,
            submission,
        };
        Self {
            source,
            wakeups: wakeups.clone(),
            reconciler_inputs: LifecycleReconcilerInputs {
                definition_build: definition_build_stream,
                patch: patch_stream,
                submission: submission_stream,
                claim_admission,
            },
        }
    }

    pub fn wakeups(&self) -> LifecycleWakeups {
        self.wakeups.clone()
    }

    pub fn into_parts(
        self,
    ) -> (
        Box<dyn LifecycleWakeupSource>,
        LifecycleWakeups,
        LifecycleReconcilerInputs,
    ) {
        (self.source, self.wakeups, self.reconciler_inputs)
    }
}

struct AllNatsLifecycleWakeupSource {
    definition_build: async_nats::Subscriber,
    patch: async_nats::Subscriber,
    submission: async_nats::Subscriber,
}

impl AllNatsLifecycleWakeupSource {
    async fn connect(nats: &async_nats::Client) -> Result<Self> {
        let definition_build = nats
            .queue_subscribe(BUILD_QUEUE_SUBJECT, BUILD_QUEUE_GROUP.into())
            .await?;
        let patch = nats
            .queue_subscribe(PATCH_BUILD_QUEUE_SUBJECT, PATCH_BUILD_QUEUE_GROUP.into())
            .await?;
        let submission = nats
            .queue_subscribe(SUBMISSION_QUEUE_SUBJECT, SUBMISSION_QUEUE_GROUP.into())
            .await?;
        nats.flush().await?;
        Ok(Self {
            definition_build,
            patch,
            submission,
        })
    }
}

#[async_trait]
impl LifecycleWakeupSource for AllNatsLifecycleWakeupSource {
    async fn run(&mut self, wakeups: LifecycleWakeups, cancel: CancellationToken) -> Result<()> {
        let mut definition_build_open = true;
        let mut patch_open = true;
        let mut submission_open = true;
        loop {
            if !definition_build_open && !patch_open && !submission_open {
                cancel.cancelled().await;
                return Ok(());
            }
            tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                notification = self.definition_build.next(), if definition_build_open => {
                    definition_build_open = notification.is_some();
                    if definition_build_open {
                        wakeups.notify(LifecyclePipeline::DefinitionBuild);
                    }
                }
                notification = self.patch.next(), if patch_open => {
                    patch_open = notification.is_some();
                    if patch_open {
                        wakeups.notify(LifecyclePipeline::PatchBuild);
                    }
                }
                notification = self.submission.next(), if submission_open => {
                    submission_open = notification.is_some();
                    if submission_open {
                        wakeups.notify(LifecyclePipeline::Submission);
                    }
                }
            }
        }
    }
}

/// Construct the fresh all-NATS LifecycleWork adapter.
///
/// Core-NATS queue subscriptions are transient latency hints. No stream,
/// durable consumer, or lifecycle-owned persistence resource is created.
pub async fn all_nats_lifecycle_work(nats: &async_nats::Client) -> Result<LifecycleWork> {
    let source = AllNatsLifecycleWakeupSource::connect(nats).await?;
    Ok(LifecycleWork::new(
        Box::new(source),
        Arc::new(OpenLifecycleClaims),
        NonZeroUsize::new(1).expect("lifecycle wakeup capacity is non-zero"),
    ))
}
