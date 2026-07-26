use anyhow::{anyhow, Context, Result};
use async_nats::jetstream::kv;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{sync::Arc, time::Duration};
use uuid::Uuid;

use crate::idempotency;

pub const CLAIM_LEASE: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct NatsIngressIdempotencyStore {
    bucket: kv::Store,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RelayIntent {
    Signal(Vec<u8>),
    WakeupSignal(Vec<u8>),
    GateOutcome(Vec<u8>),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
enum ProducerPhase {
    Reserved,
    Ready,
    Relayed,
    Rejected { reason: String },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ProducerRecord {
    version: u8,
    producer_key_sha256: String,
    payload_sha256: String,
    signal_id: Uuid,
    lease_owner: Uuid,
    lease_expires_at: DateTime<Utc>,
    phase: ProducerPhase,
    #[serde(default)]
    signal_effect: Vec<u8>,
    #[serde(default)]
    event_results: Vec<u8>,
    intents: Vec<RelayIntent>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum DeliveryOutcome {
    Accepted,
    Rejected { reason: String },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct DeliveryRecord {
    version: u8,
    producer_key_sha256: Option<String>,
    payload_sha256: String,
    outcome: DeliveryOutcome,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IngressEffects {
    pub signal_effect: Vec<u8>,
    pub event_results: Vec<u8>,
    pub relay_intents: Vec<RelayIntent>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IngressTerminalOutcome {
    Accepted,
    Rejected,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IngressOutcomeProof {
    producer_digest: String,
    payload_digest: String,
    outcome: IngressTerminalOutcome,
}

impl IngressOutcomeProof {
    pub fn new(
        producer_digest: impl Into<String>,
        payload_digest: impl Into<String>,
        outcome: IngressTerminalOutcome,
    ) -> Self {
        Self {
            producer_digest: producer_digest.into(),
            payload_digest: payload_digest.into(),
            outcome,
        }
    }

    pub fn producer_digest(&self) -> &str {
        &self.producer_digest
    }

    pub fn payload_digest(&self) -> &str {
        &self.payload_digest
    }

    pub fn outcome(&self) -> IngressTerminalOutcome {
        self.outcome
    }
}

pub enum ReservationOutcome {
    Acquired(Arc<dyn IngressReservation>),
    Pending,
    Ready(Arc<dyn IngressOperation>, IngressEffects),
    Complete(IngressOutcomeProof),
    Rejected(IngressOutcomeProof),
    Conflict {
        original_signal_id: Uuid,
        original_hash: String,
        proof: IngressOutcomeProof,
    },
}

#[async_trait]
pub trait IngressIdempotencyStore: Send + Sync {
    async fn reserve(
        &self,
        producer_key: &str,
        payload_sha256: &[u8; 32],
    ) -> Result<ReservationOutcome>;
}

#[derive(Clone)]
pub struct IngressCoordinator {
    store: Arc<dyn IngressIdempotencyStore>,
}

impl IngressCoordinator {
    pub fn new(store: Arc<dyn IngressIdempotencyStore>) -> Self {
        Self { store }
    }

    pub async fn reserve(
        &self,
        producer_key: &str,
        payload_sha256: &[u8; 32],
    ) -> Result<ReservationOutcome> {
        self.store.reserve(producer_key, payload_sha256).await
    }
}

#[async_trait]
pub trait IngressOperation: Send + Sync {
    async fn mark_relayed(&self) -> Result<IngressOutcomeProof>;
}

#[async_trait]
pub trait IngressReservation: Send + Sync {
    fn signal_id(&self) -> Uuid;
    fn operation(&self) -> Arc<dyn IngressOperation>;
    async fn persist_effects(&self, effects: IngressEffects) -> Result<IngressEffects>;
    async fn reject(&self, reason: String) -> Result<IngressOutcomeProof>;
    async fn abandon(&self) -> Result<()>;
}

#[derive(Clone)]
struct NatsIngressOperation {
    store: NatsIngressIdempotencyStore,
    key: String,
}

struct NatsIngressReservation {
    operation: NatsIngressOperation,
    owner: Uuid,
    signal_id: Uuid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IngressBoundary {
    AfterReservation,
    AfterEffects,
    AfterCapturePersistence,
    AfterRelayIntentPersistence,
    AfterPermanentRejection,
    BeforeDeliveryAck,
}

impl IngressBoundary {
    fn name(self) -> &'static str {
        match self {
            Self::AfterReservation => "after-reservation",
            Self::AfterEffects => "after-effects",
            Self::AfterCapturePersistence => "after-capture-persistence",
            Self::AfterRelayIntentPersistence => "after-relay-intent-persistence",
            Self::AfterPermanentRejection => "after-permanent-rejection",
            Self::BeforeDeliveryAck => "before-delivery-ack",
        }
    }
}

#[inline]
pub fn observe_ingress_boundary(boundary: IngressBoundary) {
    #[cfg(debug_assertions)]
    if std::env::var("TICKR_TEST_INGRESS_CRASH_BOUNDARY").as_deref() == Ok(boundary.name()) {
        std::process::exit(86);
    }
}

impl NatsIngressIdempotencyStore {
    pub async fn open(nats: &async_nats::Client) -> Result<Self> {
        Ok(Self {
            bucket: idempotency::open_bucket(nats).await?,
        })
    }

    pub async fn record_delivery(
        &self,
        delivery_sequence: u64,
        producer_key: Option<&str>,
        payload_sha256: &[u8; 32],
        outcome: DeliveryOutcome,
    ) -> Result<()> {
        let key = format!("delivery.{delivery_sequence}");
        let record = DeliveryRecord {
            version: 1,
            producer_key_sha256: producer_key.map(|value| sha256_hex(value.as_bytes())),
            payload_sha256: hex::encode(payload_sha256),
            outcome,
        };
        let bytes = serde_json::to_vec(&record).context("encode ingress delivery record")?;
        match self.bucket.create(&key, bytes.into()).await {
            Ok(_) => Ok(()),
            Err(_) => {
                let Some(existing) = self
                    .bucket
                    .get(&key)
                    .await
                    .with_context(|| format!("read ingress delivery record `{key}`"))?
                else {
                    return Err(anyhow!("ingress delivery record `{key}` disappeared"));
                };
                let existing: DeliveryRecord = serde_json::from_slice(&existing)
                    .with_context(|| format!("decode ingress delivery record `{key}`"))?;
                if existing == record {
                    Ok(())
                } else {
                    Err(anyhow!("conflicting ingress delivery record `{key}`"))
                }
            }
        }
    }
}

#[async_trait]
impl IngressIdempotencyStore for NatsIngressIdempotencyStore {
    async fn reserve(
        &self,
        producer_key: &str,
        payload_sha256: &[u8; 32],
    ) -> Result<ReservationOutcome> {
        let producer_key_sha256 = sha256_hex(producer_key.as_bytes());
        let key = format!("producer.{producer_key_sha256}");
        let payload_sha256 = hex::encode(payload_sha256);
        let owner = Uuid::new_v4();

        for _ in 0..16 {
            let Some(entry) = self
                .bucket
                .entry(&key)
                .await
                .with_context(|| format!("read ingress producer claim `{key}`"))?
            else {
                let signal_id = Uuid::new_v4();
                let record = ProducerRecord {
                    version: 1,
                    producer_key_sha256: producer_key_sha256.clone(),
                    payload_sha256: payload_sha256.clone(),
                    signal_id,
                    lease_owner: owner,
                    lease_expires_at: lease_deadline(),
                    phase: ProducerPhase::Reserved,
                    signal_effect: Vec::new(),
                    event_results: Vec::new(),
                    intents: Vec::new(),
                };
                let bytes = serde_json::to_vec(&record).context("encode ingress producer claim")?;
                if self.bucket.create(&key, bytes.into()).await.is_ok() {
                    return Ok(ReservationOutcome::Acquired(Arc::new(
                        NatsIngressReservation {
                            operation: NatsIngressOperation {
                                store: self.clone(),
                                key,
                            },
                            owner,
                            signal_id,
                        },
                    )));
                }
                continue;
            };

            let mut record: ProducerRecord = serde_json::from_slice(&entry.value)
                .with_context(|| format!("decode ingress producer claim `{key}`"))?;
            if record.version != 1 || record.producer_key_sha256 != producer_key_sha256 {
                return Err(anyhow!("invalid ingress producer claim `{key}`"));
            }
            if record.payload_sha256 != payload_sha256 {
                return Ok(ReservationOutcome::Conflict {
                    original_signal_id: record.signal_id,
                    original_hash: record.payload_sha256,
                    proof: IngressOutcomeProof::new(
                        producer_key_sha256,
                        payload_sha256,
                        IngressTerminalOutcome::Rejected,
                    ),
                });
            }

            let operation = NatsIngressOperation {
                store: self.clone(),
                key: key.clone(),
            };
            match &record.phase {
                ProducerPhase::Ready => {
                    return Ok(ReservationOutcome::Ready(
                        Arc::new(operation),
                        record.effects(),
                    ));
                }
                ProducerPhase::Relayed => {
                    return Ok(ReservationOutcome::Complete(
                        record.proof(IngressTerminalOutcome::Accepted),
                    ));
                }
                ProducerPhase::Rejected { .. } => {
                    return Ok(ReservationOutcome::Rejected(
                        record.proof(IngressTerminalOutcome::Rejected),
                    ));
                }
                ProducerPhase::Reserved if record.lease_expires_at > Utc::now() => {
                    return Ok(ReservationOutcome::Pending);
                }
                ProducerPhase::Reserved => {
                    record.lease_owner = owner;
                    record.lease_expires_at = lease_deadline();
                    let bytes = serde_json::to_vec(&record)
                        .context("encode reclaimed ingress producer claim")?;
                    if self
                        .bucket
                        .update(&key, bytes.into(), entry.revision)
                        .await
                        .is_ok()
                    {
                        return Ok(ReservationOutcome::Acquired(Arc::new(
                            NatsIngressReservation {
                                operation,
                                owner,
                                signal_id: record.signal_id,
                            },
                        )));
                    }
                }
            }
        }

        Err(anyhow!("ingress producer claim changed too frequently"))
    }
}

#[async_trait]
impl IngressOperation for NatsIngressOperation {
    async fn mark_relayed(&self) -> Result<IngressOutcomeProof> {
        self.update(None, |record| match record.phase {
            ProducerPhase::Ready => {
                record.phase = ProducerPhase::Relayed;
                Ok(())
            }
            ProducerPhase::Relayed => Ok(()),
            _ => Err(anyhow!("ingress relay completion requires ready intent")),
        })
        .await?;
        Ok(self.load().await?.proof(IngressTerminalOutcome::Accepted))
    }
}

#[async_trait]
impl IngressReservation for NatsIngressReservation {
    fn signal_id(&self) -> Uuid {
        self.signal_id
    }

    fn operation(&self) -> Arc<dyn IngressOperation> {
        Arc::new(self.operation.clone())
    }

    async fn persist_effects(&self, effects: IngressEffects) -> Result<IngressEffects> {
        self.operation
            .update(Some(self.owner), |record| {
                if record.phase != ProducerPhase::Reserved {
                    return Err(anyhow!("ingress effects require an active reservation"));
                }
                record.signal_effect = effects.signal_effect.clone();
                record.event_results = effects.event_results.clone();
                record.intents = effects.relay_intents.clone();
                record.phase = ProducerPhase::Ready;
                Ok(())
            })
            .await?;
        Ok(self.operation.load().await?.effects())
    }

    async fn reject(&self, reason: String) -> Result<IngressOutcomeProof> {
        self.operation
            .update(Some(self.owner), |record| {
                if record.phase != ProducerPhase::Reserved {
                    return Err(anyhow!("ingress rejection requires a reservation"));
                }
                record.phase = ProducerPhase::Rejected {
                    reason: reason.clone(),
                };
                Ok(())
            })
            .await?;
        Ok(self
            .operation
            .load()
            .await?
            .proof(IngressTerminalOutcome::Rejected))
    }

    async fn abandon(&self) -> Result<()> {
        self.operation
            .update(Some(self.owner), |record| {
                if record.phase == ProducerPhase::Reserved {
                    record.lease_expires_at = Utc::now();
                }
                Ok(())
            })
            .await
    }
}

impl NatsIngressOperation {
    async fn load(&self) -> Result<ProducerRecord> {
        let Some(entry) = self
            .store
            .bucket
            .entry(&self.key)
            .await
            .with_context(|| format!("read ingress producer claim `{}`", self.key))?
        else {
            return Err(anyhow!("ingress producer claim `{}` disappeared", self.key));
        };
        serde_json::from_slice(&entry.value)
            .with_context(|| format!("decode ingress producer claim `{}`", self.key))
    }

    async fn update(
        &self,
        expected_owner: Option<Uuid>,
        mutate: impl Fn(&mut ProducerRecord) -> Result<()>,
    ) -> Result<()> {
        for _ in 0..16 {
            let Some(entry) = self
                .store
                .bucket
                .entry(&self.key)
                .await
                .with_context(|| format!("read ingress producer claim `{}`", self.key))?
            else {
                return Err(anyhow!("ingress producer claim `{}` disappeared", self.key));
            };
            let mut record: ProducerRecord = serde_json::from_slice(&entry.value)
                .with_context(|| format!("decode ingress producer claim `{}`", self.key))?;
            if expected_owner.is_some_and(|owner| record.lease_owner != owner) {
                return Err(anyhow!("ingress producer claim lease changed owner"));
            }
            mutate(&mut record)?;
            let bytes = serde_json::to_vec(&record).context("encode ingress producer claim")?;
            if self
                .store
                .bucket
                .update(&self.key, bytes.into(), entry.revision)
                .await
                .is_ok()
            {
                return Ok(());
            }
        }
        Err(anyhow!("ingress producer claim changed too frequently"))
    }
}

impl ProducerRecord {
    fn effects(&self) -> IngressEffects {
        IngressEffects {
            signal_effect: self.signal_effect.clone(),
            event_results: self.event_results.clone(),
            relay_intents: self.intents.clone(),
        }
    }

    fn proof(&self, outcome: IngressTerminalOutcome) -> IngressOutcomeProof {
        IngressOutcomeProof::new(
            self.producer_key_sha256.clone(),
            self.payload_sha256.clone(),
            outcome,
        )
    }
}

fn lease_deadline() -> DateTime<Utc> {
    let mut lease = CLAIM_LEASE;
    #[cfg(debug_assertions)]
    if let Ok(value) = std::env::var("TICKR_TEST_INGRESS_CLAIM_LEASE_MS") {
        if let Ok(milliseconds) = value.parse::<u64>() {
            lease = Duration::from_millis(milliseconds);
        }
    }
    Utc::now()
        + chrono::Duration::from_std(lease).expect("ingress claim lease fits chrono duration")
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn producer_and_transport_keys_use_disjoint_namespaces() {
        let producer = format!("producer.{}", sha256_hex(b"42"));
        let delivery = "delivery.42";
        assert_ne!(producer, delivery);
        assert!(producer.starts_with("producer."));
        assert!(delivery.starts_with("delivery."));
    }

    #[test]
    fn relay_intents_round_trip_without_changing_envelope_bytes() {
        let intents = vec![
            RelayIntent::Signal(vec![1, 2, 3]),
            RelayIntent::WakeupSignal(vec![7, 8, 9]),
            RelayIntent::GateOutcome(vec![4, 5, 6]),
        ];
        let bytes = serde_json::to_vec(&intents).unwrap();
        let decoded: Vec<RelayIntent> = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded, intents);
    }
}
