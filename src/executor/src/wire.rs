//! The executor's view of the published **task-coordination contract**
//! ([`tickr_proto::task`]). The executor decodes the dispatch and cancel-request
//! protobuf messages it receives into execution-slice structs and encodes task
//! events and cancellation acknowledgements against the same contract.

use std::collections::{HashMap, HashSet};

use anyhow::{anyhow, Result};
use prost::Message;
use tickr_proto::task as tc;
use uuid::Uuid;

fn parse_uuid(s: &str, field: &str) -> Result<Uuid> {
    Uuid::parse_str(s).map_err(|e| anyhow!("{field} `{s}`: {e}"))
}

/// Runtime execution inputs reconstructed from a `TaskDispatch`.
#[derive(Clone, Debug)]
pub struct DispatchedTask {
    pub task_instance_id: Uuid,
    pub task_id: Uuid,
    pub workflow_instance_id: Uuid,
    pub workflow_id: Uuid,
    pub name: String,
    pub nix_expression_path: String,
    pub nix_args: Vec<String>,
    pub outputs: Vec<String>,
    pub inputs: Vec<String>,
    pub secrets: Vec<String>,
    /// Conductor-minted signal id of the wire `Signal::Trigger` whose dispatch
    /// produced this task; `None` for cron-fired instances. Routes
    /// `from.trigger` reads to `<signal_id>/<name>`.
    pub originating_signal_id: Option<Uuid>,
    /// Per-input-name → gate wakeup signal id, populated for `from.signal`
    /// inputs. Routes `from.signal` reads to `<gate_signal_id>/<name>`.
    pub gate_signal_ids: HashMap<String, Uuid>,
    /// Satisfied gate signal ids on every edge incident to this task — the
    /// ambient resolver set (order is never consulted, so a set).
    pub gate_signal_ids_ambient: HashSet<Uuid>,
}

/// Decode a dispatched task off the published `TaskDispatch` contract. The
/// conductor republishes the server-authored bytes verbatim onto the dispatch
/// work queue; the executor reconstructs the execution slice here.
pub fn decode_dispatch(bytes: &[u8]) -> Result<DispatchedTask> {
    let p = tc::TaskDispatch::decode(bytes).map_err(|e| anyhow!("decode TaskDispatch: {e}"))?;

    let mut gate_signal_ids = HashMap::with_capacity(p.gate_signal_ids.len());
    for (name, id) in p.gate_signal_ids {
        gate_signal_ids.insert(name, parse_uuid(&id, "TaskDispatch gate_signal_id")?);
    }
    let mut gate_signal_ids_ambient = HashSet::with_capacity(p.gate_signal_ids_ambient.len());
    for id in p.gate_signal_ids_ambient {
        gate_signal_ids_ambient.insert(parse_uuid(&id, "TaskDispatch gate_signal_ids_ambient")?);
    }
    let originating_signal_id = match p.originating_signal_id {
        Some(s) => Some(parse_uuid(&s, "TaskDispatch originating_signal_id")?),
        None => None,
    };

    Ok(DispatchedTask {
        task_instance_id: parse_uuid(&p.task_instance_id, "TaskDispatch task_instance_id")?,
        task_id: parse_uuid(&p.task_id, "TaskDispatch task_id")?,
        workflow_instance_id: parse_uuid(
            &p.workflow_instance_id,
            "TaskDispatch workflow_instance_id",
        )?,
        workflow_id: parse_uuid(&p.workflow_id, "TaskDispatch workflow_id")?,
        name: p.name,
        nix_expression_path: p.nix_expression_path,
        nix_args: p.nix_args,
        outputs: p.outputs,
        inputs: p.inputs,
        secrets: p.secrets,
        originating_signal_id,
        gate_signal_ids,
        gate_signal_ids_ambient,
    })
}

/// The lifecycle events the executor emits on a task. Its vocabulary is the
/// executor's honest *exit observation* — `Completed` = process exit 0,
/// `Failed` = exit ≠ 0 — never a lifecycle verdict (the server owns the
/// lifecycle). `Completed` is published bare; the conductor's completion drain
/// stamps the declared routing variables (and any self-patch) onto it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EmitKind {
    Assigned,
    Started,
    Completed,
    Failed,
}

/// Encode a `TaskEvent` the executor emits onto the published contract, stamping
/// the identity header off the dispatched task and this executor's id.
pub fn encode_task_event(task: &DispatchedTask, executor_id: Uuid, kind: EmitKind) -> Vec<u8> {
    use tc::task_event::{Assigned, Completed, Failed, Kind, Started};
    let kind = match kind {
        EmitKind::Assigned => Kind::Assigned(Assigned {}),
        EmitKind::Started => Kind::Started(Started {}),
        EmitKind::Completed => Kind::Completed(Completed {
            // The executor reports a bare completion; the conductor enriches it.
            routing_variables: HashMap::new(),
            self_patch: None,
            self_patch_stall_ttl: None,
        }),
        EmitKind::Failed => Kind::Failed(Failed {}),
    };
    tc::TaskEvent {
        task_instance_id: task.task_instance_id.to_string(),
        task_id: task.task_id.to_string(),
        workflow_instance_id: task.workflow_instance_id.to_string(),
        workflow_id: task.workflow_id.to_string(),
        executor_id: Some(executor_id.to_string()),
        kind: Some(kind),
    }
    .encode_to_vec()
}

/// A cancel-request the executor handles: kill a cancelled task's in-flight
/// process group. Carries the ids the executor needs to find the running task
/// and author its ack.
#[derive(Clone, Copy, Debug)]
pub struct CancelRequest {
    pub task_instance_id: Uuid,
    pub workflow_instance_id: Uuid,
}

/// Decode a cancel-request off the published `CancelTaskRequest` contract.
pub fn decode_cancel_request(bytes: &[u8]) -> Result<CancelRequest> {
    let r = tc::CancelTaskRequest::decode(bytes)
        .map_err(|e| anyhow!("decode CancelTaskRequest: {e}"))?;
    Ok(CancelRequest {
        task_instance_id: parse_uuid(&r.task_instance_id, "CancelTaskRequest task_instance_id")?,
        workflow_instance_id: parse_uuid(
            &r.workflow_instance_id,
            "CancelTaskRequest workflow_instance_id",
        )?,
    })
}

/// What the executor found when it handled a cancel-request. Both outcomes are
/// terminal for the kill — there is no surviving process either way.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KillOutcome {
    /// The task was running (or caught as it spawned); its group was torn down.
    Killed,
    /// No such task was running here — already finished or never picked up.
    NoSuchTask,
}

/// Encode a `CancelTaskAck` the executor emits onto the published contract.
pub fn encode_cancel_ack(
    task_instance_id: Uuid,
    workflow_instance_id: Uuid,
    outcome: KillOutcome,
) -> Vec<u8> {
    let outcome = match outcome {
        KillOutcome::Killed => tc::KillOutcome::Killed,
        KillOutcome::NoSuchTask => tc::KillOutcome::NoSuchTask,
    };
    tc::CancelTaskAck {
        task_instance_id: task_instance_id.to_string(),
        workflow_instance_id: workflow_instance_id.to_string(),
        outcome: outcome as i32,
    }
    .encode_to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A dispatch encoded on the contract round-trips into the execution slice
    /// the executor runs — the same proto the server authors and the conductor
    /// republishes verbatim.
    #[test]
    fn dispatch_round_trips_into_execution_slice() {
        let wf = Uuid::new_v4();
        let wi = Uuid::new_v4();
        let ti = Uuid::new_v4();
        let sig = Uuid::new_v4();
        let gate = Uuid::new_v4();
        let ambient = Uuid::new_v4();

        let mut gate_signal_ids = HashMap::new();
        gate_signal_ids.insert("approver".to_string(), gate.to_string());

        let proto = tc::TaskDispatch {
            task_instance_id: ti.to_string(),
            task_id: Uuid::new_v4().to_string(),
            workflow_instance_id: wi.to_string(),
            workflow_id: wf.to_string(),
            name: "ship".to_string(),
            task_type: 0,
            nix_expression_path: "/p#expr".to_string(),
            nix_args: vec!["--flag".to_string()],
            outputs: vec!["out".to_string()],
            inputs: vec!["in".to_string()],
            secrets: vec!["sec".to_string()],
            tenant_id: "acme".to_string(),
            originating_signal_id: Some(sig.to_string()),
            gate_signal_ids,
            gate_signal_ids_ambient: vec![ambient.to_string()],
        };

        let decoded = decode_dispatch(&proto.encode_to_vec()).expect("decode");
        assert_eq!(decoded.task_instance_id, ti);
        assert_eq!(decoded.workflow_instance_id, wi);
        assert_eq!(decoded.workflow_id, wf);
        assert_eq!(decoded.name, "ship");
        assert_eq!(decoded.nix_expression_path, "/p#expr");
        assert_eq!(decoded.nix_args, vec!["--flag".to_string()]);
        assert_eq!(decoded.originating_signal_id, Some(sig));
        assert_eq!(decoded.gate_signal_ids.get("approver"), Some(&gate));
        assert!(decoded.gate_signal_ids_ambient.contains(&ambient));
    }

    /// A cancel-ack the executor emits decodes on the peer's contract, for both
    /// outcomes.
    #[test]
    fn cancel_ack_round_trips_both_outcomes() {
        for (outcome, want) in [
            (KillOutcome::Killed, tc::KillOutcome::Killed),
            (KillOutcome::NoSuchTask, tc::KillOutcome::NoSuchTask),
        ] {
            let ti = Uuid::new_v4();
            let wi = Uuid::new_v4();
            let bytes = encode_cancel_ack(ti, wi, outcome);
            let decoded = tc::CancelTaskAck::decode(&bytes[..]).expect("decode ack");
            assert_eq!(decoded.task_instance_id, ti.to_string());
            assert_eq!(decoded.workflow_instance_id, wi.to_string());
            assert_eq!(decoded.outcome, want as i32);
        }
    }

    /// A task event the executor emits decodes on the peer's contract with the
    /// executor id stamped and the identity header carried through.
    #[test]
    fn task_event_encodes_kind_and_executor_id() {
        let task = DispatchedTask {
            task_instance_id: Uuid::new_v4(),
            task_id: Uuid::new_v4(),
            workflow_instance_id: Uuid::new_v4(),
            workflow_id: Uuid::new_v4(),
            name: "t".to_string(),
            nix_expression_path: "/p".to_string(),
            nix_args: vec![],
            outputs: vec![],
            inputs: vec![],
            secrets: vec![],
            originating_signal_id: None,
            gate_signal_ids: HashMap::new(),
            gate_signal_ids_ambient: HashSet::new(),
        };
        let executor_id = Uuid::new_v4();
        let bytes = encode_task_event(&task, executor_id, EmitKind::Completed);
        let decoded = tc::TaskEvent::decode(&bytes[..]).expect("decode event");
        assert_eq!(decoded.task_instance_id, task.task_instance_id.to_string());
        assert_eq!(decoded.executor_id, Some(executor_id.to_string()));
        assert!(matches!(
            decoded.kind,
            Some(tc::task_event::Kind::Completed(_))
        ));
    }
}
