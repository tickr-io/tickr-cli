use chrono::Utc;
use tickr_executor::local_pickup::{
    LocalAttemptOutcome, LocalPickupClaim, SafeAttemptOutcomeHandoff, TerminalElection,
};

pub async fn assert_attempt_outcome_law<H>(
    handoff: H,
    claim: &LocalPickupClaim,
) -> LocalAttemptOutcome
where
    H: SafeAttemptOutcomeHandoff,
{
    let process_handoff = handoff.clone();
    let liveness_handoff = handoff.clone();
    let process_claim = claim.clone();
    let liveness_claim = claim.clone();
    let now = Utc::now();
    let (process, liveness) = tokio::join!(
        process_handoff.elect_terminal(
            &process_claim,
            LocalAttemptOutcome::ProcessExitedFailure,
            b"backend-neutral process-exit TaskEvent",
            now,
        ),
        liveness_handoff.elect_terminal(
            &liveness_claim,
            LocalAttemptOutcome::LivenessExpired,
            b"backend-neutral liveness TaskEvent",
            now,
        ),
    );
    let process = process.expect("process-exit contender must settle");
    let liveness = liveness.expect("liveness contender must settle");
    let winner = match (process, liveness) {
        (
            TerminalElection::Won,
            TerminalElection::Settled(LocalAttemptOutcome::ProcessExitedFailure),
        ) => LocalAttemptOutcome::ProcessExitedFailure,
        (
            TerminalElection::Settled(LocalAttemptOutcome::LivenessExpired),
            TerminalElection::Won,
        ) => LocalAttemptOutcome::LivenessExpired,
        other => panic!("one contender must win and the other must read it: {other:?}"),
    };

    assert_eq!(
        handoff
            .elect_terminal(
                claim,
                LocalAttemptOutcome::ProcessSetupFailed,
                b"duplicate verdict must not stage",
                Utc::now(),
            )
            .await
            .expect("duplicate contender must read the elected outcome"),
        TerminalElection::Settled(winner),
    );

    winner
}
