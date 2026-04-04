use greenmqtt_core::{
    ClusterNodeLifecycle, ClusterNodeMembership, HealthScore, NodeId, NodeStatus,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;

pub type ProbeId = u64;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FailureDetectorConfig {
    pub ping_timeout: Duration,
    pub suspect_timeout: Duration,
    pub confirmed_dead_rounds: u8,
}

impl Default for FailureDetectorConfig {
    fn default() -> Self {
        Self {
            ping_timeout: Duration::from_secs(1),
            suspect_timeout: Duration::from_secs(5),
            confirmed_dead_rounds: 3,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FailureDetectorSnapshot {
    pub member: ClusterNodeMembership,
    pub health_score: HealthScore,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ProbeMessage {
    Ping {
        probe_id: ProbeId,
        target_incarnation: u64,
    },
    PingReq {
        probe_id: ProbeId,
        target: NodeId,
        target_incarnation: u64,
    },
    Ack {
        probe_id: ProbeId,
        incarnation: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TimeoutEvent {
    DirectProbe {
        target: NodeId,
        probe_id: ProbeId,
        helpers: Vec<NodeId>,
    },
    IndirectProbe {
        target: NodeId,
        probe_id: ProbeId,
    },
    Suspect {
        target: NodeId,
        incarnation: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DetectorInput {
    ObserveMember(ClusterNodeMembership),
    TickRound,
    ProbeTick {
        target: NodeId,
        helpers: Vec<NodeId>,
    },
    Message {
        from: NodeId,
        message: ProbeMessage,
    },
    Timeout(TimeoutEvent),
    SelfSuspected {
        node_id: NodeId,
        incarnation: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DetectorOutput {
    Send {
        to: NodeId,
        message: ProbeMessage,
    },
    MembershipChanged(ClusterNodeMembership),
    ScheduleTimeout {
        event: TimeoutEvent,
        after: Duration,
    },
    BumpIncarnationAndRefute(ClusterNodeMembership),
    MembershipRemoved(NodeId),
}

#[derive(Debug, Clone)]
struct FailureState {
    membership: ClusterNodeMembership,
    status: NodeStatus,
    health_score: HealthScore,
    confirmed_dead_rounds: u8,
}

pub struct FailureDetector {
    config: FailureDetectorConfig,
    next_probe_id: ProbeId,
    states: BTreeMap<NodeId, FailureState>,
}

impl FailureDetector {
    pub fn new(config: FailureDetectorConfig) -> Self {
        Self {
            config,
            next_probe_id: 1,
            states: BTreeMap::new(),
        }
    }

    pub fn step(&mut self, input: DetectorInput) -> Vec<DetectorOutput> {
        match input {
            DetectorInput::ObserveMember(member) => self.observe_member(member),
            DetectorInput::TickRound => self.advance_round(),
            DetectorInput::ProbeTick { target, helpers } => self.begin_probe(target, helpers),
            DetectorInput::Message { from, message } => self.on_message(from, message),
            DetectorInput::Timeout(event) => self.on_timeout(event),
            DetectorInput::SelfSuspected {
                node_id,
                incarnation,
            } => self.refute_self(node_id, incarnation),
        }
    }

    pub fn snapshot(&self, node_id: NodeId) -> Option<FailureDetectorSnapshot> {
        self.states.get(&node_id).map(|state| FailureDetectorSnapshot {
            member: state.membership.clone(),
            health_score: state.health_score,
        })
    }

    pub fn snapshots(&self) -> Vec<FailureDetectorSnapshot> {
        self.states
            .values()
            .map(|state| FailureDetectorSnapshot {
                member: state.membership.clone(),
                health_score: state.health_score,
            })
            .collect()
    }

    pub fn ping_timeout(&self) -> Duration {
        self.config.ping_timeout
    }

    pub fn suspect_timeout(&self) -> Duration {
        self.config.suspect_timeout
    }

    fn observe_member(&mut self, member: ClusterNodeMembership) -> Vec<DetectorOutput> {
        let status = map_lifecycle(member.lifecycle.clone());
        let changed = self
            .states
            .entry(member.node_id)
            .and_modify(|state| {
                state.membership = member.clone();
                state.status = status.clone();
            })
            .or_insert(FailureState {
                membership: member.clone(),
                status,
                health_score: HealthScore::default(),
                confirmed_dead_rounds: 0,
            })
            .membership
            .clone();
        vec![DetectorOutput::MembershipChanged(changed)]
    }

    fn begin_probe(&mut self, target: NodeId, helpers: Vec<NodeId>) -> Vec<DetectorOutput> {
        let Some(target_incarnation) = self.states.get(&target).map(|state| state.membership.epoch)
        else {
            return Vec::new();
        };
        let probe_id = self.allocate_probe_id();
        vec![
            DetectorOutput::Send {
                to: target,
                message: ProbeMessage::Ping {
                    probe_id,
                    target_incarnation,
                },
            },
            DetectorOutput::ScheduleTimeout {
                event: TimeoutEvent::DirectProbe {
                    target,
                    probe_id,
                    helpers,
                },
                after: self.config.ping_timeout,
            },
        ]
    }

    fn on_message(&mut self, from: NodeId, message: ProbeMessage) -> Vec<DetectorOutput> {
        match message {
            ProbeMessage::Ping {
                probe_id,
                target_incarnation,
            } => vec![DetectorOutput::Send {
                to: from,
                message: ProbeMessage::Ack {
                    probe_id,
                    incarnation: target_incarnation,
                },
            }],
            ProbeMessage::PingReq {
                probe_id,
                target,
                target_incarnation,
            } => vec![
                DetectorOutput::Send {
                    to: target,
                    message: ProbeMessage::Ping {
                        probe_id,
                        target_incarnation,
                    },
                },
                DetectorOutput::ScheduleTimeout {
                    event: TimeoutEvent::IndirectProbe { target, probe_id },
                    after: self.config.ping_timeout,
                },
            ],
            ProbeMessage::Ack {
                probe_id: _,
                incarnation,
            } => self.record_probe_success(from, incarnation),
        }
    }

    fn on_timeout(&mut self, event: TimeoutEvent) -> Vec<DetectorOutput> {
        match event {
            TimeoutEvent::DirectProbe {
                target,
                probe_id,
                helpers,
            } => {
                let Some(state) = self.states.get(&target) else {
                    return Vec::new();
                };
                if helpers.is_empty() {
                    return self.mark_suspect(target, state.membership.epoch);
                }
                let mut outputs = helpers
                    .into_iter()
                    .map(|helper| DetectorOutput::Send {
                        to: helper,
                        message: ProbeMessage::PingReq {
                            probe_id,
                            target,
                            target_incarnation: state.membership.epoch,
                        },
                    })
                    .collect::<Vec<_>>();
                outputs.push(DetectorOutput::ScheduleTimeout {
                    event: TimeoutEvent::IndirectProbe { target, probe_id },
                    after: self.config.ping_timeout,
                });
                outputs
            }
            TimeoutEvent::IndirectProbe { target, probe_id: _ } => {
                let incarnation = self
                    .states
                    .get(&target)
                    .map(|state| state.membership.epoch)
                    .unwrap_or(0);
                self.mark_suspect(target, incarnation)
            }
            TimeoutEvent::Suspect {
                target,
                incarnation,
            } => {
                let Some(state) = self.states.get(&target) else {
                    return Vec::new();
                };
                if state.membership.epoch != incarnation
                    || state.membership.lifecycle != ClusterNodeLifecycle::Suspect
                {
                    return Vec::new();
                }
                self.confirm_dead(target)
            }
        }
    }

    fn advance_round(&mut self) -> Vec<DetectorOutput> {
        let mut removed = Vec::new();
        for (node_id, state) in self.states.iter_mut() {
            if state.status == NodeStatus::ConfirmedDead {
                state.confirmed_dead_rounds = state.confirmed_dead_rounds.saturating_add(1);
                if state.confirmed_dead_rounds >= self.config.confirmed_dead_rounds {
                    removed.push(*node_id);
                }
            }
        }
        removed
            .into_iter()
            .filter_map(|node_id| self.states.remove(&node_id).map(|_| node_id))
            .map(DetectorOutput::MembershipRemoved)
            .collect()
    }

    fn refute_self(&mut self, node_id: NodeId, incarnation: u64) -> Vec<DetectorOutput> {
        let Some(state) = self.states.get_mut(&node_id) else {
            return Vec::new();
        };
        let next_incarnation = incarnation.saturating_add(1).max(state.membership.epoch + 1);
        state.membership.epoch = next_incarnation;
        state.membership.lifecycle = ClusterNodeLifecycle::Serving;
        state.status = NodeStatus::Alive;
        state.confirmed_dead_rounds = 0;
        vec![DetectorOutput::BumpIncarnationAndRefute(
            state.membership.clone(),
        )]
    }

    fn record_probe_success(&mut self, node_id: NodeId, incarnation: u64) -> Vec<DetectorOutput> {
        let Some(state) = self.states.get_mut(&node_id) else {
            return Vec::new();
        };
        state.health_score.0 = state.health_score.0.saturating_sub(1);
        state.status = NodeStatus::Alive;
        state.membership.lifecycle = ClusterNodeLifecycle::Serving;
        state.confirmed_dead_rounds = 0;
        if incarnation > state.membership.epoch {
            state.membership.epoch = incarnation;
        }
        vec![DetectorOutput::MembershipChanged(state.membership.clone())]
    }

    fn mark_suspect(&mut self, node_id: NodeId, incarnation: u64) -> Vec<DetectorOutput> {
        let Some(state) = self.states.get_mut(&node_id) else {
            return Vec::new();
        };
        state.health_score.0 = state.health_score.0.saturating_add(1);
        state.status = NodeStatus::Suspect;
        state.membership.lifecycle = ClusterNodeLifecycle::Suspect;
        state.confirmed_dead_rounds = 0;
        let membership = state.membership.clone();
        vec![
            DetectorOutput::MembershipChanged(membership.clone()),
            DetectorOutput::ScheduleTimeout {
                event: TimeoutEvent::Suspect {
                    target: node_id,
                    incarnation: incarnation.max(membership.epoch),
                },
                after: self.config.suspect_timeout,
            },
        ]
    }

    fn confirm_dead(&mut self, node_id: NodeId) -> Vec<DetectorOutput> {
        let Some(state) = self.states.get_mut(&node_id) else {
            return Vec::new();
        };
        state.status = NodeStatus::ConfirmedDead;
        state.membership.lifecycle = ClusterNodeLifecycle::Offline;
        state.confirmed_dead_rounds = 0;
        vec![DetectorOutput::MembershipChanged(state.membership.clone())]
    }

    fn allocate_probe_id(&mut self) -> ProbeId {
        let next = self.next_probe_id;
        self.next_probe_id = self.next_probe_id.saturating_add(1);
        next
    }
}

fn map_lifecycle(lifecycle: ClusterNodeLifecycle) -> NodeStatus {
    match lifecycle {
        ClusterNodeLifecycle::Serving | ClusterNodeLifecycle::Joining => NodeStatus::Alive,
        ClusterNodeLifecycle::Suspect => NodeStatus::Suspect,
        ClusterNodeLifecycle::Leaving | ClusterNodeLifecycle::Offline => NodeStatus::ConfirmedDead,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DetectorInput, DetectorOutput, FailureDetector, FailureDetectorConfig, HealthScore,
        ProbeMessage, TimeoutEvent,
    };
    use greenmqtt_core::{ClusterNodeLifecycle, ClusterNodeMembership, ServiceEndpoint, ServiceKind};
    use std::time::Duration;

    fn member(node_id: u64, epoch: u64, lifecycle: ClusterNodeLifecycle) -> ClusterNodeMembership {
        ClusterNodeMembership::new(
            node_id,
            epoch,
            lifecycle,
            vec![ServiceEndpoint::new(
                ServiceKind::Broker,
                node_id,
                format!("http://127.0.0.1:{}", 50000 + node_id),
            )],
        )
    }

    #[test]
    fn detector_tracks_alive_suspect_and_confirmed_dead() {
        let mut detector = FailureDetector::new(FailureDetectorConfig::default());
        detector.step(DetectorInput::ObserveMember(member(
            7,
            1,
            ClusterNodeLifecycle::Serving,
        )));
        assert_eq!(
            detector.snapshot(7).unwrap().member.lifecycle,
            ClusterNodeLifecycle::Serving
        );

        detector.step(DetectorInput::Timeout(TimeoutEvent::IndirectProbe {
            target: 7,
            probe_id: 1,
        }));
        let snapshot = detector.snapshot(7).unwrap();
        assert_eq!(snapshot.member.lifecycle, ClusterNodeLifecycle::Suspect);
        assert_eq!(snapshot.health_score, HealthScore(1));

        detector.step(DetectorInput::Timeout(TimeoutEvent::Suspect {
            target: 7,
            incarnation: 1,
        }));
        assert_eq!(
            detector.snapshot(7).unwrap().member.lifecycle,
            ClusterNodeLifecycle::Offline
        );
    }

    #[test]
    fn detector_probe_success_restores_alive_and_updates_incarnation() {
        let mut detector = FailureDetector::new(FailureDetectorConfig {
            ping_timeout: Duration::from_secs(2),
            suspect_timeout: Duration::from_secs(8),
            confirmed_dead_rounds: 3,
        });
        detector.step(DetectorInput::ObserveMember(member(
            9,
            1,
            ClusterNodeLifecycle::Suspect,
        )));
        detector.step(DetectorInput::Message {
            from: 9,
            message: ProbeMessage::Ack {
                probe_id: 1,
                incarnation: 4,
            },
        });
        let snapshot = detector.snapshot(9).unwrap();
        assert_eq!(snapshot.member.lifecycle, ClusterNodeLifecycle::Serving);
        assert_eq!(snapshot.health_score, HealthScore(0));
        assert_eq!(snapshot.member.epoch, 4);
        assert_eq!(detector.ping_timeout(), Duration::from_secs(2));
        assert_eq!(detector.suspect_timeout(), Duration::from_secs(8));
    }

    #[test]
    fn detector_probe_tick_emits_ping_and_indirect_probe_actions() {
        let mut detector = FailureDetector::new(FailureDetectorConfig::default());
        detector.step(DetectorInput::ObserveMember(member(
            11,
            3,
            ClusterNodeLifecycle::Serving,
        )));
        let outputs = detector.step(DetectorInput::ProbeTick {
            target: 11,
            helpers: vec![21, 22],
        });
        assert!(matches!(
            &outputs[0],
            DetectorOutput::Send {
                to: 11,
                message: ProbeMessage::Ping { .. }
            }
        ));
        assert!(matches!(
            &outputs[1],
            DetectorOutput::ScheduleTimeout {
                event: TimeoutEvent::DirectProbe { target: 11, .. },
                ..
            }
        ));
    }

    #[test]
    fn detector_direct_timeout_emits_pingreqs_before_suspect() {
        let mut detector = FailureDetector::new(FailureDetectorConfig::default());
        detector.step(DetectorInput::ObserveMember(member(
            15,
            2,
            ClusterNodeLifecycle::Serving,
        )));
        let outputs = detector.step(DetectorInput::Timeout(TimeoutEvent::DirectProbe {
            target: 15,
            probe_id: 7,
            helpers: vec![31, 32],
        }));
        assert_eq!(outputs.len(), 3);
        assert!(matches!(
            &outputs[0],
            DetectorOutput::Send {
                to: 31,
                message: ProbeMessage::PingReq { target: 15, .. }
            }
        ));
        assert!(matches!(
            &outputs[1],
            DetectorOutput::Send {
                to: 32,
                message: ProbeMessage::PingReq { target: 15, .. }
            }
        ));
        assert!(matches!(
            &outputs[2],
            DetectorOutput::ScheduleTimeout {
                event: TimeoutEvent::IndirectProbe { target: 15, .. },
                ..
            }
        ));
    }

    #[test]
    fn detector_self_suspected_bumps_incarnation_and_refutes() {
        let mut detector = FailureDetector::new(FailureDetectorConfig::default());
        detector.step(DetectorInput::ObserveMember(member(
            5,
            4,
            ClusterNodeLifecycle::Suspect,
        )));
        let outputs = detector.step(DetectorInput::SelfSuspected {
            node_id: 5,
            incarnation: 4,
        });
        assert!(matches!(
            &outputs[0],
            DetectorOutput::BumpIncarnationAndRefute(member)
                if member.node_id == 5 && member.epoch == 5
        ));
    }

    #[test]
    fn detector_indirect_timeout_marks_suspect_and_schedules_confirm_dead() {
        let mut detector = FailureDetector::new(FailureDetectorConfig::default());
        detector.step(DetectorInput::ObserveMember(member(
            13,
            2,
            ClusterNodeLifecycle::Serving,
        )));
        let outputs = detector.step(DetectorInput::Timeout(TimeoutEvent::IndirectProbe {
            target: 13,
            probe_id: 9,
        }));
        assert!(outputs.iter().any(|output| matches!(
            output,
            DetectorOutput::MembershipChanged(member)
                if member.node_id == 13 && member.lifecycle == ClusterNodeLifecycle::Suspect
        )));
        assert!(outputs.iter().any(|output| matches!(
            output,
            DetectorOutput::ScheduleTimeout {
                event: TimeoutEvent::Suspect { target: 13, incarnation: 2 },
                ..
            }
        )));
    }

    #[test]
    fn detector_prunes_confirmed_dead_members_after_grace_rounds() {
        let mut detector = FailureDetector::new(FailureDetectorConfig {
            ping_timeout: Duration::from_secs(1),
            suspect_timeout: Duration::from_secs(5),
            confirmed_dead_rounds: 2,
        });
        detector.step(DetectorInput::ObserveMember(member(
            21,
            1,
            ClusterNodeLifecycle::Serving,
        )));
        detector.step(DetectorInput::Timeout(TimeoutEvent::IndirectProbe {
            target: 21,
            probe_id: 3,
        }));
        detector.step(DetectorInput::Timeout(TimeoutEvent::Suspect {
            target: 21,
            incarnation: 1,
        }));
        assert!(detector.snapshot(21).is_some());

        assert!(detector.step(DetectorInput::TickRound).is_empty());
        let outputs = detector.step(DetectorInput::TickRound);
        assert!(matches!(
            &outputs[0],
            DetectorOutput::MembershipRemoved(21)
        ));
        assert!(detector.snapshot(21).is_none());
    }

    #[test]
    fn detector_new_member_is_visible_immediately_after_observe() {
        let mut detector = FailureDetector::new(FailureDetectorConfig::default());
        let outputs = detector.step(DetectorInput::ObserveMember(member(
            31,
            1,
            ClusterNodeLifecycle::Joining,
        )));
        assert!(matches!(
            &outputs[0],
            DetectorOutput::MembershipChanged(member)
                if member.node_id == 31 && member.lifecycle == ClusterNodeLifecycle::Joining
        ));
        assert_eq!(detector.snapshots().len(), 1);
    }

    #[test]
    fn detector_timeout_chain_reaches_suspect_then_confirm_dead() {
        let mut detector = FailureDetector::new(FailureDetectorConfig::default());
        detector.step(DetectorInput::ObserveMember(member(
            41,
            1,
            ClusterNodeLifecycle::Serving,
        )));
        let direct = detector.step(DetectorInput::ProbeTick {
            target: 41,
            helpers: vec![51],
        });
        assert!(direct.iter().any(|output| matches!(
            output,
            DetectorOutput::ScheduleTimeout {
                event: TimeoutEvent::DirectProbe { target: 41, .. },
                ..
            }
        )));
        detector.step(DetectorInput::Timeout(TimeoutEvent::DirectProbe {
            target: 41,
            probe_id: 1,
            helpers: vec![51],
        }));
        detector.step(DetectorInput::Timeout(TimeoutEvent::IndirectProbe {
            target: 41,
            probe_id: 1,
        }));
        assert_eq!(
            detector.snapshot(41).unwrap().member.lifecycle,
            ClusterNodeLifecycle::Suspect
        );
        detector.step(DetectorInput::Timeout(TimeoutEvent::Suspect {
            target: 41,
            incarnation: 1,
        }));
        assert_eq!(
            detector.snapshot(41).unwrap().member.lifecycle,
            ClusterNodeLifecycle::Offline
        );
    }

    #[test]
    fn detector_ack_with_new_incarnation_restores_converged_membership() {
        let mut detector = FailureDetector::new(FailureDetectorConfig::default());
        detector.step(DetectorInput::ObserveMember(member(
            61,
            1,
            ClusterNodeLifecycle::Suspect,
        )));
        detector.step(DetectorInput::Message {
            from: 61,
            message: ProbeMessage::Ack {
                probe_id: 7,
                incarnation: 3,
            },
        });
        let snapshot = detector.snapshot(61).unwrap();
        assert_eq!(snapshot.member.lifecycle, ClusterNodeLifecycle::Serving);
        assert_eq!(snapshot.member.epoch, 3);
        assert_eq!(snapshot.health_score, HealthScore(0));
    }
}
