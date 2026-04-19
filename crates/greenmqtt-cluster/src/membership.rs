use greenmqtt_core::{ClusterNodeLifecycle, ClusterNodeMembership, NodeId};
use std::collections::BTreeMap;

#[derive(Default)]
pub struct MemberList {
    members: BTreeMap<NodeId, ClusterNodeMembership>,
}

impl MemberList {
    pub fn upsert(&mut self, member: ClusterNodeMembership) -> Option<ClusterNodeMembership> {
        self.members.insert(member.node_id, member)
    }

    pub fn mark_lifecycle(
        &mut self,
        node_id: NodeId,
        lifecycle: ClusterNodeLifecycle,
    ) -> Option<ClusterNodeMembership> {
        let member = self.members.get_mut(&node_id)?;
        member.lifecycle = lifecycle;
        Some(member.clone())
    }

    pub fn remove(&mut self, node_id: NodeId) -> Option<ClusterNodeMembership> {
        self.members.remove(&node_id)
    }

    pub fn list(&self) -> Vec<ClusterNodeMembership> {
        self.members.values().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::MemberList;
    use greenmqtt_core::{
        ClusterNodeLifecycle, ClusterNodeMembership, ServiceEndpoint, ServiceKind,
    };

    fn member(node_id: u64, lifecycle: ClusterNodeLifecycle) -> ClusterNodeMembership {
        ClusterNodeMembership::new(
            node_id,
            1,
            lifecycle,
            vec![ServiceEndpoint::new(
                ServiceKind::Broker,
                node_id,
                format!("http://127.0.0.1:{}", 50000 + node_id),
            )],
        )
    }

    #[test]
    fn member_list_upserts_marks_and_removes_members() {
        let mut members = MemberList::default();
        assert!(members
            .upsert(member(7, ClusterNodeLifecycle::Joining))
            .is_none());
        assert_eq!(members.list().len(), 1);

        let updated = members
            .mark_lifecycle(7, ClusterNodeLifecycle::Serving)
            .unwrap();
        assert_eq!(updated.lifecycle, ClusterNodeLifecycle::Serving);

        let removed = members.remove(7).unwrap();
        assert_eq!(removed.node_id, 7);
        assert!(members.list().is_empty());
    }
}
