@0xf4f4d7df9b6a4c11;

enum ServiceKind {
  sessionDict @0;
  dist @1;
  inbox @2;
  retain @3;
  metadata @4;
  kvRange @5;
  raftTransport @6;
  rangeAdmin @7;
  rangeControl @8;
  brokerPeer @9;
}

enum TransportKind {
  grpcHttp @0;
  grpcHttps @1;
  quic @2;
}

enum FrameKind {
  requestHeader @0;
  requestBody @1;
  responseHeader @2;
  responseBody @3;
  streamItem @4;
  streamEnd @5;
  cancel @6;
  protocolError @7;
}

enum StatusCode {
  ok @0;
  invalidArgument @1;
  notFound @2;
  unavailable @3;
  resourceExhausted @4;
  deadlineExceeded @5;
  internal @6;
  notSupported @7;
}

struct RpcRequestHeader {
  service @0 :ServiceKind;
  methodId @1 :UInt16;
  protocolVersion @2 :UInt16;
  requestId @3 :UInt64;
  timeoutMs @4 :UInt64;
  traceId @5 :Text;
  expectedEpoch @6 :UInt64;
  fencingToken @7 :UInt64;
}

struct RpcResponseHeader {
  status @0 :StatusCode;
  retryable @1 :Bool;
  retryAfterMs @2 :UInt64;
  errorMessage @3 :Text;
}

enum SessionKind {
  transient @0;
  persistent @1;
}

struct ClientIdentity {
  tenantId @0 :Text;
  userId @1 :Text;
  clientId @2 :Text;
}

struct SessionRecord {
  sessionId @0 :Text;
  nodeId @1 :UInt64;
  kind @2 :SessionKind;
  identity @3 :ClientIdentity;
  sessionExpiryIntervalSecs @4 :UInt32;
  hasSessionExpiryIntervalSecs @5 :Bool;
  expiresAtMs @6 :UInt64;
  hasExpiresAtMs @7 :Bool;
}

struct LookupSessionRequest {
  identity @0 :ClientIdentity;
}

struct LookupSessionByIdRequest {
  sessionId @0 :Text;
}

struct LookupSessionReply {
  record @0 :SessionRecord;
  hasRecord @1 :Bool;
}

struct RegisterSessionRequest {
  record @0 :SessionRecord;
}

struct RegisterSessionReply {
  replaced @0 :SessionRecord;
  hasReplaced @1 :Bool;
}

struct UnregisterSessionRequest {
  sessionId @0 :Text;
}

struct ListSessionsRequest {
  tenantId @0 :Text;
  hasTenantId @1 :Bool;
}

struct ListSessionsReply {
  records @0 :List(SessionRecord);
}

struct RouteRecord {
  tenantId @0 :Text;
  topicFilter @1 :Text;
  sessionId @2 :Text;
  nodeId @3 :UInt64;
  subscriptionIdentifier @4 :UInt32;
  hasSubscriptionIdentifier @5 :Bool;
  noLocal @6 :Bool;
  retainAsPublished @7 :Bool;
  sharedGroup @8 :Text;
  hasSharedGroup @9 :Bool;
  kind @10 :SessionKind;
}

struct AddRouteRequest {
  route @0 :RouteRecord;
}

struct RemoveRouteRequest {
  route @0 :RouteRecord;
}

struct ListSessionRoutesRequest {
  sessionId @0 :Text;
}

struct ListSessionRoutesReply {
  routes @0 :List(RouteRecord);
}

struct MatchTopicRequest {
  tenantId @0 :Text;
  topic @1 :Text;
}

struct MatchTopicReply {
  routes @0 :List(RouteRecord);
}

struct ListRoutesRequest {
  tenantId @0 :Text;
  hasTenantId @1 :Bool;
}

struct ListRoutesReply {
  routes @0 :List(RouteRecord);
}

struct InboxAttachRequest {
  sessionId @0 :Text;
}

struct InboxDetachRequest {
  sessionId @0 :Text;
}

struct InboxPurgeSessionRequest {
  sessionId @0 :Text;
}

struct InboxAckInflightRequest {
  sessionId @0 :Text;
  packetId @1 :UInt16;
}

struct SubscriptionRecord {
  sessionId @0 :Text;
  tenantId @1 :Text;
  topicFilter @2 :Text;
  qos @3 :UInt8;
  subscriptionIdentifier @4 :UInt32;
  hasSubscriptionIdentifier @5 :Bool;
  noLocal @6 :Bool;
  retainAsPublished @7 :Bool;
  retainHandling @8 :UInt8;
  sharedGroup @9 :Text;
  hasSharedGroup @10 :Bool;
  kind @11 :SessionKind;
}

struct RetainedMessage {
  tenantId @0 :Text;
  topic @1 :Text;
  payload @2 :Data;
  qos @3 :UInt8;
}

struct RetainWriteRequest {
  message @0 :RetainedMessage;
}

struct RetainMatchRequest {
  tenantId @0 :Text;
  topicFilter @1 :Text;
}

struct RetainMatchReply {
  messages @0 :List(RetainedMessage);
}

struct UserProperty {
  key @0 :Text;
  value @1 :Text;
}

struct PublishProperties {
  payloadFormatIndicator @0 :UInt8;
  hasPayloadFormatIndicator @1 :Bool;
  contentType @2 :Text;
  hasContentType @3 :Bool;
  messageExpiryIntervalSecs @4 :UInt32;
  hasMessageExpiryIntervalSecs @5 :Bool;
  storedAtMs @6 :UInt64;
  hasStoredAtMs @7 :Bool;
  responseTopic @8 :Text;
  hasResponseTopic @9 :Bool;
  correlationData @10 :Data;
  hasCorrelationData @11 :Bool;
  subscriptionIdentifiers @12 :List(UInt32);
  userProperties @13 :List(UserProperty);
}

struct OfflineMessage {
  tenantId @0 :Text;
  sessionId @1 :Text;
  topic @2 :Text;
  payload @3 :Data;
  qos @4 :UInt8;
  retain @5 :Bool;
  fromSessionId @6 :Text;
  properties @7 :PublishProperties;
}

enum InflightPhase {
  publish @0;
  release @1;
}

struct InflightMessage {
  tenantId @0 :Text;
  sessionId @1 :Text;
  packetId @2 :UInt16;
  topic @3 :Text;
  payload @4 :Data;
  qos @5 :UInt8;
  retain @6 :Bool;
  fromSessionId @7 :Text;
  properties @8 :PublishProperties;
  phase @9 :InflightPhase;
}

struct Delivery {
  tenantId @0 :Text;
  sessionId @1 :Text;
  topic @2 :Text;
  payload @3 :Data;
  qos @4 :UInt8;
  retain @5 :Bool;
  fromSessionId @6 :Text;
  properties @7 :PublishProperties;
}

struct ConnectRequest {
  identity @0 :ClientIdentity;
  nodeId @1 :UInt64;
  kind @2 :SessionKind;
  cleanStart @3 :Bool;
  sessionExpiryIntervalSecs @4 :UInt32;
  hasSessionExpiryIntervalSecs @5 :Bool;
}

struct PublishRequest {
  topic @0 :Text;
  payload @1 :Data;
  qos @2 :UInt8;
  retain @3 :Bool;
  properties @4 :PublishProperties;
}

struct ConnectReply {
  session @0 :SessionRecord;
  sessionPresent @1 :Bool;
  localSessionEpoch @2 :UInt64;
  replaced @3 :SessionRecord;
  hasReplaced @4 :Bool;
  offlineMessages @5 :List(OfflineMessage);
  inflightMessages @6 :List(InflightMessage);
}

enum ServiceShardKind {
  sessionDict @0;
  inbox @1;
  inflight @2;
  dist @3;
  retain @4;
}

enum ServiceShardLifecycle {
  bootstrapping @0;
  serving @1;
  draining @2;
  recovering @3;
  offline @4;
}

enum ReplicaRole {
  voter @0;
  learner @1;
}

enum ReplicaSyncState {
  probing @0;
  snapshotting @1;
  replicating @2;
  offline @3;
}

struct ServiceShardKey {
  kind @0 :ServiceShardKind;
  tenantId @1 :Text;
  scope @2 :Text;
}

struct RangeBoundary {
  startKey @0 :Data;
  hasStartKey @1 :Bool;
  endKey @2 :Data;
  hasEndKey @3 :Bool;
}

struct RangeReplica {
  nodeId @0 :UInt64;
  role @1 :ReplicaRole;
  syncState @2 :ReplicaSyncState;
}

struct ReplicatedRangeDescriptor {
  id @0 :Text;
  shard @1 :ServiceShardKey;
  boundary @2 :RangeBoundary;
  epoch @3 :UInt64;
  configVersion @4 :UInt64;
  leaderNodeId @5 :UInt64;
  hasLeaderNodeId @6 :Bool;
  replicas @7 :List(RangeReplica);
  commitIndex @8 :UInt64;
  appliedIndex @9 :UInt64;
  lifecycle @10 :ServiceShardLifecycle;
}

enum NodeServiceKind {
  broker @0;
  sessionDict @1;
  dist @2;
  inbox @3;
  retain @4;
  httpApi @5;
}

struct ServiceEndpoint {
  kind @0 :NodeServiceKind;
  nodeId @1 :UInt64;
  endpoint @2 :Text;
}

enum ClusterNodeLifecycle {
  joining @0;
  serving @1;
  suspect @2;
  leaving @3;
  offline @4;
}

struct ClusterNodeMembership {
  nodeId @0 :UInt64;
  epoch @1 :UInt64;
  lifecycle @2 :ClusterNodeLifecycle;
  endpoints @3 :List(ServiceEndpoint);
}

struct BalancerState {
  disabled @0 :Bool;
  loadRules @1 :List(UserProperty);
}

struct MemberLookupRequest {
  nodeId @0 :UInt64;
}

struct MemberRecordReply {
  member @0 :ClusterNodeMembership;
  hasMember @1 :Bool;
}

struct MemberListReply {
  members @0 :List(ClusterNodeMembership);
}

struct BalancerStateUpsertRequest {
  name @0 :Text;
  state @1 :BalancerState;
}

struct BalancerStateUpsertReply {
  previous @0 :BalancerState;
  hasPrevious @1 :Bool;
}

struct BalancerStateRequest {
  name @0 :Text;
}

struct BalancerStateReply {
  state @0 :BalancerState;
  hasState @1 :Bool;
}

struct NamedBalancerState {
  name @0 :Text;
  state @1 :BalancerState;
  hasState @2 :Bool;
}

struct BalancerStateListReply {
  entries @0 :List(NamedBalancerState);
}

struct RangeLookupRequest {
  rangeId @0 :Text;
}

struct RangeRecordReply {
  descriptor @0 :ReplicatedRangeDescriptor;
  hasDescriptor @1 :Bool;
}

struct RangeListRequest {
  shardKind @0 :ServiceShardKind;
  hasShardKind @1 :Bool;
  tenantId @2 :Text;
  hasTenantId @3 :Bool;
  scope @4 :Text;
  hasScope @5 :Bool;
}

struct RangeListReply {
  descriptors @0 :List(ReplicatedRangeDescriptor);
}

struct RouteRangeRequest {
  shard @0 :ServiceShardKey;
  key @1 :Data;
}

struct ReplicaLag {
  nodeId @0 :UInt64;
  lag @1 :UInt64;
  matchIndex @2 :UInt64;
  nextIndex @3 :UInt64;
}

enum ReconfigurationPhase {
  stagingLearners @0;
  jointConsensus @1;
  finalizing @2;
}

struct RangeReconfiguration {
  oldVoters @0 :List(UInt64);
  oldLearners @1 :List(UInt64);
  currentVoters @2 :List(UInt64);
  currentLearners @3 :List(UInt64);
  jointVoters @4 :List(UInt64);
  jointLearners @5 :List(UInt64);
  pendingVoters @6 :List(UInt64);
  pendingLearners @7 :List(UInt64);
  phase @8 :ReconfigurationPhase;
  hasPhase @9 :Bool;
  blockedOnCatchUp @10 :Bool;
}

enum RaftRole {
  follower @0;
  candidate @1;
  leader @2;
}

struct RangeHealth {
  rangeId @0 :Text;
  lifecycle @1 :ServiceShardLifecycle;
  role @2 :RaftRole;
  currentTerm @3 :UInt64;
  leaderNodeId @4 :UInt64;
  hasLeaderNodeId @5 :Bool;
  commitIndex @6 :UInt64;
  appliedIndex @7 :UInt64;
  latestSnapshotIndex @8 :UInt64;
  hasLatestSnapshotIndex @9 :Bool;
  replicaLag @10 :List(ReplicaLag);
  reconfiguration @11 :RangeReconfiguration;
}

struct RangeHealthRequest {
  rangeId @0 :Text;
}

struct RangeHealthReply {
  health @0 :RangeHealth;
  hasHealth @1 :Bool;
}

struct RangeHealthListReply {
  entries @0 :List(RangeHealth);
}

struct RangeDebugReply {
  text @0 :Text;
}

struct ZombieRange {
  rangeId @0 :Text;
  lifecycle @1 :ServiceShardLifecycle;
  leaderNodeId @2 :UInt64;
  hasLeaderNodeId @3 :Bool;
}

struct ZombieRangeListReply {
  entries @0 :List(ZombieRange);
}

struct RangeBootstrapRequest {
  descriptor @0 :ReplicatedRangeDescriptor;
}

struct RangeBootstrapReply {
  rangeId @0 :Text;
}

struct RangeChangeReplicasRequest {
  rangeId @0 :Text;
  voters @1 :List(UInt64);
  learners @2 :List(UInt64);
}

struct RangeTransferLeadershipRequest {
  rangeId @0 :Text;
  targetNodeId @1 :UInt64;
}

struct RangeRecoverRequest {
  rangeId @0 :Text;
  newLeaderNodeId @1 :UInt64;
}

struct RangeSplitRequest {
  rangeId @0 :Text;
  splitKey @1 :Data;
}

struct RangeSplitReply {
  leftRangeId @0 :Text;
  rightRangeId @1 :Text;
}

struct RangeMergeRequest {
  leftRangeId @0 :Text;
  rightRangeId @1 :Text;
}

struct RangeMergeReply {
  rangeId @0 :Text;
}

struct RangeDrainRequest {
  rangeId @0 :Text;
}

struct RangeRetireRequest {
  rangeId @0 :Text;
}

struct KvRangeGetRequest {
  rangeId @0 :Text;
  key @1 :Data;
  expectedEpoch @2 :UInt64;
}

struct KvRangeGetReply {
  value @0 :Data;
  found @1 :Bool;
}

struct KvEntry {
  key @0 :Data;
  value @1 :Data;
}

struct KvMutation {
  key @0 :Data;
  value @1 :Data;
  hasValue @2 :Bool;
}

struct KvRangeScanRequest {
  rangeId @0 :Text;
  boundary @1 :RangeBoundary;
  hasBoundary @2 :Bool;
  limit @3 :UInt32;
  expectedEpoch @4 :UInt64;
}

struct KvRangeScanReply {
  entries @0 :List(KvEntry);
}

struct KvRangeApplyRequest {
  rangeId @0 :Text;
  mutations @1 :List(KvMutation);
  expectedEpoch @2 :UInt64;
}

struct KvRangeCheckpointRequest {
  rangeId @0 :Text;
  checkpointId @1 :Text;
  expectedEpoch @2 :UInt64;
}

struct KvRangeCheckpointReply {
  rangeId @0 :Text;
  checkpointId @1 :Text;
  path @2 :Text;
}

struct KvRangeSnapshotRequest {
  rangeId @0 :Text;
  expectedEpoch @1 :UInt64;
}

struct KvRangeSnapshotReply {
  rangeId @0 :Text;
  boundary @1 :RangeBoundary;
  term @2 :UInt64;
  index @3 :UInt64;
  checksum @4 :UInt64;
  layoutVersion @5 :UInt32;
  dataPath @6 :Text;
}

struct RaftLogEntry {
  term @0 :UInt64;
  index @1 :UInt64;
  command @2 :Data;
}

struct RaftSnapshot {
  rangeId @0 :Text;
  term @1 :UInt64;
  index @2 :UInt64;
  payload @3 :Data;
}

struct RaftAppendEntriesRequest {
  term @0 :UInt64;
  leaderId @1 :UInt64;
  prevLogIndex @2 :UInt64;
  prevLogTerm @3 :UInt64;
  entries @4 :List(RaftLogEntry);
  leaderCommit @5 :UInt64;
}

struct RaftAppendEntriesResponse {
  term @0 :UInt64;
  success @1 :Bool;
  matchIndex @2 :UInt64;
}

struct RaftRequestVoteRequest {
  term @0 :UInt64;
  candidateId @1 :UInt64;
  lastLogIndex @2 :UInt64;
  lastLogTerm @3 :UInt64;
}

struct RaftRequestVoteResponse {
  term @0 :UInt64;
  voteGranted @1 :Bool;
}

struct RaftInstallSnapshotRequest {
  term @0 :UInt64;
  leaderId @1 :UInt64;
  snapshot @2 :RaftSnapshot;
}

struct RaftInstallSnapshotResponse {
  term @0 :UInt64;
  accepted @1 :Bool;
}

enum RaftMessageKind {
  appendEntries @0;
  appendEntriesResponse @1;
  requestVote @2;
  requestVoteResponse @3;
  installSnapshot @4;
  installSnapshotResponse @5;
}

struct RaftTransportRequest {
  rangeId @0 :Text;
  fromNodeId @1 :UInt64;
  kind @2 :RaftMessageKind;
  appendEntries @3 :RaftAppendEntriesRequest;
  appendEntriesResponse @4 :RaftAppendEntriesResponse;
  requestVote @5 :RaftRequestVoteRequest;
  requestVoteResponse @6 :RaftRequestVoteResponse;
  installSnapshot @7 :RaftInstallSnapshotRequest;
  installSnapshotResponse @8 :RaftInstallSnapshotResponse;
}

struct RpcFrame {
  kind @0 :FrameKind;
  requestHeader @1 :RpcRequestHeader;
  responseHeader @2 :RpcResponseHeader;
  payload @3 :Data;
}
