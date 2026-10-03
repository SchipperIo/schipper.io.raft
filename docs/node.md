# The node

[Index](README.md)

`RaftNode` implements `IRaftNode`. Construct it, call `StartAsync`, and keep the instance for the life of the process.

## Identity and role

| Member | Meaning |
| --- | --- |
| `NodeId` | The id passed to the constructor |
| `Role` | `RaftRole.Follower`, `Candidate`, or `Leader` |
| `LeaderId` | The leader this node currently knows. Null when it knows none |
| `IsReady` | True when `LeaderId` is non-null, including when this node is the leader |
| `CommitIndex` | The commit index the node is tracking |
| `HasVotingPeers` | True when the voting-peer map is non-empty |

`OnLeaderChanged` fires with `LeaderChangedEventArgs.LeaderId` after the node lock is released, so the handler may call back into the node (`GetSafeCompactionIndex`, `ProposeAsync`). `OnCommit` is `Func<RaftLogEntry, Task>`. The handler runs as entries are applied, and `lastApplied` advances only after it returns. Keep it ordered and return only after the command is durable in the state machine if a later crash must not reapply it as new work.

## Membership

Voting peers are the dictionary passed to the constructor. A successful `HandleJoinAsync` records the new node as a **readonly** peer. Readonly peers receive replication and count toward the safe compaction index. They do not vote and they do not count in the commit majority. With one voting member the leader commits from its own log. With two, the primary may commit from its own log without the backup. True Raft majority starts at three voting members. The voting set does not shrink.

```csharp
JoinResponse response = await node.HandleJoinAsync(new JoinRequest
{
    NodeId = "n4",
    Address = "n4.internal:7000",
    AuthToken = clusterToken,
    LastLogIndex = seededIndex, // 0 replays from the start of the log
}, cancellationToken);

if (response.Success)
{
    node.PromoteReadonlyPeer("n4"); // now a voting peer
}

await node.RemovePeerAsync("n4", cancellationToken);
bool reader = node.IsReadonlyPeer("n4");
```

`LastLogIndex` is the highest index the joiner already holds. The leader starts replication at `LastLogIndex + 1`. The default `0` replays the log from the beginning. A node restored from an out-of-band seed sets this to the seeded index so it does not download history it already has.

`JoinResponse.Peers` is the voting set plus readonly peers, as id to address. `LeaderId` on a failed join is the leader to retry against when this node is not the leader.

`PromoteReadonlyPeer` moves a readonly peer into the voting set. `RemovePeerAsync` drops the id from both sets and from the replication cursors.

## Health

```csharp
IEnumerable<string> stale = node.GetUnhealthyPeers(TimeSpan.FromSeconds(10));
```

A peer is listed when the node has a contact time for it and that time is older than the threshold. A peer that has never been contacted is omitted.

## Compaction boundary

```csharp
long safe = node.GetSafeCompactionIndex();
```

On a leader, this is the minimum of `CommitIndex` and every voting or readonly peer's `matchIndex`. On a follower or candidate, or when there are no peers, it is `CommitIndex`. Compact only up to this index. See [Log and storage](log-and-storage.md).

## Startup checks

Unless `SkipConnectionValidation` is set, `StartAsync` calls `IRaftTransport.ValidateConnectionAsync` for each voting peer and waits up to `QuorumConnectionTimeoutMs` for a quorum of those calls to succeed. `ValidateConnectionAsync` is also fired when a vote or append arrives, so the node can open the return path to the sender.

A `VoteRequest` with `Term == 0` is a probe. The handler replies with the current term and `VoteGranted = false`, and does not change term or vote.

## Metrics

`IRaftMetrics` is optional. Pass null to skip it.

| Method | When |
| --- | --- |
| `SetRaftTerm` | The current term changes |
| `SetRaftLeader` | The known leader changes. Empty string when there is none |
| `RecordRaftVoteReceived` | A vote arrives, including this node's own vote |
| `RecordRaftElectionDuration` | An election attempt finishes, in seconds |
| `SetRaftCommitIndex` | The commit index changes |

## Loop

`StartAsync` starts `RunAsync` on a background task. Call `StartAsync` once. `StopAsync` cancels that loop and waits. `Dispose` releases the node. Dispose the log after the node has stopped.
