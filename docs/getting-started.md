# Getting started

[Index](README.md)

Three objects make a node: a log, a transport, and a `RaftNode`. The node does not open sockets. Your `IRaftTransport` does, and your host dispatches each inbound RPC to the matching `Handle*` method.

```csharp
using Microsoft.Extensions.Logging;
using Schipper.Io.Raft;
using Schipper.Io.Raft.Protocol;
using Schipper.Io.Raft.Storage;

using var log = new SegmentedRaftLog(Path.Combine(dataDir, "raft"));
var transport = new ClusterTransport(); // your IRaftTransport

var node = new RaftNode(
    nodeId: "n1",
    peers: new Dictionary<string, string>
    {
        ["n2"] = "n2.internal:7000",
        ["n3"] = "n3.internal:7000",
    },
    transport,
    log,
    logger,
    metrics: null,
    options: new RaftOptions { AuthToken = clusterToken });

node.OnCommit += entry =>
{
    Apply(entry.Index, entry.Command.Span);
    return Task.CompletedTask;
};

node.OnLeaderChanged += (_, change) =>
{
    logger.LogInformation("Leader is {Leader}", change.LeaderId ?? "(none)");
};

await node.StartAsync();
```

`peers` maps each other node's id to an address string. The string is opaque to Raft. Your transport interprets it. Leave this node out of the dictionary.

`SegmentedRaftLog` implements `IRaftHardStateStore`, so the node loads the current term and vote in the constructor and saves them before a new term or vote is visible. A restart then cannot vote twice in the same term.

## Propose

```csharp
bool accepted = await node.ProposeAsync(command, cancellationToken);
```

`ProposeAsync` appends on the leader and returns true only after `CommitIndex` includes that entry and the node is still leader in the term that appended it. One voter commits from its own log. Two voters are primary/backup: the leader's log is enough, so success does not wait for the backup. Three or more voting members require a voting majority. On a follower it forwards `ProposeRequest` to `LeaderId` through `IRaftTransport.ProposeAsync`. It returns false when this node has no leader to forward to, when the leader rejects the proposal, or when the entry is not committed.

`OnCommit` runs as the node applies committed indexes, and for a local propose that succeeds it has already observed that index. Apply in log order inside that handler. The same entry is delivered again if the process restarts and the commit index has to catch up, so the state machine has to tolerate reapplying an index it already applied, or it has to record the last applied index itself.

## Inbound RPCs

Your transport calls these on the node that received the message:

```csharp
VoteResponse vote = await node.HandleRequestVoteAsync(request, cancellationToken);
AppendEntriesResponse appended = await node.HandleAppendEntriesAsync(request, cancellationToken);
ProposeResponse proposed = await node.HandleProposeAsync(request, cancellationToken);
JoinResponse joined = await node.HandleJoinAsync(request, cancellationToken);
```

`HandleProposeAsync` is the local propose path. `ProposeAsync` calls it, then forwards when this node is not the leader. Expose `HandleProposeAsync` on the leader so followers can forward.

## Shut down

```csharp
await node.StopAsync();
node.Dispose();
log.Dispose();
```

`StopAsync` waits for the background loop, up to `RaftOptions.StopTimeoutMs` (default 5 seconds).

## Timeouts

`RaftOptions` defaults:

| Property | Default |
| --- | --- |
| `ElectionTimeoutMinMs` / `ElectionTimeoutMaxMs` | 3000 / 6000 |
| `HeartbeatIntervalMs` | 300 |
| `VoteRpcDeadlineMs` / `AppendEntriesRpcDeadlineMs` | 1500 |
| `InitialValidationDelayMs` | 1000 |
| `ConnectionRetryDelayMs` | 2000 |
| `ErrorRecoveryDelayMs` | 1000 |
| `QuorumConnectionTimeoutMs` | 5000 |
| `StopTimeoutMs` | 5000 |

`SetFastMode(true)` collapses those to test-scale values and sets `SkipConnectionValidation`. Pass `IRaftClock` and `drawElectionTimeoutMs` to the `RaftNode` constructor to control `UtcNow`, `Delay`, and the election-timeout draw. The default clock is wall-clock UTC and `Task.Delay`. The default draw is `RandomNumberGenerator.GetInt32` over `ElectionTimeoutMinMs`..`ElectionTimeoutMaxMs`. Replay of the log itself does not read the clock. Keep `Stopwatch` and `Environment.MachineName` out of term and vote transitions.

`AuthToken`, when set, must match `AuthToken` on vote, append, and join requests. An empty token accepts every request. A mismatch returns `Authenticated = false` and does not grant the vote, append, or join. `ProposeRequest` has no token field.

See [The node](node.md), [Log and storage](log-and-storage.md), and [Transport and messages](transport.md).
