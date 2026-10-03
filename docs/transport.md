# Transport and messages

[Index](README.md)

`IRaftTransport` is the outbound side. The host is the inbound side: decode a message, call the matching `Handle*` method, and send the response back.

## IRaftTransport

```csharp
public interface IRaftTransport
{
    Task<VoteResponse> RequestVoteAsync(string targetNodeId, VoteRequest request, CancellationToken cancellationToken = default);
    Task<AppendEntriesResponse> AppendEntriesAsync(string targetNodeId, AppendEntriesRequest request, CancellationToken cancellationToken = default);
    Task<ProposeResponse> ProposeAsync(string targetNodeId, ProposeRequest request, CancellationToken cancellationToken = default);
    Task<JoinResponse> JoinAsync(string targetNodeId, JoinRequest request, CancellationToken cancellationToken = default);
    Task ValidateConnectionAsync(string targetNodeId, CancellationToken cancellationToken = default);
}
```

`targetNodeId` is a key in the peer map. Resolve it to the address stored on the node, or keep your own endpoint table. The node passes the id, not the address, on every call.

`ValidateConnectionAsync` should establish that the return path to `targetNodeId` can carry later RPCs. The node calls it during startup and when a vote or append arrives from a node it may not have dialed yet. Failures are logged. A failed validation does not by itself change Raft state.

Timeouts belong in the transport. `VoteRpcDeadlineMs` and `AppendEntriesRpcDeadlineMs` are the deadlines the node expects those calls to honor. Keep them inside the election window so a stalled peer cannot freeze an election.

## Messages

All of these are records in `Schipper.Io.Raft.Protocol`. Serialize them with whatever codec the cluster agrees on. The library does not define a wire format.

### Vote

`VoteRequest`: `Term`, `CandidateId`, `LastLogIndex`, `LastLogTerm`, `AuthToken`.

`VoteResponse`: `Term`, `VoteGranted`, `Authenticated` (default true).

### Append entries

`AppendEntriesRequest`: `Term`, `LeaderId`, `PrevLogIndex`, `PrevLogTerm`, `Entries`, `LeaderCommit`, `AuthToken`.

`AppendEntriesResponse`: `Term`, `Success`, `Authenticated` (default true).

An empty `Entries` list is a heartbeat.

### Propose

`ProposeRequest`: `Command` (`ReadOnlyMemory<byte>`).

`ProposeResponse`: `Success`, `LeaderId`.

Followers forward this to the leader. The leader appends the command and returns success only after that index is committed (local commit on one voter, primary log on two voters, voting majority from three up). The state machine observes it through `OnCommit`.

### Join

`JoinRequest`: `NodeId`, `Address`, `AuthToken`, `LastLogIndex`.

`JoinResponse`: `Success`, `LeaderId`, `Peers`, `Authenticated` (default true).

The leader records the joiner as a readonly peer and starts replication at `LastLogIndex + 1`. Promote it with `PromoteReadonlyPeer` when it should vote. See [The node](node.md).

### Log entry

`RaftLogEntry`: `Term`, `Index`, `Command`.

`Command` is opaque. The node replicates the bytes and hands them to `OnCommit` unchanged.

## Auth token

Set `RaftOptions.AuthToken` to the same string on every node. The node copies it onto the vote and append requests it sends. Compare it on the requests you forward unchanged. The handler rejects a mismatch with `Authenticated = false` and does not mutate the log or the vote.

Leave `AuthToken` empty only on a network that already authenticates the transport.

## A dispatch sketch

```csharp
switch (message.Kind)
{
    case RpcKind.Vote:
        VoteResponse vote = await node.HandleRequestVoteAsync(message.Vote, cancellationToken);
        await SendAsync(message.From, vote, cancellationToken);
        break;
    case RpcKind.Append:
        AppendEntriesResponse appended = await node.HandleAppendEntriesAsync(message.Append, cancellationToken);
        await SendAsync(message.From, appended, cancellationToken);
        break;
    case RpcKind.Propose:
        ProposeResponse proposed = await node.HandleProposeAsync(message.Propose, cancellationToken);
        await SendAsync(message.From, proposed, cancellationToken);
        break;
    case RpcKind.Join:
        JoinResponse joined = await node.HandleJoinAsync(message.Join, cancellationToken);
        await SendAsync(message.From, joined, cancellationToken);
        break;
}
```

Handle one node's RPCs on that node. Sharing one `RaftNode` across processes, or calling `Handle*` on a copy, breaks the election lock and the log.

## What the transport owns

Connections, retries, authentication of the pipe, and the codec. Raft owns terms, votes, the log match, and commit. `RaftNode` will retry replication on the next heartbeat when an append throws. Surface the failure from the transport as an exception so that retry stays visible in the log.
