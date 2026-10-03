# Schipper.Io.Raft

A transport-agnostic Raft core for .NET 10: leader election, log replication, and commit. The host supplies the network, the log, and the state machine.

The only package dependency is `Microsoft.Extensions.Logging.Abstractions`.

## Guides

| Guide | What it covers |
| --- | --- |
| [Getting started](getting-started.md) | Construct a node, propose a command, apply commits |
| [The node](node.md) | Roles, RPC handlers, membership, readiness |
| [Log and storage](log-and-storage.md) | `IRaftLog`, `FileRaftLog`, `SegmentedRaftLog`, compaction |
| [Transport and messages](transport.md) | `IRaftTransport` and the protocol records |

## Namespaces

| Namespace | Types |
| --- | --- |
| `Schipper.Io.Raft` | `RaftNode`, `IRaftNode`, `RaftRole`, `RaftOptions`, `LeaderChangedEventArgs`, `IRaftClock`, `IRaftLog`, `ICompactableRaftLog`, `IRaftHardStateStore`, `IRaftTransport`, `IRaftMetrics` |
| `Schipper.Io.Raft.Protocol` | `VoteRequest`, `VoteResponse`, `AppendEntriesRequest`, `AppendEntriesResponse`, `ProposeRequest`, `ProposeResponse`, `JoinRequest`, `JoinResponse`, `RaftLogEntry` |
| `Schipper.Io.Raft.Storage` | `FileRaftLog`, `SegmentedRaftLog`, `SegmentedRaftLogOptions` |

## Package

```xml
<PackageReference Include="Schipper.Io.Raft" Version="0.1.0-dev" />
```

Target framework: `net10.0`.

InstallSnapshot is not part of this library. A peer whose log starts above index 1 is seeded out of band, then joins with `JoinRequest.LastLogIndex`.
