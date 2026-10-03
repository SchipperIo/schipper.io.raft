# Log and storage

[Index](README.md)

The log is the host's. `RaftNode` calls `IRaftLog`. Two implementations ship in `Schipper.Io.Raft.Storage`.

## IRaftLog

| Member | Contract |
| --- | --- |
| `LastIndex`, `LastTerm` | The tail of the log. An empty log reports index 0 |
| `CommitIndex` | Get and set on the log. `RaftNode` tracks commit on `RaftNode.CommitIndex` and does not assign this property |
| `AppendAsync` | Persist the entries before the task completes |
| `GetEntryAsync` | One entry, or null when the index is absent |
| `GetEntriesAsync` | Up to `count` entries starting at `startIndex` |
| `TruncateFromAsync` | Delete the entry at `index` and everything after it |
| `GetTermAtIndexAsync` | The term stored at `index` |

Indexes in `RaftLogEntry` start at 1. `Term` is the term that created the entry. `Command` is the opaque payload your state machine applies.

## FileRaftLog

```csharp
var log = new FileRaftLog(directory);
```

One file, `raft.log`, in `directory`. It implements `IRaftLog` only. It does not store the current term and vote, and it cannot drop a prefix. A node restarted on `FileRaftLog` loads term 0 and votes again. Use it where the process lifetime matches the log, or where another component owns hard state.

`FileRaftLog` stays in this shape so existing hosts can keep opening the same file.

## SegmentedRaftLog

```csharp
var log = new SegmentedRaftLog(directory, new SegmentedRaftLogOptions
{
    FsyncOnAppend = true,
    SegmentBytes = 4 * 1024 * 1024,
    CommitIndexFlushInterval = TimeSpan.FromSeconds(2),
});
```

This is the compactable log. It implements `ICompactableRaftLog` and `IRaftHardStateStore`.

On disk, under `directory`:

| Path | Contents |
| --- | --- |
| `meta.bin` | Commit index, current term, and voted-for. Written atomically. Hard state is fsynced on every save and copied to `meta.bin.prev`. The commit index is flushed on `CommitIndexFlushInterval` without replacing that durable vote |
| `segments/{firstIndex:D20}.seg` | Header (`RSEG`, version, first index, previous term) and frames `length u32 \| term u64 \| index u64 \| payload \| crc32` |
| `segments/{firstIndex:D20}.idx` | One `u64` file offset per entry, beside the segment. A sealed index ends with a footer and is trusted on recovery. The active segment's index is rebuilt by scanning, and a torn tail is truncated |

`RaftNode` keeps the live commit index on `RaftNode.CommitIndex`. It does not assign `IRaftLog.CommitIndex`. Set `log.CommitIndex` from the host when `meta.bin` should follow the node. That write is debounced by `CommitIndexFlushInterval` and must not replace a fsynced vote: a durable save keeps `meta.bin.prev`, and load falls back to that file when `meta.bin` is missing or corrupt. Hard state still fsyncs on every `SaveHardStateAsync`.

`FirstIndex` is 0 when the log is empty, and otherwise the first index of the oldest retained segment. After a compaction that drops every entry, `FirstIndex` can be `LastIndex + 1`.

`GetEntriesReverseAsync(fromIndexInclusive, count)` walks downward from that index, clamped to the retained range.

## Hard state

When the log implements `IRaftHardStateStore`, `RaftNode` calls `LoadHardState` in the constructor and `SaveHardStateAsync` before a term or vote change is visible to other nodes.

```csharp
public interface IRaftHardStateStore
{
    (long Term, string? VotedFor) LoadHardState();
    Task SaveHardStateAsync(long term, string? votedFor, CancellationToken cancellationToken = default);
}
```

`LoadHardState` returns `(0, null)` when nothing has been saved. `SaveHardStateAsync` completes only after the record is on stable storage.

## Compaction

```csharp
long safe = node.GetSafeCompactionIndex();
await log.CompactToAsync(safe);
```

`CompactToAsync` drops whole segments whose last index is below `index`. A segment is never split, so `FirstIndex` advances to a segment boundary at or below the index you pass. Call it only with a value at or below `GetSafeCompactionIndex`. Compacting past a peer's `matchIndex` forces that peer to be seeded out of band. The next `JoinRequest.LastLogIndex` tells the leader where replication should resume.

A peer whose `nextIndex` falls below `FirstIndex` needs that seed. The library does not send InstallSnapshot.

## Writing your own log

Implement `IRaftLog`. Add `IRaftHardStateStore` when a restart must keep the term and the vote. Add `ICompactableRaftLog` when the host will drop a prefix. `AppendAsync` and `SaveHardStateAsync` are the durability boundary: the node treats a completed task as stable. `FileRaftLog` and `SegmentedRaftLog` publish `LastIndex` / `LastTerm` only after the batch is fsynced (or flushed, when `FsyncOnAppend` is false). A failed batch truncates back to the pre-batch length. `GetTermAtIndexAsync` on a segmented frame verifies the CRC; a mismatch throws `InvalidDataException` rather than returning term 0.
