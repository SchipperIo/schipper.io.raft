// IonStream License 1.0
// Copyright (c) 2026 SCHIPPER.IO LLC - All Rights Reserved
// Licensor: SCHIPPER.IO LLC - Contact: contact@schipper.io

using Schipper.Io.Raft.Protocol;

namespace Schipper.Io.Raft;

public interface IRaftLog
{
    long LastIndex { get; }
    long LastTerm { get; }
    long CommitIndex { get; set; }

    Task AppendAsync(IEnumerable<RaftLogEntry> entries, CancellationToken cancellationToken = default);
    Task<RaftLogEntry?> GetEntryAsync(long index, CancellationToken cancellationToken = default);
    IAsyncEnumerable<RaftLogEntry> GetEntriesAsync(long startIndex, int count, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes the entry at <paramref name="index"/> and all subsequent entries.
    /// </summary>
    Task TruncateFromAsync(long index, CancellationToken cancellationToken = default);

    Task<long> GetTermAtIndexAsync(long index, CancellationToken cancellationToken = default);
}
