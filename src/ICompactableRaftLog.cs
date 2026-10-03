// IonStream License 1.0
// Copyright (c) 2026 SCHIPPER.IO LLC - All Rights Reserved
// Licensor: SCHIPPER.IO LLC - Contact: contact@schipper.io

using Schipper.Io.Raft.Protocol;

namespace Schipper.Io.Raft;

/// <summary>
/// A raft log that supports prefix compaction: a whole prefix of the log can be dropped once the
/// host has captured its effect elsewhere (for example in a snapshot). Hosts must never compact
/// above <see cref="IRaftNode.GetSafeCompactionIndex"/>.
/// </summary>
public interface ICompactableRaftLog : IRaftLog
{
    /// <summary>
    /// First retained index in the log: 1 if the log has never been compacted, 0 when the log is
    /// empty. After compaction this can exceed <see cref="IRaftLog.LastIndex"/> by one (all
    /// retained entries were compacted away).
    /// </summary>
    long FirstIndex { get; }

    /// <summary>
    /// Enumerates up to <paramref name="count"/> entries in descending index order, starting at
    /// <paramref name="fromIndexInclusive"/>. The range is clamped to
    /// [<see cref="FirstIndex"/>, <see cref="IRaftLog.LastIndex"/>]; requests entirely outside
    /// the retained range yield nothing.
    /// </summary>
    IAsyncEnumerable<RaftLogEntry> GetEntriesReverseAsync(long fromIndexInclusive, int count, CancellationToken cancellationToken = default);

    /// <summary>
    /// Compacts the log prefix: drops whole storage segments whose last index is below
    /// <paramref name="index"/>. A segment is never split, so <see cref="FirstIndex"/> advances
    /// to the nearest segment boundary at or below <paramref name="index"/>.
    /// </summary>
    Task CompactToAsync(long index, CancellationToken cancellationToken = default);
}
