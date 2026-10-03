// IonStream License 1.0
// Copyright (c) 2026 SCHIPPER.IO LLC - All Rights Reserved
// Licensor: SCHIPPER.IO LLC - Contact: contact@schipper.io

namespace Schipper.Io.Raft;

/// <summary>
/// Durable storage for Raft hard state (current term and the candidate voted for in that term).
/// When the <see cref="IRaftLog"/> handed to a <see cref="RaftNode"/> also implements this
/// interface, the node seeds its term/vote from <see cref="LoadHardState"/> at construction and
/// persists via <see cref="SaveHardStateAsync"/> before any externally visible term or vote
/// change, which prevents double-voting within a term across restarts.
/// </summary>
public interface IRaftHardStateStore
{
    /// <summary>
    /// Loads the persisted hard state. Returns <c>(0, null)</c> when nothing has been persisted.
    /// </summary>
    (long Term, string? VotedFor) LoadHardState();

    /// <summary>
    /// Durably persists the current term and vote. Implementations must flush to stable storage
    /// before completing so a granted vote survives a crash.
    /// </summary>
    Task SaveHardStateAsync(long term, string? votedFor, CancellationToken cancellationToken = default);
}
