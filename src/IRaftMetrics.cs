// Schipper.Io.Raft
// Copyright (c) 2026 SCHIPPER.IO LLC

namespace Schipper.Io.Raft;

/// <summary>
/// Minimal metrics sink used by <see cref="RaftNode"/>. Implement this to observe
/// term changes, leadership changes, votes, election timing, and commit progress.
/// All members are optional from the caller's perspective: <see cref="RaftNode"/>
/// accepts a nullable instance and skips reporting when none is supplied.
/// </summary>
public interface IRaftMetrics
{
    /// <summary>Reports the node's current Raft term.</summary>
    void SetRaftTerm(long term);

    /// <summary>Reports the currently known leader id (empty string when none).</summary>
    void SetRaftLeader(string leaderId);

    /// <summary>Records that a vote was received (including the candidate's self-vote).</summary>
    void RecordRaftVoteReceived();

    /// <summary>Records the duration of a completed election attempt, in seconds.</summary>
    void RecordRaftElectionDuration(double durationSeconds);

    /// <summary>Reports the node's current commit index.</summary>
    void SetRaftCommitIndex(long index);
}
