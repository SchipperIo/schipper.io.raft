// IonStream License 1.0
// Copyright (c) 2026 SCHIPPER.IO LLC - All Rights Reserved
// Licensor: SCHIPPER.IO LLC - Contact: contact@schipper.io

using Schipper.Io.Raft.Protocol;

namespace Schipper.Io.Raft;

public interface IRaftNode : IDisposable
{
    string? LeaderId { get; }
    string NodeId { get; }
    bool IsReady { get; }

#pragma warning disable CA1003 // Use generic event handler instances
    event Func<RaftLogEntry, Task>? OnCommit;
#pragma warning restore CA1003
    event EventHandler<LeaderChangedEventArgs>? OnLeaderChanged;

    Task StartAsync();
    Task StopAsync();
    Task<bool> ProposeAsync(ReadOnlyMemory<byte> command, CancellationToken token = default);
    IEnumerable<string> GetUnhealthyPeers(TimeSpan threshold);
    void PromoteReadonlyPeer(string nodeId);
    bool IsReadonlyPeer(string nodeId);
    bool HasVotingPeers { get; }

    /// <summary>
    /// Highest log index that is safe to compact away. As leader this is
    /// min(matchIndex over all voting and readonly peers, CommitIndex); as follower/candidate
    /// (or with no peers) it is CommitIndex. Hosts must not compact above this value.
    /// </summary>
    long GetSafeCompactionIndex();
}
