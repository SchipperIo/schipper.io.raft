// IonStream License 1.0
// Copyright (c) 2026 SCHIPPER.IO LLC - All Rights Reserved
// Licensor: SCHIPPER.IO LLC - Contact: contact@schipper.io

namespace Schipper.Io.Raft.Protocol;

public record VoteRequest
{
    public long Term { get; init; }
    public required string CandidateId { get; init; }
    public long LastLogIndex { get; init; }
    public long LastLogTerm { get; init; }
    public string? AuthToken { get; init; }
}

public record VoteResponse
{
    public long Term { get; init; }
    public bool VoteGranted { get; init; }
    public bool Authenticated { get; init; } = true;
}

public record AppendEntriesRequest
{
    public long Term { get; init; }
    public required string LeaderId { get; init; }
    public long PrevLogIndex { get; init; }
    public long PrevLogTerm { get; init; }
    public required IReadOnlyList<RaftLogEntry> Entries { get; init; }
    public long LeaderCommit { get; init; }
    public string? AuthToken { get; init; }
}

public record AppendEntriesResponse
{
    public long Term { get; init; }
    public bool Success { get; init; }
    public bool Authenticated { get; init; } = true;
}

public record ProposeRequest
{
    public required ReadOnlyMemory<byte> Command { get; init; }
}

public record ProposeResponse
{
    public bool Success { get; init; }
    public string? LeaderId { get; init; }
}

public record JoinRequest
{
    public required string NodeId { get; init; }
    public required string Address { get; init; }
    public string? AuthToken { get; init; }

    /// <summary>
    /// Highest log index the joining node already holds (e.g. restored from a seed/snapshot).
    /// The leader starts replication at <c>LastLogIndex + 1</c>; the default 0 keeps the
    /// historical full-replay behavior.
    /// </summary>
    public long LastLogIndex { get; init; }
}

public record JoinResponse
{
    public bool Success { get; init; }
    public string? LeaderId { get; init; }
    public IReadOnlyDictionary<string, string>? Peers { get; init; }
    public bool Authenticated { get; init; } = true;
}

public record RaftLogEntry
{
    public long Term { get; init; }
    public long Index { get; init; }
    public ReadOnlyMemory<byte> Command { get; init; }
}
