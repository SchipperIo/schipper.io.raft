// IonStream License 1.0
// Copyright (c) 2026 SCHIPPER.IO LLC - All Rights Reserved
// Licensor: SCHIPPER.IO LLC - Contact: contact@schipper.io

using Schipper.Io.Raft.Protocol;

namespace Schipper.Io.Raft;

public interface IRaftTransport
{
    Task<VoteResponse> RequestVoteAsync(string targetNodeId, VoteRequest request, CancellationToken cancellationToken = default);
    Task<AppendEntriesResponse> AppendEntriesAsync(string targetNodeId, AppendEntriesRequest request, CancellationToken cancellationToken = default);
    Task<ProposeResponse> ProposeAsync(string targetNodeId, ProposeRequest request, CancellationToken cancellationToken = default);
    Task<JoinResponse> JoinAsync(string targetNodeId, JoinRequest request, CancellationToken cancellationToken = default);
    Task ValidateConnectionAsync(string targetNodeId, CancellationToken cancellationToken = default);
}
