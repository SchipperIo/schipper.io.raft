// IonStream License 1.0
// Copyright (c) 2026 SCHIPPER.IO LLC - All Rights Reserved
// Licensor: SCHIPPER.IO LLC - Contact: contact@schipper.io

using Schipper.Io.Raft.Protocol;
using System.Reflection;

namespace Schipper.Io.Raft.Tests.Protocol;

[TestClass]
public class RaftMessagesTests
{
    private static readonly Type[] MessageTypes =
    [
        typeof(VoteRequest),
        typeof(VoteResponse),
        typeof(AppendEntriesRequest),
        typeof(AppendEntriesResponse),
        typeof(ProposeRequest),
        typeof(ProposeResponse),
        typeof(JoinRequest),
        typeof(JoinResponse),
        typeof(RaftLogEntry)
    ];

    [TestMethod]
    public void VoteRequest_Properties_RoundTripValues()
    {
        VoteRequest request = new()
        {
            Term = 3,
            CandidateId = "node-1",
            LastLogIndex = 5,
            LastLogTerm = 2,
            AuthToken = "token"
        };

        Assert.AreEqual(3L, request.Term);
        Assert.AreEqual("node-1", request.CandidateId);
        Assert.AreEqual(5L, request.LastLogIndex);
        Assert.AreEqual(2L, request.LastLogTerm);
        Assert.AreEqual("token", request.AuthToken);
    }

    [TestMethod]
    public void VoteResponse_Properties_RoundTripValues()
    {
        VoteResponse response = new() { Term = 4, VoteGranted = true, Authenticated = false };

        Assert.AreEqual(4L, response.Term);
        Assert.IsTrue(response.VoteGranted);
        Assert.IsFalse(response.Authenticated);
    }

    [TestMethod]
    public void AppendEntriesRequest_Properties_RoundTripValues()
    {
        IReadOnlyList<RaftLogEntry> entries =
        [
            new RaftLogEntry { Term = 2, Index = 8, Command = new byte[] { 1, 2 } }
        ];

        AppendEntriesRequest request = new()
        {
            Term = 5,
            LeaderId = "leader-1",
            PrevLogIndex = 7,
            PrevLogTerm = 4,
            Entries = entries,
            LeaderCommit = 6,
            AuthToken = "token"
        };

        Assert.AreEqual(5L, request.Term);
        Assert.AreEqual("leader-1", request.LeaderId);
        Assert.AreEqual(7L, request.PrevLogIndex);
        Assert.AreEqual(4L, request.PrevLogTerm);
        Assert.AreEqual(entries, request.Entries);
        Assert.AreEqual(6L, request.LeaderCommit);
        Assert.AreEqual("token", request.AuthToken);
    }

    [TestMethod]
    public void AppendEntriesResponse_Properties_RoundTripValues()
    {
        AppendEntriesResponse response = new() { Term = 8, Success = true, Authenticated = false };

        Assert.AreEqual(8L, response.Term);
        Assert.IsTrue(response.Success);
        Assert.IsFalse(response.Authenticated);
    }

    [TestMethod]
    public void ProposeRequestAndResponse_Properties_RoundTripValues()
    {
        ReadOnlyMemory<byte> command = new byte[] { 1, 2, 3 };
        ProposeRequest request = new() { Command = command };
        ProposeResponse response = new() { Success = true, LeaderId = "leader-1" };

        CollectionAssert.AreEqual(command.ToArray(), request.Command.ToArray());
        Assert.IsTrue(response.Success);
        Assert.AreEqual("leader-1", response.LeaderId);
    }

    [TestMethod]
    public void JoinRequestAndResponse_Properties_RoundTripValues()
    {
        IReadOnlyDictionary<string, string> peers = new Dictionary<string, string>
        {
            ["node-1"] = "http://localhost:5001"
        };

        JoinRequest request = new() { NodeId = "node-2", Address = "http://localhost:5002", AuthToken = "token" };
        JoinResponse response = new() { Success = true, LeaderId = "node-1", Peers = peers, Authenticated = false };

        Assert.AreEqual("node-2", request.NodeId);
        Assert.AreEqual("http://localhost:5002", request.Address);
        Assert.AreEqual("token", request.AuthToken);
        Assert.IsTrue(response.Success);
        Assert.AreEqual("node-1", response.LeaderId);
        Assert.AreSame(peers, response.Peers);
        Assert.IsFalse(response.Authenticated);
    }

    [TestMethod]
    public void RaftLogEntry_Properties_RoundTripValues()
    {
        ReadOnlyMemory<byte> command = new byte[] { 7, 8, 9 };
        RaftLogEntry entry = new() { Term = 9, Index = 10, Command = command };

        Assert.AreEqual(9L, entry.Term);
        Assert.AreEqual(10L, entry.Index);
        CollectionAssert.AreEqual(command.ToArray(), entry.Command.ToArray());
    }

    [TestMethod]
    public void MessageTypes_HaveInvokableParameterlessConstructors()
    {
        foreach (Type messageType in MessageTypes)
        {
            ConstructorInfo constructor = messageType.GetConstructor(Type.EmptyTypes)!;

            object? instance = constructor.Invoke(null);

            Assert.IsNotNull(instance);
            Assert.AreEqual(messageType, instance.GetType());
        }
    }

    // Record equality / GetHashCode coverage (each record has 1 uncovered line -- equality/hash)

    [TestMethod]
    public void ProposeRequest_Equality_SameCommandBytesAreEqual()
    {
        byte[] bytes = [1, 2, 3];
        ProposeRequest a = new() { Command = bytes };
        ProposeRequest b = new() { Command = bytes };

        Assert.AreEqual(a, b);
        Assert.AreEqual(a.GetHashCode(), b.GetHashCode());
    }

    [TestMethod]
    public void ProposeRequest_Inequality_DifferentCommandsAreNotEqual()
    {
        ProposeRequest a = new() { Command = new byte[] { 1 } };
        ProposeRequest b = new() { Command = new byte[] { 2 } };

        Assert.AreNotEqual(a, b);
    }

    [TestMethod]
    public void ProposeResponse_Equality_SameValuesAreEqual()
    {
        ProposeResponse a = new() { Success = true, LeaderId = "leader" };
        ProposeResponse b = new() { Success = true, LeaderId = "leader" };

        Assert.AreEqual(a, b);
        Assert.AreEqual(a.GetHashCode(), b.GetHashCode());
    }

    [TestMethod]
    public void ProposeResponse_Inequality_DifferentLeaderIdIsNotEqual()
    {
        ProposeResponse a = new() { Success = true, LeaderId = "leader1" };
        ProposeResponse b = new() { Success = true, LeaderId = "leader2" };

        Assert.AreNotEqual(a, b);
    }

    [TestMethod]
    public void AppendEntriesResponse_Equality_SameValuesAreEqual()
    {
        AppendEntriesResponse a = new() { Term = 3, Success = true, Authenticated = true };
        AppendEntriesResponse b = new() { Term = 3, Success = true, Authenticated = true };

        Assert.AreEqual(a, b);
        Assert.AreEqual(a.GetHashCode(), b.GetHashCode());
    }

    [TestMethod]
    public void AppendEntriesResponse_Inequality_DifferentTermIsNotEqual()
    {
        AppendEntriesResponse a = new() { Term = 1, Success = true };
        AppendEntriesResponse b = new() { Term = 2, Success = true };

        Assert.AreNotEqual(a, b);
    }

    [TestMethod]
    public void JoinRequest_Equality_SameValuesAreEqual()
    {
        JoinRequest a = new() { NodeId = "n1", Address = "addr1", AuthToken = "tok" };
        JoinRequest b = new() { NodeId = "n1", Address = "addr1", AuthToken = "tok" };

        Assert.AreEqual(a, b);
        Assert.AreEqual(a.GetHashCode(), b.GetHashCode());
    }

    [TestMethod]
    public void JoinRequest_Inequality_DifferentAddressIsNotEqual()
    {
        JoinRequest a = new() { NodeId = "n1", Address = "addr1" };
        JoinRequest b = new() { NodeId = "n1", Address = "addr2" };

        Assert.AreNotEqual(a, b);
    }

    [TestMethod]
    public void RaftLogEntry_Equality_SameBytesAreEqual()
    {
        byte[] cmd = [7, 8];
        RaftLogEntry a = new() { Term = 2, Index = 5, Command = cmd };
        RaftLogEntry b = new() { Term = 2, Index = 5, Command = cmd };

        Assert.AreEqual(a, b);
        Assert.AreEqual(a.GetHashCode(), b.GetHashCode());
    }

    [TestMethod]
    public void RaftLogEntry_Inequality_DifferentIndexIsNotEqual()
    {
        byte[] cmd = [1];
        RaftLogEntry a = new() { Term = 1, Index = 1, Command = cmd };
        RaftLogEntry b = new() { Term = 1, Index = 2, Command = cmd };

        Assert.AreNotEqual(a, b);
    }

    [TestMethod]
    public void VoteResponse_Equality_SameValuesAreEqual()
    {
        VoteResponse a = new() { Term = 5, VoteGranted = true, Authenticated = true };
        VoteResponse b = new() { Term = 5, VoteGranted = true, Authenticated = true };

        Assert.AreEqual(a, b);
        Assert.AreEqual(a.GetHashCode(), b.GetHashCode());
    }

    [TestMethod]
    public void VoteResponse_Inequality_DifferentVoteGrantedIsNotEqual()
    {
        VoteResponse a = new() { Term = 1, VoteGranted = true };
        VoteResponse b = new() { Term = 1, VoteGranted = false };

        Assert.AreNotEqual(a, b);
    }

    [TestMethod]
    public void VoteResponse_WithExpression_ProducesNewRecordWithUpdatedField()
    {
        VoteResponse original = new() { Term = 1, VoteGranted = false, Authenticated = true };
        VoteResponse modified = original with { VoteGranted = true };

        Assert.IsTrue(modified.VoteGranted);
        Assert.AreEqual(1, modified.Term);
        Assert.AreNotEqual(original, modified);
    }

    [TestMethod]
    public void AppendEntriesResponse_WithExpression_ProducesNewRecordWithUpdatedField()
    {
        AppendEntriesResponse original = new() { Term = 2, Success = false, Authenticated = true };
        AppendEntriesResponse modified = original with { Success = true };

        Assert.IsTrue(modified.Success);
        Assert.AreEqual(2, modified.Term);
        Assert.AreNotEqual(original, modified);
    }

    [TestMethod]
    public void ProposeResponse_WithExpression_ProducesNewRecordWithUpdatedField()
    {
        ProposeResponse original = new() { Success = false, LeaderId = "leader1" };
        ProposeResponse modified = original with { LeaderId = "leader2" };

        Assert.AreEqual("leader2", modified.LeaderId);
        Assert.IsFalse(modified.Success);
        Assert.AreNotEqual(original, modified);
    }

    [TestMethod]
    public void JoinRequest_WithExpression_ProducesNewRecordWithUpdatedField()
    {
        JoinRequest original = new() { NodeId = "n1", Address = "addr1", AuthToken = null };
        JoinRequest modified = original with { AuthToken = "tok" };

        Assert.AreEqual("tok", modified.AuthToken);
        Assert.AreEqual("n1", modified.NodeId);
        Assert.AreNotEqual(original, modified);
    }

    [TestMethod]
    public void RaftLogEntry_WithExpression_ProducesNewRecordWithUpdatedField()
    {
        RaftLogEntry original = new() { Term = 1, Index = 1, Command = new byte[] { 1 } };
        RaftLogEntry modified = original with { Term = 2 };

        Assert.AreEqual(2, modified.Term);
        Assert.AreEqual(1, modified.Index);
        Assert.AreNotEqual(original, modified);
    }

    [TestMethod]
    [DataRow(true, "leader")]
    [DataRow(false, null)]
    [DataRow(false, "other")]
    public void ProposeResponse_DefaultSuccess_IsFalse_WhenNotSet(bool success, string? leaderId)
    {
        ProposeResponse r = new() { Success = success, LeaderId = leaderId };

        Assert.AreEqual(success, r.Success);
        Assert.AreEqual(leaderId, r.LeaderId);
    }

    [TestMethod]
    [DataRow(true, true)]
    [DataRow(false, false)]
    [DataRow(true, false)]
    public void AppendEntriesResponse_AuthenticatedDefaultIsTrue(bool success, bool authenticated)
    {
        AppendEntriesResponse r = new() { Term = 1, Success = success, Authenticated = authenticated };

        Assert.AreEqual(success, r.Success);
        Assert.AreEqual(authenticated, r.Authenticated);
    }

    [TestMethod]
    [DataRow("n1", "addr1", null)]
    [DataRow("n2", "addr2", "tok")]
    public void JoinRequest_ParameterizedProperties_RoundTrip(string nodeId, string address, string? authToken)
    {
        JoinRequest r = new() { NodeId = nodeId, Address = address, AuthToken = authToken };

        Assert.AreEqual(nodeId, r.NodeId);
        Assert.AreEqual(address, r.Address);
        Assert.AreEqual(authToken, r.AuthToken);
    }

    [TestMethod]
    public void VoteResponse_DefaultAuthenticated_IsTrue()
    {
        VoteResponse r = new() { Term = 0, VoteGranted = false };

        Assert.IsTrue(r.Authenticated);
    }

    [TestMethod]
    public void AppendEntriesResponse_DefaultAuthenticated_IsTrue()
    {
        AppendEntriesResponse r = new() { Term = 0, Success = false };

        Assert.IsTrue(r.Authenticated);
    }
}
