// IonStream License 1.0
// Copyright (c) 2026 SCHIPPER.IO LLC - All Rights Reserved
// Licensor: SCHIPPER.IO LLC - Contact: contact@schipper.io

using Microsoft.Extensions.Logging;
using Moq;
using Schipper.Io.Raft.Protocol;

namespace Schipper.Io.Raft.Tests.Node;

[TestClass]
public class RaftVotingTests
{
    private readonly Mock<IRaftTransport> _mockTransport;
    private readonly Mock<ILogger<RaftNode>> _mockLogger;
    private readonly Mock<IRaftLog> _mockLog;
    private readonly Dictionary<string, string> _peers = new() { { "node2", "address2" }, { "node3", "address3" } };

    public RaftVotingTests()
    {
        _mockTransport = new Mock<IRaftTransport>();
        _mockLogger = new Mock<ILogger<RaftNode>>();
        _mockLog = new Mock<IRaftLog>();

        _mockLog.Setup(l => l.LastIndex).Returns(0);
        _mockLog.Setup(l => l.LastTerm).Returns(0);
    }

    [TestMethod]
    public async Task HandleRequestVote_ShouldGrantVote_WhenTermIsHigherAndLogIsUpToDate()
    {
        // Arrange
        using RaftNode node = new("node1", _peers, _mockTransport.Object, _mockLog.Object, _mockLogger.Object);
        VoteRequest request = new() { CandidateId = "node2", Term = 2, LastLogIndex = 0, LastLogTerm = 0 };

        // Act
        VoteResponse response = await node.HandleRequestVoteAsync(request, TestContext.CancellationToken);

        // Assert
        Assert.IsTrue(response.VoteGranted);
        Assert.AreEqual(2, response.Term);
    }

    [TestMethod]
    public async Task HandleRequestVote_ShouldDenyVote_WhenTermIsLower()
    {
        // Arrange
        using RaftNode node = new("node1", _peers, _mockTransport.Object, _mockLog.Object, _mockLogger.Object);

        await node.HandleRequestVoteAsync(new VoteRequest { CandidateId = "node2", Term = 2, LastLogIndex = 0, LastLogTerm = 0 }, TestContext.CancellationToken);

        VoteRequest request = new() { CandidateId = "node3", Term = 1, LastLogIndex = 0, LastLogTerm = 0 };

        // Act
        VoteResponse response = await node.HandleRequestVoteAsync(request, TestContext.CancellationToken);

        // Assert
        Assert.IsFalse(response.VoteGranted);
        Assert.AreEqual(2, response.Term);
    }

    [TestMethod]
    public async Task HandleRequestVote_ShouldDenyVote_WhenAlreadyVotedForAnotherCandidateInSameTerm()
    {
        // Arrange
        using RaftNode node = new("node1", _peers, _mockTransport.Object, _mockLog.Object, _mockLogger.Object);

        // Vote for node2 in term 2
        await node.HandleRequestVoteAsync(new VoteRequest { CandidateId = "node2", Term = 2, LastLogIndex = 0, LastLogTerm = 0 }, TestContext.CancellationToken);

        // Try to vote for node3 in same term 2
        VoteRequest request = new() { CandidateId = "node3", Term = 2, LastLogIndex = 0, LastLogTerm = 0 };

        // Act
        VoteResponse response = await node.HandleRequestVoteAsync(request, TestContext.CancellationToken);

        // Assert
        Assert.IsFalse(response.VoteGranted);
    }

    public TestContext TestContext { get; set; } = null!;
}
