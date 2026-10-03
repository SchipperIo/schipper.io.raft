// IonStream License 1.0
// Copyright (c) 2026 SCHIPPER.IO LLC - All Rights Reserved
// Licensor: SCHIPPER.IO LLC - Contact: contact@schipper.io

using Schipper.Io.Raft.Protocol;
using Microsoft.Extensions.Logging;
using Moq;
using System.Collections.Concurrent;
using System.Reflection;

namespace Schipper.Io.Raft.Tests.Node;

[TestClass]
public class RaftNodeTests : IDisposable
{
    private const BindingFlags PrivateInstanceFlags = BindingFlags.Instance | BindingFlags.NonPublic;
    private static readonly string[] ExpectedUnhealthyPeer = ["node2"];
    private readonly Mock<IRaftTransport> _mockTransport;
    private readonly Mock<IRaftLog> _mockLog;
    private readonly Mock<ILogger<RaftNode>> _mockLogger;
    private readonly Dictionary<string, string> _peers;
    private readonly RaftNode _node;

    public RaftNodeTests()
    {
        _mockTransport = new Mock<IRaftTransport>();
        _mockLog = new Mock<IRaftLog>();
        _mockLogger = new Mock<ILogger<RaftNode>>();
        _peers = new Dictionary<string, string> { { "node2", "localhost:5002" }, { "node3", "localhost:5003" } };

        // Setup default log behavior
        _mockLog.Setup(l => l.LastIndex).Returns(0);
        _mockLog.Setup(l => l.LastTerm).Returns(0);
        _mockLog.Setup(l => l.GetTermAtIndexAsync(It.IsAny<long>(), It.IsAny<CancellationToken>())).ReturnsAsync(0);
        _mockLog.Setup(l => l.GetEntriesAsync(It.IsAny<long>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .Returns(AsyncEnumerable.Empty<RaftLogEntry>());

        // Enable Information logging by default
        _mockLogger.Setup(x => x.IsEnabled(LogLevel.Information)).Returns(true);

        // Use options with no initial validation delay for faster tests
        RaftOptions options = new() { InitialValidationDelayMs = 0 };
        _node = new RaftNode("node1", _peers, _mockTransport.Object, _mockLog.Object, _mockLogger.Object, options: options);
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (disposing)
        {
            _node.Dispose();
        }
    }

    [TestMethod]
    public async Task HandleRequestVote_ShouldGrantVote_WhenTermIsNewerAndLogIsUpToDate()
    {
        // Arrange
        VoteRequest request = new()
        {
            Term = 2,
            CandidateId = "node2",
            LastLogIndex = 0,
            LastLogTerm = 0
        };

        // Act
        VoteResponse response = await _node.HandleRequestVoteAsync(request, TestContext.CancellationToken);

        // Assert
        Assert.IsTrue(response.VoteGranted);
        Assert.AreEqual(2, response.Term);
    }

    [TestMethod]
    public async Task HandleRequestVote_ShouldRejectVote_WhenTermIsOlder()
    {
        // Arrange
        // First, bump the node's term to 2
        await _node.HandleRequestVoteAsync(new VoteRequest { Term = 2, CandidateId = "node2", LastLogIndex = 0, LastLogTerm = 0 }, TestContext.CancellationToken);

        VoteRequest request = new()
        {
            Term = 1,
            CandidateId = "node3",
            LastLogIndex = 0,
            LastLogTerm = 0
        };

        // Act
        VoteResponse response = await _node.HandleRequestVoteAsync(request, TestContext.CancellationToken);

        // Assert
        Assert.IsFalse(response.VoteGranted);
        Assert.AreEqual(2, response.Term); // Should return current term
    }

    [TestMethod]
    public async Task HandleRequestVote_ShouldRejectVote_WhenAlreadyVotedForSomeoneElse()
    {
        // Arrange
        // Vote for node2 in term 2
        await _node.HandleRequestVoteAsync(new VoteRequest { Term = 2, CandidateId = "node2", LastLogIndex = 0, LastLogTerm = 0 }, TestContext.CancellationToken);

        // Try to vote for node3 in same term
        VoteRequest request = new()
        {
            Term = 2,
            CandidateId = "node3",
            LastLogIndex = 0,
            LastLogTerm = 0
        };

        // Act
        VoteResponse response = await _node.HandleRequestVoteAsync(request, TestContext.CancellationToken);

        // Assert
        Assert.IsFalse(response.VoteGranted);
        Assert.AreEqual(2, response.Term);
    }

    [TestMethod]
    public async Task HandleRequestVoteAsync_ShouldReject_WhenLastLogTermIsLower()
    {
        // Arrange
        _node._currentTerm = 2;
        _mockLog.Setup(l => l.LastTerm).Returns(2);
        _mockLog.Setup(l => l.LastIndex).Returns(10);

        VoteRequest request = new()
        {
            Term = 2,
            CandidateId = "node2",
            LastLogIndex = 10,
            LastLogTerm = 1 // Lower than 2
        };

        // Act
        VoteResponse response = await _node.HandleRequestVoteAsync(request, TestContext.CancellationToken);

        // Assert
        Assert.IsFalse(response.VoteGranted);
    }

    [TestMethod]
    public async Task HandleRequestVoteAsync_ShouldReject_WhenLastLogTermEqualButIndexLower()
    {
        // Arrange
        _node._currentTerm = 2;
        _mockLog.Setup(l => l.LastTerm).Returns(2);
        _mockLog.Setup(l => l.LastIndex).Returns(10);

        VoteRequest request = new()
        {
            Term = 2,
            CandidateId = "node2",
            LastLogIndex = 9, // Lower than 10
            LastLogTerm = 2
        };

        // Act
        VoteResponse response = await _node.HandleRequestVoteAsync(request, TestContext.CancellationToken);

        // Assert
        Assert.IsFalse(response.VoteGranted);
    }

    [TestMethod]
    public async Task HandleAppendEntries_ShouldUpdateTerm_WhenReceivedHigherTerm()
    {
        // Arrange
        AppendEntriesRequest request = new()
        {
            Term = 5,
            LeaderId = "node2",
            PrevLogIndex = 0,
            PrevLogTerm = 0,
            Entries = [],
            LeaderCommit = 0
        };

        // Act
        AppendEntriesResponse response = await _node.HandleAppendEntriesAsync(request, TestContext.CancellationToken);

        // Assert
        Assert.IsTrue(response.Success);
        Assert.AreEqual(5, response.Term);
    }

    [TestMethod]
    public async Task HandleAppendEntries_ShouldReject_WhenTermIsOlder()
    {
        // Arrange
        // Bump term to 5
        await _node.HandleAppendEntriesAsync(new AppendEntriesRequest { Term = 5, LeaderId = "node2", PrevLogIndex = 0, PrevLogTerm = 0, Entries = [], LeaderCommit = 0 }, TestContext.CancellationToken);

        AppendEntriesRequest request = new()
        {
            Term = 3, // Older
            LeaderId = "node3",
            PrevLogIndex = 0,
            PrevLogTerm = 0,
            Entries = [],
            LeaderCommit = 0
        };

        // Act
        AppendEntriesResponse response = await _node.HandleAppendEntriesAsync(request, TestContext.CancellationToken);

        // Assert
        Assert.IsFalse(response.Success);
        Assert.AreEqual(5, response.Term);
    }

    [TestMethod]
    public async Task ProposeAsync_ShouldAppendToLog_WhenLeader()
    {
        // Arrange
        _node._role = RaftRole.Leader;
        _node._currentTerm = 1;

        long lastIndex = 10;
        _mockLog.Setup(l => l.LastIndex).Returns(() => lastIndex);
        _mockLog.Setup(l => l.GetTermAtIndexAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((long index, CancellationToken _) => index == 0 ? 0L : 1L);

        byte[] command = [1, 2, 3];
        RaftLogEntry[]? capturedEntries = null;
        _mockLog.Setup(l => l.AppendAsync(It.IsAny<IEnumerable<RaftLogEntry>>(), It.IsAny<CancellationToken>()))
            .Callback<IEnumerable<RaftLogEntry>, CancellationToken>((e, ct) =>
            {
                capturedEntries = [.. e];
                lastIndex = capturedEntries[^1].Index;
            })
            .Returns(Task.CompletedTask);
        _mockLog.Setup(l => l.GetEntriesAsync(It.IsAny<long>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .Returns((long start, int _, CancellationToken _) =>
                start <= lastIndex
                    ? AsAsync([new RaftLogEntry { Index = lastIndex, Term = 1, Command = command }])
                    : AsyncEnumerable.Empty<RaftLogEntry>());
        _mockLog.Setup(l => l.GetEntryAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((long idx, CancellationToken _) => new RaftLogEntry { Index = idx, Term = 1, Command = command });

        _mockTransport.Setup(t => t.AppendEntriesAsync(It.IsAny<string>(), It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new AppendEntriesResponse { Term = 1, Success = true });

        bool result = await _node.ProposeAsync(command, TestContext.CancellationToken);

        Assert.IsTrue(result);
        Assert.AreEqual(11, _node._commitIndex);
        Assert.IsNotNull(capturedEntries);
        Assert.HasCount(1, capturedEntries);
        Assert.AreEqual(11, capturedEntries[0].Index);
    }

    [TestMethod]
    public async Task ProposeAsync_ShouldForwardToLeader_WhenFollower()
    {
        // Arrange
        _node._role = RaftRole.Follower;
        _node.LeaderId = "node2";

        byte[] command = [1, 2, 3];
        _mockTransport.Setup(t => t.ProposeAsync("node2", It.IsAny<ProposeRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ProposeResponse { Success = true, LeaderId = "node2" });

        // Act
        bool result = await _node.ProposeAsync(command, TestContext.CancellationToken);

        // Assert
        Assert.IsTrue(result);
        _mockTransport.Verify(t => t.ProposeAsync("node2", It.IsAny<ProposeRequest>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [TestMethod]
    public async Task HandleAppendEntries_ShouldTruncateLog_WhenConflictFound()
    {
        // Arrange
        // Log has entry at 10 with term 1
        _mockLog.Setup(l => l.LastIndex).Returns(10);
        _mockLog.Setup(l => l.GetTermAtIndexAsync(10, It.IsAny<CancellationToken>())).ReturnsAsync(1);

        // Request sends entry at 10 with term 2 (conflict)
        AppendEntriesRequest request = new()
        {
            Term = 2,
            LeaderId = "node2",
            PrevLogIndex = 9,
            PrevLogTerm = 1,
            Entries = [new RaftLogEntry { Index = 10, Term = 2, Command = ReadOnlyMemory<byte>.Empty }],
            LeaderCommit = 0
        };

        _mockLog.Setup(l => l.GetTermAtIndexAsync(9, It.IsAny<CancellationToken>())).ReturnsAsync(1); // PrevLog match

        // Act
        AppendEntriesResponse response = await _node.HandleAppendEntriesAsync(request, TestContext.CancellationToken);

        // Assert
        Assert.IsTrue(response.Success);
        _mockLog.Verify(l => l.TruncateFromAsync(10, It.IsAny<CancellationToken>()), Times.Once);
        _mockLog.Verify(l => l.AppendAsync(It.IsAny<RaftLogEntry[]>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [TestMethod]
    public async Task HandleAppendEntries_ShouldUpdateCommitIndex_WhenLeaderCommitIsHigher()
    {
        // Arrange
        _mockLog.Setup(l => l.LastIndex).Returns(10);
        _mockLog.Setup(l => l.GetTermAtIndexAsync(It.IsAny<long>(), It.IsAny<CancellationToken>())).ReturnsAsync(1);

        // Capture commit callback
        List<RaftLogEntry> committedEntries = [];
        _node.OnCommit += (entry) => { committedEntries.Add(entry); return Task.CompletedTask; };
        _mockLog.Setup(l => l.GetEntryAsync(It.IsAny<long>(), It.IsAny<CancellationToken>())).ReturnsAsync((long index, CancellationToken ct) => new RaftLogEntry { Index = index, Term = 1 });

        AppendEntriesRequest request = new()
        {
            Term = 1,
            LeaderId = "node2",
            PrevLogIndex = 10,
            PrevLogTerm = 1,
            Entries = [],
            LeaderCommit = 5 // Leader says 5 is committed
        };

        // Act
        await _node.HandleAppendEntriesAsync(request, TestContext.CancellationToken);

        // Assert
        // Commit index should move from 0 to 5
        Assert.HasCount(5, committedEntries);
        Assert.AreEqual(5, committedEntries.Last().Index);
    }

    [TestMethod]
    public async Task RunElectionAsync_ShouldSendRequestVoteToAllPeers()
    {
        // Arrange
        // Ensure we are Candidate and Term is 1
        _node._role = RaftRole.Candidate;
        _node._currentTerm = 1;

        // Mock transport to return votes with same term
        _mockTransport.Setup(t => t.RequestVoteAsync(It.IsAny<string>(), It.IsAny<VoteRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new VoteResponse { Term = 1, VoteGranted = true });

        // Act
        // Invoke internal RunElectionAsync
        await _node.RunElectionAsync(CancellationToken.None);

        // Assert
        // Should have sent RequestVote to both peers (node2, node3)
        _mockTransport.Verify(t => t.RequestVoteAsync("node2", It.IsAny<VoteRequest>(), It.IsAny<CancellationToken>()), Times.Once);
        _mockTransport.Verify(t => t.RequestVoteAsync("node3", It.IsAny<VoteRequest>(), It.IsAny<CancellationToken>()), Times.Once);

        // Should become Leader because it got votes from everyone (plus self)
        Assert.AreEqual(RaftRole.Leader, _node._role);
    }

    [TestMethod]
    public async Task SendHeartbeatsAsync_ShouldSendAppendEntriesToAllPeers()
    {
        // Arrange
        _node._role = RaftRole.Leader;
        _node._currentTerm = 1;

        _mockTransport.Setup(t => t.AppendEntriesAsync(It.IsAny<string>(), It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new AppendEntriesResponse { Term = 1, Success = true });

        // Act
        await _node.SendHeartbeatsAsync(CancellationToken.None);

        // Assert
        _mockTransport.Verify(t => t.AppendEntriesAsync("node2", It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()), Times.Once);
        _mockTransport.Verify(t => t.AppendEntriesAsync("node3", It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [TestMethod]
    public async Task CheckElectionTimeoutAsync_ShouldBecomeCandidate_WhenTimeoutElapsed()
    {
        // Arrange
        _node._role = RaftRole.Follower;
        _node._lastHeartbeat = DateTimeOffset.UtcNow.AddSeconds(-10);
        _node._currentElectionTimeout = TimeSpan.FromSeconds(1);
        _node._currentTerm = 1;

        // Act
        await _node.CheckElectionTimeoutAsync(CancellationToken.None);

        // Assert
        Assert.AreEqual(RaftRole.Candidate, _node._role);
        Assert.AreEqual(2, _node._currentTerm);
        Assert.AreEqual("node1", _node._votedFor);
    }

    [TestMethod]
    public async Task CheckElectionTimeoutAsync_ShouldNotBecomeCandidate_WhenTimeoutNotElapsed()
    {
        // Arrange
        _node._role = RaftRole.Follower;
        _node._lastHeartbeat = DateTimeOffset.UtcNow;
        _node._currentElectionTimeout = TimeSpan.FromSeconds(10);
        _node._currentTerm = 1;

        // Act
        await _node.CheckElectionTimeoutAsync(CancellationToken.None);

        // Assert
        Assert.AreEqual(RaftRole.Follower, _node._role);
        Assert.AreEqual(1, _node._currentTerm);
    }

    [TestMethod]
    public async Task RunElectionAsync_ShouldStepDown_WhenHigherTermReceived()
    {
        // Arrange
        _node._role = RaftRole.Candidate;
        _node._currentTerm = 1;

        _mockTransport.Setup(t => t.RequestVoteAsync(It.IsAny<string>(), It.IsAny<VoteRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new VoteResponse { Term = 2, VoteGranted = false });

        // Act
        await _node.RunElectionAsync(CancellationToken.None);

        // Assert
        Assert.AreEqual(RaftRole.Follower, _node._role);
        Assert.AreEqual(2, _node._currentTerm);
    }

    [TestMethod]
    public async Task RunElectionAsync_ShouldStayCandidate_WhenElectionLost()
    {
        // Arrange
        _node._role = RaftRole.Candidate;
        _node._currentTerm = 1;

        _mockTransport.Setup(t => t.RequestVoteAsync(It.IsAny<string>(), It.IsAny<VoteRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new VoteResponse { Term = 1, VoteGranted = false });

        // Act
        await _node.RunElectionAsync(CancellationToken.None);

        // Assert
        Assert.AreEqual(RaftRole.Candidate, _node._role);
        Assert.AreEqual(1, _node._currentTerm);
    }

    [TestMethod]
    public async Task SendHeartbeatsAsync_ShouldUpdateNextIndex_WhenLogReplicationSucceeds()
    {
        // Arrange
        _node._role = RaftRole.Leader;
        _node._currentTerm = 1;
        _mockLog.Setup(l => l.LastIndex).Returns(11);
        _mockLog.Setup(l => l.GetTermAtIndexAsync(It.IsAny<long>(), It.IsAny<CancellationToken>())).ReturnsAsync(1);
        _mockLog.Setup(l => l.GetEntriesAsync(It.IsAny<long>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .Returns((long _, int _, CancellationToken _) => AsAsync([new RaftLogEntry { Index = 11, Term = 1 }]));

        _node._nextIndex["node2"] = 11;
        _node._nextIndex["node3"] = 11;

        _mockTransport.Setup(t => t.AppendEntriesAsync(It.IsAny<string>(), It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new AppendEntriesResponse { Term = 1, Success = true });

        // Act
        await _node.SendHeartbeatsAsync(CancellationToken.None);

        // Assert
        Assert.AreEqual(12, _node._nextIndex["node2"]);
        Assert.AreEqual(11, _node._matchIndex["node2"]);
    }

    [TestMethod]
    public async Task SendHeartbeatsAsync_ShouldDecrementNextIndex_WhenLogReplicationFails()
    {
        // Arrange
        _node._role = RaftRole.Leader;
        _node._currentTerm = 1;
        _mockLog.Setup(l => l.LastIndex).Returns(10);
        _mockLog.Setup(l => l.GetTermAtIndexAsync(It.IsAny<long>(), It.IsAny<CancellationToken>())).ReturnsAsync(1);
        _mockLog.Setup(l => l.GetEntriesAsync(It.IsAny<long>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .Returns(AsyncEnumerable.Empty<RaftLogEntry>());

        _node._nextIndex["node2"] = 10;
        _node._nextIndex["node3"] = 10;

        _mockTransport.Setup(t => t.AppendEntriesAsync(It.IsAny<string>(), It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new AppendEntriesResponse { Term = 1, Success = false });

        // Act
        await _node.SendHeartbeatsAsync(CancellationToken.None);

        // Assert
        Assert.AreEqual(9, _node._nextIndex["node2"]);
    }

    [TestMethod]
    public async Task SendHeartbeatsAsync_ShouldUpdateCommitIndex_WhenMajorityReplicated()
    {
        // Arrange
        _node._role = RaftRole.Leader;
        _node._currentTerm = 1;
        _node._commitIndex = 0;

        // Log has entry 1 with term 1
        _mockLog.Setup(l => l.LastIndex).Returns(1);
        _mockLog.Setup(l => l.GetTermAtIndexAsync(It.IsAny<long>(), It.IsAny<CancellationToken>())).ReturnsAsync(1);
        _mockLog.Setup(l => l.GetEntriesAsync(It.IsAny<long>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .Returns((long _, int _, CancellationToken _) => AsAsync([new RaftLogEntry { Index = 1, Term = 1 }]));

        // Peers need entry 1
        _node._nextIndex["node2"] = 1;
        _node._nextIndex["node3"] = 1;

        _mockTransport.Setup(t => t.AppendEntriesAsync(It.IsAny<string>(), It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new AppendEntriesResponse { Term = 1, Success = true });

        // Act
        await _node.SendHeartbeatsAsync(CancellationToken.None);

        // Assert
        // Majority is 2 (Leader + 1 peer).
        // Both peers succeed, so matchIndex becomes 1 for both.
        // Leader matchIndex is 1.
        // Sorted matchIndexes: 1, 1, 1. Median is 1.
        // CommitIndex should be 1.
        Assert.AreEqual(1, _node._commitIndex);
    }

    [TestMethod]
    public async Task ApplyEntriesAsync_ShouldLogError_WhenOnCommitThrows()
    {
        // Arrange
        _mockLogger.Setup(x => x.IsEnabled(LogLevel.Error)).Returns(true);
        _mockLog.Setup(l => l.GetEntryAsync(It.IsAny<long>(), It.IsAny<CancellationToken>())).ReturnsAsync(new RaftLogEntry { Index = 1, Term = 1 });
        _node.OnCommit += (entry) => throw new InvalidOperationException("Commit failed");

        InvalidOperationException ex = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => _node.ApplyEntriesAsync(1, 1, CancellationToken.None));

        Assert.AreEqual("Commit failed", ex.Message);
        Assert.AreEqual(0L, _node._lastApplied);
#pragma warning disable CA1873 // Avoid expensive logging
        _mockLogger.Verify(
            x => x.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Error applying committed entry")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
#pragma warning restore CA1873
    }

    [TestMethod]
    public async Task StartAsync_ShouldRunLoopAndDispose()
    {
        // Arrange
        // Act
        await _node.StartAsync();
        await Task.Delay(100, It.IsAny<CancellationToken>());
        // Dispose is called by test class cleanup
    }

    // Helper to set private fields
    // Removed reflection helpers as we now use InternalsVisibleTo

    [TestMethod]
    public async Task RunElectionAsync_ShouldHandleTransportException()
    {
        // Arrange
        _mockLogger.Setup(x => x.IsEnabled(LogLevel.Warning)).Returns(true);
        _node._role = RaftRole.Candidate;
        _node._currentTerm = 1;
        _mockTransport.Setup(t => t.RequestVoteAsync(It.IsAny<string>(), It.IsAny<VoteRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new Exception("Network error"));

        // Act
        await _node.RunElectionAsync(CancellationToken.None);

        // Assert
        // Should not crash
        // Verify warning logged
#pragma warning disable CA1873
        _mockLogger.Verify(
            x => x.Log(
                LogLevel.Warning,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Failed to request vote")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.AtLeastOnce);
#pragma warning restore CA1873
    }

    [TestMethod]
    public async Task SendHeartbeatsAsync_ShouldHandleTransportException()
    {
        // Arrange
        _mockLogger.Setup(x => x.IsEnabled(LogLevel.Warning)).Returns(true);
        _node._role = RaftRole.Leader;
        _node._currentTerm = 1;
        _mockTransport.Setup(t => t.AppendEntriesAsync(It.IsAny<string>(), It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new Exception("Network error"));

        // Act
        await _node.SendHeartbeatsAsync(CancellationToken.None);

        // Assert
        // Should not crash
#pragma warning disable CA1873
        _mockLogger.Verify(
            x => x.Log(
                LogLevel.Warning,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("failed --")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.AtLeastOnce);
#pragma warning restore CA1873
    }

    [TestMethod]
    public async Task ProposeAsync_ShouldHandleTransportException_WhenForwarding()
    {
        // Arrange
        _mockLogger.Setup(x => x.IsEnabled(LogLevel.Warning)).Returns(true);
        _node._role = RaftRole.Follower;
        _node.LeaderId = "node2";
        _mockTransport.Setup(t => t.ProposeAsync("localhost:5002", It.IsAny<ProposeRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new Exception("Network error"));

        // Act
        bool result = await _node.ProposeAsync(new byte[] { 1 }, TestContext.CancellationToken);

        // Assert
        Assert.IsFalse(result);
#pragma warning disable CA1873
        _mockLogger.Verify(
            x => x.Log(
                LogLevel.Warning,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Failed to forward proposal")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
#pragma warning restore CA1873
    }

    [TestMethod]
    public async Task ProposeAsync_ShouldReturnFalse_WhenLeaderNotFound()
    {
        // Arrange
        _mockLogger.Setup(x => x.IsEnabled(LogLevel.Warning)).Returns(true);
        _node._role = RaftRole.Follower;
        _node.LeaderId = "node99"; // Not in peers

        // Act
        bool result = await _node.ProposeAsync(new byte[] { 1 }, TestContext.CancellationToken);

        // Assert
        Assert.IsFalse(result);
#pragma warning disable CA1873
        _mockLogger.Verify(
            x => x.Log(
                LogLevel.Warning,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Leader node99 not found")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
#pragma warning restore CA1873
    }

    [TestMethod]
    public async Task RunAsync_ShouldTransitionToCandidate_WhenTimeoutElapses()
    {
        // Arrange
        _node._role = RaftRole.Follower;
        _node._lastHeartbeat = DateTimeOffset.UtcNow.AddSeconds(-10); // Way past timeout
        _node._currentElectionTimeout = TimeSpan.FromMilliseconds(100);

        // Mock transport to avoid errors during election
        _mockTransport.Setup(t => t.RequestVoteAsync(It.IsAny<string>(), It.IsAny<VoteRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new VoteResponse { Term = 1, VoteGranted = true });

        using CancellationTokenSource cts = new();
        cts.CancelAfter(150); // Run for a bit

        // Act
        await _node.RunAsync(cts.Token);

        // Assert
        Assert.AreEqual(RaftRole.Leader, _node._role); // Should have become Candidate -> Leader (since votes granted)
    }

    [TestMethod]
    public async Task RunAsync_ShouldSendHeartbeats_WhenLeader()
    {
        // Arrange
        _node._role = RaftRole.Leader;
        _node._currentTerm = 1;

        _mockTransport.Setup(t => t.AppendEntriesAsync(It.IsAny<string>(), It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new AppendEntriesResponse { Term = 1, Success = true });

        using CancellationTokenSource cts = new();
        cts.CancelAfter(150); // Run for a bit

        // Act
        await _node.RunAsync(cts.Token);

        // Assert
        _mockTransport.Verify(t => t.AppendEntriesAsync(It.IsAny<string>(), It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()), Times.AtLeastOnce);
    }

    [TestMethod]
    public async Task RunAsync_ShouldLogWarning_WhenTransportFails()
    {
        // Arrange
        _mockLogger.Setup(x => x.IsEnabled(LogLevel.Warning)).Returns(true);
        _node._role = RaftRole.Leader;

        // Force exception in SendHeartbeatsAsync via transport
        _mockTransport.Setup(t => t.AppendEntriesAsync(It.IsAny<string>(), It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new Exception("Transport error"));

        using CancellationTokenSource cts = new();
        cts.CancelAfter(150);

        // Act
        await _node.RunAsync(cts.Token);

        // Assert
        // Should log warning, not error, and continue
#pragma warning disable CA1873
        _mockLogger.Verify(
            x => x.Log(
                LogLevel.Warning,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("failed --")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.AtLeastOnce);
#pragma warning restore CA1873
    }

    [TestMethod]
    public async Task ProposeAsync_ShouldCommitImmediately_WhenSingleNode()
    {
        // Arrange
        using RaftNode singleNode = new("node1", new Dictionary<string, string>(), _mockTransport.Object, _mockLog.Object, _mockLogger.Object);
        singleNode._role = RaftRole.Leader;

        long lastIndex = 0;
        _mockLog.Setup(l => l.LastIndex).Returns(() => lastIndex);
        _mockLog.Setup(l => l.GetTermAtIndexAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(0L);
        _mockLog.Setup(l => l.AppendAsync(It.IsAny<IEnumerable<RaftLogEntry>>(), It.IsAny<CancellationToken>()))
            .Callback(() => lastIndex++)
            .Returns(Task.CompletedTask);

        // Capture commit
        List<RaftLogEntry> committed = [];
        singleNode.OnCommit += (e) => { committed.Add(e); return Task.CompletedTask; };
        _mockLog.Setup(l => l.GetEntryAsync(It.IsAny<long>(), It.IsAny<CancellationToken>())).ReturnsAsync((long i, CancellationToken ct) => new RaftLogEntry { Index = i, Term = 0 });

        // Act
        bool result = await singleNode.ProposeAsync(new byte[] { 1 }, TestContext.CancellationToken);

        // Assert
        Assert.IsTrue(result);
        Assert.HasCount(1, committed);
        Assert.AreEqual(1, committed[0].Index);
    }

    [TestMethod]
    public void OnLeaderChanged_ShouldRaiseEvent_WhenLeaderChanges()
    {
        // Arrange
        LeaderChangedEventArgs? eventArgs = null;
        _node.OnLeaderChanged += (sender, args) => eventArgs = args;

        // Act: Use reflection to call private SetLeader method
        MethodInfo? setLeaderMethod = typeof(RaftNode).GetMethod("SetLeader", BindingFlags.NonPublic | BindingFlags.Instance);
        setLeaderMethod!.Invoke(_node, ["newLeader"]);
        MethodInfo? flush = typeof(RaftNode).GetMethod("FlushLeaderChanged", BindingFlags.NonPublic | BindingFlags.Instance);
        flush!.Invoke(_node, null);

        // Assert
        Assert.IsNotNull(eventArgs);
        Assert.AreEqual("newLeader", eventArgs.LeaderId);
    }

    private static T GetPrivateField<T>(object instance, string name)
    {
        return (T)instance.GetType().GetField(name, PrivateInstanceFlags)!.GetValue(instance)!;
    }

    [TestMethod]
    public void Properties_AndGetUnhealthyPeers_ReturnExpectedValues()
    {
        _node._role = RaftRole.Leader;
        _node._commitIndex = 42;
        ConcurrentDictionary<string, DateTimeOffset> peerLastContact = GetPrivateField<ConcurrentDictionary<string, DateTimeOffset>>(_node, "_peerLastContact");
        peerLastContact["node2"] = DateTimeOffset.UtcNow - TimeSpan.FromMinutes(1);
        peerLastContact["node3"] = DateTimeOffset.UtcNow;

        string[] unhealthyPeers = [.. _node.GetUnhealthyPeers(TimeSpan.FromSeconds(30))];

        Assert.AreEqual("node1", _node.NodeId);
        Assert.AreEqual(RaftRole.Leader, _node.Role);
        Assert.AreEqual(42L, _node.CommitIndex);
        CollectionAssert.AreEqual(ExpectedUnhealthyPeer, unhealthyPeers);
    }

    [TestMethod]
    public async Task HandleJoinAsync_WhenAuthenticatedLeader_AddsReadonlyPeerAndReturnsPeers()
    {
        using RaftNode securedNode = new(
            "node1",
            _peers,
            _mockTransport.Object,
            _mockLog.Object,
            _mockLogger.Object,
            options: new RaftOptions { AuthToken = "secret", InitialValidationDelayMs = 0 });
        securedNode._role = RaftRole.Leader;

        JoinResponse response = await securedNode.HandleJoinAsync(
            new JoinRequest { NodeId = "node4", Address = "localhost:5004", AuthToken = "secret" },
            TestContext.CancellationToken);

        Assert.IsTrue(response.Success);
        Assert.AreEqual("node1", response.LeaderId);
        Assert.IsNotNull(response.Peers);
        Assert.AreEqual("localhost:5004", response.Peers["node4"]);
        Assert.AreEqual(_mockLog.Object.LastIndex + 1, securedNode._nextIndex["node4"]);
    }

    [TestMethod]
    public async Task HandleJoinAsync_WhenAuthTokenInvalid_ReturnsUnauthenticated()
    {
        using RaftNode securedNode = new(
            "node1",
            _peers,
            _mockTransport.Object,
            _mockLog.Object,
            _mockLogger.Object,
            options: new RaftOptions { AuthToken = "secret", InitialValidationDelayMs = 0 });

        JoinResponse response = await securedNode.HandleJoinAsync(
            new JoinRequest { NodeId = "node4", Address = "localhost:5004", AuthToken = "wrong" },
            TestContext.CancellationToken);

        Assert.IsFalse(response.Success);
        Assert.IsFalse(response.Authenticated);
    }

    [TestMethod]
    public async Task SendHeartbeatsAsync_WhenReadonlyPeerAndInFlightPeerExist_SkipsInFlightAndDispatchesOthers()
    {
        _mockLogger.Setup(logger => logger.IsEnabled(LogLevel.Debug)).Returns(true);
        _node._role = RaftRole.Leader;
        _node._currentTerm = 3;
        ConcurrentDictionary<string, string> readonlyPeers = GetPrivateField<ConcurrentDictionary<string, string>>(_node, "_readonlyPeers");
        ConcurrentDictionary<string, Task> inFlightHeartbeats = GetPrivateField<ConcurrentDictionary<string, Task>>(_node, "_inFlightHeartbeats");
        readonlyPeers["node4"] = "localhost:5004";
        inFlightHeartbeats["node2"] = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously).Task;
        _node._nextIndex["node3"] = 1;
        _node._nextIndex["node4"] = 1;
        _mockLog.Setup(log => log.LastIndex).Returns(0);
        _mockLog.Setup(log => log.GetEntriesAsync(It.IsAny<long>(), It.IsAny<int>(), It.IsAny<CancellationToken>())).Returns(AsyncEnumerable.Empty<RaftLogEntry>());
        _mockLog.Setup(log => log.GetTermAtIndexAsync(It.IsAny<long>(), It.IsAny<CancellationToken>())).ReturnsAsync(0);
        _mockTransport.Setup(transport => transport.AppendEntriesAsync(It.IsAny<string>(), It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new AppendEntriesResponse { Term = 3, Success = true });

        await _node.SendHeartbeatsAsync(CancellationToken.None);

        _mockTransport.Verify(transport => transport.AppendEntriesAsync("node2", It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()), Times.Never);
        _mockTransport.Verify(transport => transport.AppendEntriesAsync("node3", It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()), Times.Once);
        _mockTransport.Verify(transport => transport.AppendEntriesAsync("node4", It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()), Times.Once);
#pragma warning disable CA1873
        _mockLogger.Verify(
            logger => logger.Log(
                LogLevel.Debug,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((state, _) => state.ToString()!.Contains("Heartbeat dispatch round", StringComparison.Ordinal)),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
#pragma warning restore CA1873
    }

    [TestMethod]
    public void DispatchHeartbeats_Continuation_LogsWarningForFaultedTask()
    {
        _mockLogger.Setup(logger => logger.IsEnabled(LogLevel.Warning)).Returns(true);
        // Find the ContinueWith lambda inside DispatchHeartbeats by name prefix rather than
        // a fixed compiler-generated index, which can shift when other lambdas are added/removed.
        MethodInfo? continuation = typeof(RaftNode)
            .GetMethods(PrivateInstanceFlags)
            .FirstOrDefault(m => m.Name.StartsWith("<DispatchHeartbeats>b__", StringComparison.Ordinal));
        Assert.IsNotNull(continuation, "Could not find DispatchHeartbeats continuation lambda via reflection.");
        Task faultedTask = Task.FromException(new InvalidOperationException("dispatch failed"));

        continuation.Invoke(_node, [faultedTask]);

#pragma warning disable CA1873
        _mockLogger.Verify(
            logger => logger.Log(
                LogLevel.Warning,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((state, _) => state.ToString()!.Contains("Heartbeat dispatch task faulted", StringComparison.Ordinal)),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
#pragma warning restore CA1873
    }

    [TestMethod]
    public async Task HandleAppendEntriesAsync_WhenAuthTokenInvalid_ReturnsUnauthenticated()
    {
        using RaftNode securedNode = new(
            "node1",
            _peers,
            _mockTransport.Object,
            _mockLog.Object,
            _mockLogger.Object,
            options: new RaftOptions { AuthToken = "secret", InitialValidationDelayMs = 0 });

        AppendEntriesResponse response = await securedNode.HandleAppendEntriesAsync(
            new AppendEntriesRequest { Term = 1, LeaderId = "node2", PrevLogIndex = 0, PrevLogTerm = 0, Entries = [], LeaderCommit = 0, AuthToken = "wrong" },
            TestContext.CancellationToken);

        Assert.IsFalse(response.Success);
        Assert.IsFalse(response.Authenticated);
    }

    [TestMethod]
    public void ProtocolRecords_ShouldCoverAllProperties()
    {
        // Test VoteResponse with Authenticated = false
        VoteResponse voteResponse = new() { Term = 1, VoteGranted = true, Authenticated = false };
        Assert.AreEqual(1, voteResponse.Term);
        Assert.IsTrue(voteResponse.VoteGranted);
        Assert.IsFalse(voteResponse.Authenticated);

        // Test AppendEntriesResponse with Authenticated = false
        AppendEntriesResponse appendResponse = new() { Term = 1, Success = true, Authenticated = false };
        Assert.AreEqual(1, appendResponse.Term);
        Assert.IsTrue(appendResponse.Success);
        Assert.IsFalse(appendResponse.Authenticated);

        // Test ProposeResponse with LeaderId set
        ProposeResponse proposeResponse = new() { Success = true, LeaderId = "leader" };
        Assert.IsTrue(proposeResponse.Success);
        Assert.AreEqual("leader", proposeResponse.LeaderId);

        // Test JoinResponse with Peers
        JoinResponse joinResponse = new() { Success = true, Peers = new Dictionary<string, string> { { "node1", "addr1" } }, Authenticated = false };
        Assert.IsTrue(joinResponse.Success);
        Assert.IsNotNull(joinResponse.Peers);
        Assert.HasCount(1, joinResponse.Peers);
        Assert.IsFalse(joinResponse.Authenticated);

        // Test RaftLogEntry
        RaftLogEntry logEntry = new() { Term = 1, Index = 1, Command = new byte[] { 1, 2, 3 } };
        Assert.AreEqual(1, logEntry.Term);
        Assert.AreEqual(1, logEntry.Index);
        Assert.AreEqual(3, logEntry.Command.Length);
    }

    // ------- Additional branch coverage tests -------

    [TestMethod]
    public async Task HandleRequestVoteAsync_WhenTermIsZero_ReturnsFalseWithoutStateChange()
    {
        // Term == 0 is a connection probe; node must not change state
        _node._currentTerm = 3;
        _node._role = RaftRole.Follower;

        VoteResponse response = await _node.HandleRequestVoteAsync(
            new VoteRequest { Term = 0, CandidateId = "node2", LastLogIndex = 0, LastLogTerm = 0 },
            TestContext.CancellationToken);

        Assert.IsFalse(response.VoteGranted);
        Assert.AreEqual(3, response.Term);
        Assert.AreEqual(RaftRole.Follower, _node._role); // Unchanged
        Assert.AreEqual(3, _node._currentTerm);           // Unchanged
    }

    [TestMethod]
    public async Task HandleRequestVoteAsync_WhenAuthTokenInvalid_ReturnsUnauthenticated()
    {
        using RaftNode secured = new(
            "node1", _peers, _mockTransport.Object, _mockLog.Object, _mockLogger.Object,
            options: new RaftOptions { AuthToken = "secret", InitialValidationDelayMs = 0 });

        VoteResponse response = await secured.HandleRequestVoteAsync(
            new VoteRequest { Term = 2, CandidateId = "node2", LastLogIndex = 0, LastLogTerm = 0, AuthToken = "wrong" },
            TestContext.CancellationToken);

        Assert.IsFalse(response.VoteGranted);
        Assert.IsFalse(response.Authenticated);
    }

    [TestMethod]
    public async Task HandleJoinAsync_WhenNotLeader_ReturnsFalseWithCurrentLeaderId()
    {
        _node._role = RaftRole.Follower;
        _node.LeaderId = "node2";

        JoinResponse response = await _node.HandleJoinAsync(
            new JoinRequest { NodeId = "node5", Address = "addr5" },
            TestContext.CancellationToken);

        Assert.IsFalse(response.Success);
        Assert.AreEqual("node2", response.LeaderId);
    }

    [TestMethod]
    public async Task ProposeAsync_WhenFollowerAndNoLeader_ReturnsFalse()
    {
        _node._role = RaftRole.Follower;
        _node.LeaderId = null;

        bool result = await _node.ProposeAsync(new byte[] { 1 }, TestContext.CancellationToken);

        Assert.IsFalse(result);
    }

    [TestMethod]
    public async Task HandleProposeAsync_WhenNotLeader_ReturnsFalseWithLeaderId()
    {
        _node._role = RaftRole.Follower;
        _node.LeaderId = "node2";

        ProposeResponse response = await _node.HandleProposeAsync(
            new ProposeRequest { Command = new byte[] { 1 } },
            TestContext.CancellationToken);

        Assert.IsFalse(response.Success);
        Assert.AreEqual("node2", response.LeaderId);
    }

    [TestMethod]
    public async Task HandleAppendEntriesAsync_WhenCandidateReceivesSameTerm_StepsDownToFollower()
    {
        _node._role = RaftRole.Candidate;
        _node._currentTerm = 3;

        AppendEntriesResponse response = await _node.HandleAppendEntriesAsync(
            new AppendEntriesRequest
            {
                Term = 3,
                LeaderId = "node2",
                PrevLogIndex = 0,
                PrevLogTerm = 0,
                Entries = [],
                LeaderCommit = 0
            },
            TestContext.CancellationToken);

        Assert.IsTrue(response.Success);
        Assert.AreEqual(RaftRole.Follower, _node._role);
        Assert.AreEqual("node2", _node.LeaderId);
    }

    [TestMethod]
    public async Task HandleAppendEntriesAsync_WhenPrevLogIndexIsZero_SkipsPrevLogCheck()
    {
        // PrevLogIndex == 0 means no previous entries; should accept immediately
        _node._role = RaftRole.Follower;
        _node._currentTerm = 1;

        AppendEntriesResponse response = await _node.HandleAppendEntriesAsync(
            new AppendEntriesRequest
            {
                Term = 1,
                LeaderId = "node2",
                PrevLogIndex = 0,
                PrevLogTerm = 0,
                Entries = [],
                LeaderCommit = 0
            },
            TestContext.CancellationToken);

        Assert.IsTrue(response.Success);
    }

    [TestMethod]
    public async Task HandleAppendEntriesAsync_WhenPrevLogIndexExceedsLog_ReturnsFalse()
    {
        _mockLog.Setup(l => l.LastIndex).Returns(2);
        _node._currentTerm = 1;

        AppendEntriesResponse response = await _node.HandleAppendEntriesAsync(
            new AppendEntriesRequest
            {
                Term = 1,
                LeaderId = "node2",
                PrevLogIndex = 10, // Beyond log end
                PrevLogTerm = 1,
                Entries = [],
                LeaderCommit = 0
            },
            TestContext.CancellationToken);

        Assert.IsFalse(response.Success);
    }

    [TestMethod]
    public async Task HandleAppendEntriesAsync_WhenPrevLogTermMismatch_ReturnsFalse()
    {
        _mockLog.Setup(l => l.LastIndex).Returns(5);
        _mockLog.Setup(l => l.GetTermAtIndexAsync(3, It.IsAny<CancellationToken>())).ReturnsAsync(1L);
        _node._currentTerm = 2;

        AppendEntriesResponse response = await _node.HandleAppendEntriesAsync(
            new AppendEntriesRequest
            {
                Term = 2,
                LeaderId = "node2",
                PrevLogIndex = 3,
                PrevLogTerm = 99, // Mismatched
                Entries = [],
                LeaderCommit = 0
            },
            TestContext.CancellationToken);

        Assert.IsFalse(response.Success);
    }

    [TestMethod]
    public async Task HandleAppendEntriesAsync_WhenHigherTermResponse_StepsDownFromLeader()
    {
        // Set up leader node that receives a heartbeat response implying higher term
        // This tests the branch in SendAppendEntriesToPeerAsync where response.Term > currentTerm
        _node._role = RaftRole.Leader;
        _node._currentTerm = 2;
        _node._nextIndex["node2"] = 1;
        _node._nextIndex["node3"] = 1;

        _mockLog.Setup(l => l.GetEntriesAsync(It.IsAny<long>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .Returns(AsyncEnumerable.Empty<RaftLogEntry>());
        _mockLog.Setup(l => l.GetTermAtIndexAsync(It.IsAny<long>(), It.IsAny<CancellationToken>())).ReturnsAsync(0L);

        // Peer returns a higher term (causes step-down)
        _mockTransport.Setup(t => t.AppendEntriesAsync("node2", It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new AppendEntriesResponse { Term = 5, Success = false, Authenticated = true });
        _mockTransport.Setup(t => t.AppendEntriesAsync("node3", It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new AppendEntriesResponse { Term = 1, Success = true, Authenticated = true });

        await _node.SendHeartbeatsAsync(CancellationToken.None);

        Assert.AreEqual(RaftRole.Follower, _node._role);
        Assert.AreEqual(5L, _node._currentTerm);
    }

    [TestMethod]
    public async Task ApplyEntriesAsync_WhenOnCommitIsNull_AdvancesLastApplied()
    {
        // OnCommit has no subscribers: entries are applied but no callback fires
        _mockLog.Setup(l => l.GetEntryAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((long idx, CancellationToken _) => new RaftLogEntry { Index = idx, Term = 1 });

        await _node.ApplyEntriesAsync(1, 3, CancellationToken.None);

        Assert.AreEqual(3L, _node._lastApplied);
    }

    [TestMethod]
    public async Task ApplyEntriesAsync_WhenEntryIsNull_DoesNotAdvanceLastApplied()
    {
        _mockLog.Setup(l => l.GetEntryAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((RaftLogEntry?)null);

        List<RaftLogEntry> committed = [];
        _node.OnCommit += e => { committed.Add(e); return Task.CompletedTask; };

        await _node.ApplyEntriesAsync(1, 3, CancellationToken.None);

        Assert.IsEmpty(committed);
        Assert.AreEqual(0L, _node._lastApplied);
    }

    [TestMethod]
    public async Task ApplyEntriesAsync_SkipsAlreadyAppliedEntries()
    {
        _node._lastApplied = 5; // Already applied up to 5
        List<RaftLogEntry> committed = [];
        _node.OnCommit += e => { committed.Add(e); return Task.CompletedTask; };
        _mockLog.Setup(l => l.GetEntryAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((long idx, CancellationToken _) => new RaftLogEntry { Index = idx, Term = 1 });

        // Ask to apply 3-7; only 6 and 7 are new
        await _node.ApplyEntriesAsync(3, 7, CancellationToken.None);

        Assert.HasCount(2, committed);
        Assert.AreEqual(6L, committed[0].Index);
        Assert.AreEqual(7L, committed[1].Index);
        Assert.AreEqual(7L, _node._lastApplied);
    }

    [TestMethod]
    public void IsReady_ReturnsFalse_WhenLeaderIdIsNull()
    {
        _node.LeaderId = null;

        Assert.IsFalse(_node.IsReady);
    }

    [TestMethod]
    public void IsReady_ReturnsTrue_WhenLeaderIdIsSet()
    {
        _node.LeaderId = "node2";

        Assert.IsTrue(_node.IsReady);
    }

    [TestMethod]
    public async Task RunElectionAsync_WhenAlreadyElectionStarted_ReturnsImmediately()
    {
        _node._role = RaftRole.Candidate;
        _node._currentTerm = 1;
        // Set private _electionStarted via reflection
        typeof(RaftNode).GetField("_electionStarted", PrivateInstanceFlags)!.SetValue(_node, true);

        await _node.RunElectionAsync(CancellationToken.None);

        // Transport should NOT have been called
        _mockTransport.Verify(t => t.RequestVoteAsync(It.IsAny<string>(), It.IsAny<VoteRequest>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [TestMethod]
    public async Task RunElectionAsync_WhenNotCandidate_ReturnsImmediately()
    {
        _node._role = RaftRole.Follower; // Not a candidate

        await _node.RunElectionAsync(CancellationToken.None);

        _mockTransport.Verify(t => t.RequestVoteAsync(It.IsAny<string>(), It.IsAny<VoteRequest>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [TestMethod]
    public async Task RunElectionAsync_WhenUnauthenticatedResponseReceived_IgnoresResponse()
    {
        _node._role = RaftRole.Candidate;
        _node._currentTerm = 1;

        // Peers return unauthenticated (Authenticated = false)
        _mockTransport.Setup(t => t.RequestVoteAsync(It.IsAny<string>(), It.IsAny<VoteRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new VoteResponse { Term = 1, VoteGranted = true, Authenticated = false });

        await _node.RunElectionAsync(CancellationToken.None);

        // Unauthenticated votes don't count; node should stay Candidate (lost election)
        Assert.AreEqual(RaftRole.Candidate, _node._role);
    }

    [TestMethod]
    public async Task HandleAppendEntriesAsync_WhenLeaderCommitEqualsCommitIndex_DoesNotApply()
    {
        // LeaderCommit == _commitIndex: no new commit, shouldApply stays false
        _node._commitIndex = 5;
        _node._currentTerm = 1;

        AppendEntriesResponse response = await _node.HandleAppendEntriesAsync(
            new AppendEntriesRequest
            {
                Term = 1,
                LeaderId = "node2",
                PrevLogIndex = 0,
                PrevLogTerm = 0,
                Entries = [],
                LeaderCommit = 5 // Same as commitIndex
            },
            TestContext.CancellationToken);

        Assert.IsTrue(response.Success);
        Assert.AreEqual(5L, _node._commitIndex); // Unchanged
    }

    [TestMethod]
    public async Task SendHeartbeatsAsync_WhenUnauthenticatedResponse_DoesNotUpdateNextIndex()
    {
        _node._role = RaftRole.Leader;
        _node._currentTerm = 2;
        _node._nextIndex["node2"] = 1;
        _node._nextIndex["node3"] = 1;

        _mockLog.Setup(l => l.GetEntriesAsync(It.IsAny<long>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .Returns((long _, int _, CancellationToken _) => AsAsync([new RaftLogEntry { Index = 1, Term = 2 }]));
        _mockLog.Setup(l => l.GetTermAtIndexAsync(It.IsAny<long>(), It.IsAny<CancellationToken>())).ReturnsAsync(0L);

        _mockTransport.Setup(t => t.AppendEntriesAsync(It.IsAny<string>(), It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new AppendEntriesResponse { Term = 2, Success = true, Authenticated = false });

        await _node.SendHeartbeatsAsync(CancellationToken.None);

        // nextIndex should be unchanged because response was not authenticated
        Assert.AreEqual(1L, _node._nextIndex["node2"]);
    }

    [TestMethod]
    public async Task HandleAppendEntriesAsync_WithEntries_NoConflict_AppendsNewEntries()
    {
        // Log has 2 entries; request appends entry 3 without conflict
        _mockLog.Setup(l => l.LastIndex).Returns(2);
        _mockLog.Setup(l => l.GetTermAtIndexAsync(2, It.IsAny<CancellationToken>())).ReturnsAsync(1L);
        _node._currentTerm = 1;

        bool appendCalled = false;
        _mockLog.Setup(l => l.AppendAsync(It.IsAny<IEnumerable<RaftLogEntry>>(), It.IsAny<CancellationToken>()))
            .Callback<IEnumerable<RaftLogEntry>, CancellationToken>((_, _) => appendCalled = true)
            .Returns(Task.CompletedTask);

        AppendEntriesResponse response = await _node.HandleAppendEntriesAsync(
            new AppendEntriesRequest
            {
                Term = 1,
                LeaderId = "node2",
                PrevLogIndex = 2,
                PrevLogTerm = 1,
                Entries = [new RaftLogEntry { Index = 3, Term = 1, Command = ReadOnlyMemory<byte>.Empty }],
                LeaderCommit = 0
            },
            TestContext.CancellationToken);

        Assert.IsTrue(response.Success);
        Assert.IsTrue(appendCalled);
    }

    [TestMethod]
    public async Task GetUnhealthyPeers_ReturnsEmpty_WhenNoPeerLastContactRecorded()
    {
        // No contact recorded for any peer -> TryGetValue returns false -> not in unhealthy list
        IEnumerable<string> unhealthy = _node.GetUnhealthyPeers(TimeSpan.FromSeconds(1));

        Assert.IsFalse(unhealthy.Any());
    }

    [TestMethod]
    public async Task HandleAppendEntriesAsync_LeaderCommitWithNewEntries_AdvancesCommitIndex()
    {
        // Test min(leaderCommit, lastNewEntryIndex) branch
        _node._currentTerm = 1;
        _node._commitIndex = 0;
        _mockLog.Setup(l => l.LastIndex).Returns(0);

        _mockLog.Setup(l => l.AppendAsync(It.IsAny<IEnumerable<RaftLogEntry>>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        List<RaftLogEntry> committed = [];
        _node.OnCommit += e => { committed.Add(e); return Task.CompletedTask; };
        _mockLog.Setup(l => l.GetEntryAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((long idx, CancellationToken _) => new RaftLogEntry { Index = idx, Term = 1 });

        // LeaderCommit=10 but we only have 1 new entry -> commitIndex becomes min(10, 0+1) = 1
        AppendEntriesResponse response = await _node.HandleAppendEntriesAsync(
            new AppendEntriesRequest
            {
                Term = 1,
                LeaderId = "node2",
                PrevLogIndex = 0,
                PrevLogTerm = 0,
                Entries = [new RaftLogEntry { Index = 1, Term = 1, Command = ReadOnlyMemory<byte>.Empty }],
                LeaderCommit = 10
            },
            TestContext.CancellationToken);

        Assert.IsTrue(response.Success);
        Assert.AreEqual(1L, _node._commitIndex);
        Assert.HasCount(1, committed);
    }

    private static async IAsyncEnumerable<T> AsAsync<T>(IEnumerable<T> items)
    {
        foreach (T item in items) yield return item;
        await Task.CompletedTask;
    }

    public TestContext TestContext { get; set; } = null!;
}
