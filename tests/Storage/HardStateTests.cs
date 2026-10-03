// IonStream License 1.0
// Copyright (c) 2026 SCHIPPER.IO LLC - All Rights Reserved
// Licensor: SCHIPPER.IO LLC - Contact: contact@schipper.io

using Microsoft.Extensions.Logging;
using Moq;
using Schipper.Io.Raft.Protocol;
using Schipper.Io.Raft.Storage;

namespace Schipper.Io.Raft.Tests.Storage;

/// <summary>
/// RaftNode + IRaftHardStateStore integration: term/votedFor survive a restart, so a restarted
/// node can never grant a second, conflicting vote in the same term.
/// </summary>
[TestClass]
public class HardStateTests
{
    private string _tempDir = string.Empty;
    private Mock<IRaftTransport> _mockTransport = null!;
    private Mock<ILogger<RaftNode>> _mockLogger = null!;
    private Dictionary<string, string> _peers = null!;

    [TestInitialize]
    public void Setup()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "HardStateTests_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
        _mockTransport = new Mock<IRaftTransport>();
        _mockLogger = new Mock<ILogger<RaftNode>>();
        _peers = new Dictionary<string, string> { { "node2", "localhost:5002" }, { "node3", "localhost:5003" } };
    }

    [TestCleanup]
    public void Cleanup()
    {
        if (Directory.Exists(_tempDir))
        {
            Directory.Delete(_tempDir, true);
        }
    }

    private static SegmentedRaftLogOptions LogOptions => new() { FsyncOnAppend = false };

    private RaftNode CreateNode(SegmentedRaftLog log) =>
        new("node1", _peers, _mockTransport.Object, log, _mockLogger.Object,
            options: new RaftOptions { InitialValidationDelayMs = 0 });

    [TestMethod]
    public async Task RestartedNode_DoesNotVoteTwice_InSameTerm()
    {
        // First lifetime: grant a vote to node2 in term 5.
        using (SegmentedRaftLog log = new(_tempDir, LogOptions))
        using (RaftNode node = CreateNode(log))
        {
            VoteResponse granted = await node.HandleRequestVoteAsync(
                new VoteRequest { Term = 5, CandidateId = "node2", LastLogIndex = 0, LastLogTerm = 0 },
                CancellationToken.None);
            Assert.IsTrue(granted.VoteGranted);
        }

        // Second lifetime: the restarted node loads term 5 / votedFor node2 from the log.
        using SegmentedRaftLog reopened = new(_tempDir, LogOptions);
        using RaftNode restarted = CreateNode(reopened);

        Assert.AreEqual(5L, restarted._currentTerm);
        Assert.AreEqual("node2", restarted._votedFor);

        // A different candidate asking in the same term must be rejected.
        VoteResponse conflicting = await restarted.HandleRequestVoteAsync(
            new VoteRequest { Term = 5, CandidateId = "node3", LastLogIndex = 0, LastLogTerm = 0 },
            CancellationToken.None);
        Assert.IsFalse(conflicting.VoteGranted);
        Assert.AreEqual(5L, conflicting.Term);

        // The original candidate may be re-granted (idempotent re-vote).
        VoteResponse sameCandidate = await restarted.HandleRequestVoteAsync(
            new VoteRequest { Term = 5, CandidateId = "node2", LastLogIndex = 0, LastLogTerm = 0 },
            CancellationToken.None);
        Assert.IsTrue(sameCandidate.VoteGranted);
    }

    [TestMethod]
    public async Task ElectionStart_PersistsTermAndSelfVote()
    {
        using (SegmentedRaftLog log = new(_tempDir, LogOptions))
        using (RaftNode node = CreateNode(log))
        {
            node._lastHeartbeat = DateTimeOffset.UtcNow.AddSeconds(-30);
            node._currentElectionTimeout = TimeSpan.FromMilliseconds(1);

            await node.CheckElectionTimeoutAsync(CancellationToken.None);

            Assert.AreEqual(RaftRole.Candidate, node._role);
            Assert.AreEqual(1L, node._currentTerm);
            Assert.AreEqual("node1", node._votedFor);
        }

        using SegmentedRaftLog reopened = new(_tempDir, LogOptions);
        (long term, string? votedFor) = reopened.LoadHardState();
        Assert.AreEqual(1L, term);
        Assert.AreEqual("node1", votedFor);
    }

    [TestMethod]
    public async Task HigherTermAppendEntries_PersistsSteppedDownTerm()
    {
        using (SegmentedRaftLog log = new(_tempDir, LogOptions))
        using (RaftNode node = CreateNode(log))
        {
            AppendEntriesResponse response = await node.HandleAppendEntriesAsync(
                new AppendEntriesRequest { Term = 7, LeaderId = "node2", PrevLogIndex = 0, PrevLogTerm = 0, Entries = [], LeaderCommit = 0 },
                CancellationToken.None);
            Assert.IsTrue(response.Success);
        }

        using SegmentedRaftLog reopened = new(_tempDir, LogOptions);
        (long term, string? votedFor) = reopened.LoadHardState();
        Assert.AreEqual(7L, term);
        Assert.IsNull(votedFor);

        using RaftNode restarted = CreateNode(reopened);
        Assert.AreEqual(7L, restarted._currentTerm);
    }

    [TestMethod]
    public async Task NodeWithPlainLog_BehavesAsBefore()
    {
        // A log without IRaftHardStateStore: node starts at term 0 and persists nothing.
        Mock<IRaftLog> mockLog = new();
        mockLog.Setup(l => l.LastIndex).Returns(0);
        mockLog.Setup(l => l.LastTerm).Returns(0);

        using RaftNode node = new("node1", _peers, _mockTransport.Object, mockLog.Object, _mockLogger.Object,
            options: new RaftOptions { InitialValidationDelayMs = 0 });

        Assert.AreEqual(0L, node._currentTerm);
        Assert.IsNull(node._votedFor);

        VoteResponse response = await node.HandleRequestVoteAsync(
            new VoteRequest { Term = 2, CandidateId = "node2", LastLogIndex = 0, LastLogTerm = 0 },
            CancellationToken.None);
        Assert.IsTrue(response.VoteGranted);
    }
}
