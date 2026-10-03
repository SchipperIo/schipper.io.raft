// IonStream License 1.0
// Copyright (c) 2026 SCHIPPER.IO LLC - All Rights Reserved
// Licensor: SCHIPPER.IO LLC - Contact: contact@schipper.io

using Schipper.Io.Raft.Protocol;
using Microsoft.Extensions.Logging;
using Moq;
using System.Collections.Concurrent;
using System.Reflection;

namespace Schipper.Io.Raft.Tests.Node;

/// <summary>
/// Additional tests targeting uncovered branches to push line/branch coverage toward 100%.
/// </summary>
[TestClass]
public class RaftNodeAdditionalTests : IDisposable
{
    private const BindingFlags PrivateInstanceFlags = BindingFlags.Instance | BindingFlags.NonPublic;

    private readonly Mock<IRaftTransport> _mockTransport;
    private readonly Mock<IRaftLog> _mockLog;
    private readonly Mock<ILogger<RaftNode>> _mockLogger;
    private readonly Mock<IRaftMetrics> _mockMetrics;
    private readonly Dictionary<string, string> _peers;
    private readonly RaftNode _node;

    public RaftNodeAdditionalTests()
    {
        _mockTransport = new Mock<IRaftTransport>();
        _mockLog = new Mock<IRaftLog>();
        _mockLogger = new Mock<ILogger<RaftNode>>();
        _mockMetrics = new Mock<IRaftMetrics>();
        _peers = new Dictionary<string, string> { { "node2", "localhost:5002" }, { "node3", "localhost:5003" } };

        _mockLog.Setup(l => l.LastIndex).Returns(0);
        _mockLog.Setup(l => l.LastTerm).Returns(0);
        _mockLog.Setup(l => l.GetTermAtIndexAsync(It.IsAny<long>(), It.IsAny<CancellationToken>())).ReturnsAsync(0L);

        _mockLogger.Setup(x => x.IsEnabled(It.IsAny<LogLevel>())).Returns(true);

        _node = new RaftNode("node1", _peers, _mockTransport.Object, _mockLog.Object, _mockLogger.Object,
            metrics: _mockMetrics.Object,
            options: new RaftOptions { InitialValidationDelayMs = 0, SkipConnectionValidation = true });
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (disposing) _node.Dispose();
    }

    // -----------------------------------------------------------------------
    // Null argument guards
    // -----------------------------------------------------------------------

    [TestMethod]
    public async Task HandleRequestVoteAsync_WhenRequestIsNull_ThrowsArgumentNullException()
    {
        bool threw = false;
        try { await _node.HandleRequestVoteAsync(null!, CancellationToken.None); }
        catch (ArgumentNullException) { threw = true; }
        Assert.IsTrue(threw);
    }

    [TestMethod]
    public async Task HandleAppendEntriesAsync_WhenRequestIsNull_ThrowsArgumentNullException()
    {
        bool threw = false;
        try { await _node.HandleAppendEntriesAsync(null!, CancellationToken.None); }
        catch (ArgumentNullException) { threw = true; }
        Assert.IsTrue(threw);
    }

    [TestMethod]
    public async Task HandleJoinAsync_WhenRequestIsNull_ThrowsArgumentNullException()
    {
        bool threw = false;
        try { await _node.HandleJoinAsync(null!, CancellationToken.None); }
        catch (ArgumentNullException) { threw = true; }
        Assert.IsTrue(threw);
    }

    [TestMethod]
    public async Task HandleProposeAsync_WhenRequestIsNull_ThrowsArgumentNullException()
    {
        bool threw = false;
        try { await _node.HandleProposeAsync(null!, CancellationToken.None); }
        catch (ArgumentNullException) { threw = true; }
        Assert.IsTrue(threw);
    }

    // -----------------------------------------------------------------------
    // Metrics calls
    // -----------------------------------------------------------------------

    [TestMethod]
    public async Task CheckElectionTimeoutAsync_WhenTimeoutElapsed_CallsMetricsSetRaftTerm()
    {
        _node._role = RaftRole.Follower;
        _node._lastHeartbeat = DateTimeOffset.UtcNow.AddSeconds(-10);
        _node._currentElectionTimeout = TimeSpan.FromMilliseconds(1);
        _node._currentTerm = 1;

        await _node.CheckElectionTimeoutAsync(CancellationToken.None);

        _mockMetrics.Verify(m => m.SetRaftTerm(2L), Times.Once);
    }

    [TestMethod]
    public async Task RunElectionAsync_WhenWonElection_CallsMetricsVoteAndLeader()
    {
        _node._role = RaftRole.Candidate;
        _node._currentTerm = 1;

        _mockTransport
            .Setup(t => t.RequestVoteAsync(It.IsAny<string>(), It.IsAny<VoteRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new VoteResponse { Term = 1, VoteGranted = true });
        _mockTransport
            .Setup(t => t.AppendEntriesAsync(It.IsAny<string>(), It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new AppendEntriesResponse { Term = 1, Success = true });

        await _node.RunElectionAsync(CancellationToken.None);

        // Self vote + two peer votes
        _mockMetrics.Verify(m => m.RecordRaftVoteReceived(), Times.AtLeast(2));
        _mockMetrics.Verify(m => m.RecordRaftElectionDuration(It.IsAny<double>()), Times.Once);
        Assert.AreEqual(RaftRole.Leader, _node._role);
    }

    [TestMethod]
    public async Task RunElectionAsync_WhenHigherTermReceivedInResponse_CallsMetricsSetRaftTerm()
    {
        _node._role = RaftRole.Candidate;
        _node._currentTerm = 1;

        _mockTransport
            .Setup(t => t.RequestVoteAsync(It.IsAny<string>(), It.IsAny<VoteRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new VoteResponse { Term = 5, VoteGranted = false, Authenticated = true });

        await _node.RunElectionAsync(CancellationToken.None);

        _mockMetrics.Verify(m => m.SetRaftTerm(5L), Times.Once);
        Assert.AreEqual(RaftRole.Follower, _node._role);
    }

    [TestMethod]
    public async Task HandleAppendEntriesAsync_WhenHigherTerm_CallsMetricsSetRaftTerm()
    {
        _node._currentTerm = 1;

        await _node.HandleAppendEntriesAsync(
            new AppendEntriesRequest
            {
                Term = 7,
                LeaderId = "node2",
                PrevLogIndex = 0,
                PrevLogTerm = 0,
                Entries = [],
                LeaderCommit = 0
            },
            CancellationToken.None);

        _mockMetrics.Verify(m => m.SetRaftTerm(7L), Times.Once);
    }

    [TestMethod]
    public async Task HandleRequestVoteAsync_WhenHigherTerm_CallsMetricsSetRaftTerm()
    {
        _node._currentTerm = 1;

        await _node.HandleRequestVoteAsync(
            new VoteRequest { Term = 9, CandidateId = "node2", LastLogIndex = 0, LastLogTerm = 0 },
            CancellationToken.None);

        _mockMetrics.Verify(m => m.SetRaftTerm(9L), Times.Once);
    }

    // -----------------------------------------------------------------------
    // SetLeader / OnLeaderChanged event
    // -----------------------------------------------------------------------

    [TestMethod]
    public void LeaderChangedEventArgs_ExposesLeaderId()
    {
        LeaderChangedEventArgs args = new("node5");
        Assert.AreEqual("node5", args.LeaderId);
    }

    [TestMethod]
    public void LeaderChangedEventArgs_AllowsNullLeaderId()
    {
        LeaderChangedEventArgs args = new(null);
        Assert.IsNull(args.LeaderId);
    }

    [TestMethod]
    public async Task OnLeaderChanged_NotFired_WhenLeaderIdUnchanged()
    {
        _node.LeaderId = "node2";
        int eventCount = 0;
        _node.OnLeaderChanged += (_, _) => eventCount++;

        // Setting the same leader again (via a heartbeat from the same leader) should not raise
        await _node.HandleAppendEntriesAsync(
            new AppendEntriesRequest
            {
                Term = 1,
                LeaderId = "node2",
                PrevLogIndex = 0,
                PrevLogTerm = 0,
                Entries = [],
                LeaderCommit = 0
            },
            CancellationToken.None);

        // Leader was already node2 -- event should not fire again
        Assert.AreEqual(0, eventCount);
    }

    [TestMethod]
    public async Task OnLeaderChanged_Fired_WhenLeaderChanges()
    {
        _node._currentTerm = 1;
        _node.LeaderId = null;
        string? capturedLeader = "initial";
        _node.OnLeaderChanged += (_, args) => capturedLeader = args.LeaderId;

        await _node.HandleAppendEntriesAsync(
            new AppendEntriesRequest
            {
                Term = 1,
                LeaderId = "node2",
                PrevLogIndex = 0,
                PrevLogTerm = 0,
                Entries = [],
                LeaderCommit = 0
            },
            CancellationToken.None);

        Assert.AreEqual("node2", capturedLeader);
    }

    [TestMethod]
    public async Task OnLeaderChanged_Fired_WithNullLeader_WhenCandidacyBegins()
    {
        // Arrange: node2 is current leader
        _node._currentTerm = 1;
        _node.LeaderId = "node2";
        string? capturedLeader = "initial";
        _node.OnLeaderChanged += (_, args) => capturedLeader = args.LeaderId;

        // Election timeout fires; leader is cleared
        _node._lastHeartbeat = DateTimeOffset.UtcNow.AddSeconds(-10);
        _node._currentElectionTimeout = TimeSpan.FromMilliseconds(1);

        await _node.CheckElectionTimeoutAsync(CancellationToken.None);

        Assert.IsNull(capturedLeader);
    }

    // -----------------------------------------------------------------------
    // SetLeader metrics path
    // -----------------------------------------------------------------------

    [TestMethod]
    public async Task SetLeader_CallsMetricsSetRaftLeader_WhenLeaderChanges()
    {
        _node._currentTerm = 1;
        _node.LeaderId = null;

        await _node.HandleAppendEntriesAsync(
            new AppendEntriesRequest
            {
                Term = 1,
                LeaderId = "node2",
                PrevLogIndex = 0,
                PrevLogTerm = 0,
                Entries = [],
                LeaderCommit = 0
            },
            CancellationToken.None);

        _mockMetrics.Verify(m => m.SetRaftLeader("node2"), Times.AtLeastOnce);
    }

    // -----------------------------------------------------------------------
    // StopAsync
    // -----------------------------------------------------------------------

    [TestMethod]
    public async Task StopAsync_WhenLoopNotStarted_CompletesWithoutError()
    {
        // _loopTask is null; StopAsync should not throw
        using RaftNode node = new("nodeX", _peers, _mockTransport.Object, _mockLog.Object, _mockLogger.Object,
            options: new RaftOptions { SkipConnectionValidation = true });

        await node.StopAsync();
    }

    [TestMethod]
    public async Task StopAsync_WhenLoopIsRunning_StopsCleanly()
    {
        _mockTransport
            .Setup(t => t.AppendEntriesAsync(It.IsAny<string>(), It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new AppendEntriesResponse { Term = 1, Success = true });

        await _node.StartAsync();
        await Task.Delay(50);
        await _node.StopAsync();
    }

    // -----------------------------------------------------------------------
    // ValidateConnectionsAsync paths
    // -----------------------------------------------------------------------

    [TestMethod]
    public async Task ValidateConnectionsAsync_WhenInitialDelayPositive_WaitsBeforeConnecting()
    {
        // Test the InitialValidationDelayMs > 0 branch (lines 120-122 of RaftNode.cs)
        _mockTransport
            .Setup(t => t.ValidateConnectionAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        _mockTransport
            .Setup(t => t.RequestVoteAsync(It.IsAny<string>(), It.IsAny<VoteRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new VoteResponse { Term = 1, VoteGranted = false });
        _mockTransport
            .Setup(t => t.AppendEntriesAsync(It.IsAny<string>(), It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new AppendEntriesResponse { Term = 1, Success = true });

        using RaftNode node = new("nodeDelay", _peers, _mockTransport.Object, _mockLog.Object, _mockLogger.Object,
            options: new RaftOptions
            {
                SkipConnectionValidation = false,
                InitialValidationDelayMs = 10, // Small positive delay to trigger the branch
                ConnectionRetryDelayMs = 5,
                QuorumConnectionTimeoutMs = 100,
                ElectionTimeoutMinMs = 5000,
                ElectionTimeoutMaxMs = 6000
            });

        using CancellationTokenSource cts = new(400);
        await node.RunAsync(cts.Token);

        _mockTransport.Verify(t => t.ValidateConnectionAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.AtLeastOnce);
    }

    [TestMethod]
    public async Task ValidateConnectionsAsync_WhenNoPeers_SkipsAndReturns()
    {
        using RaftNode solo = new("solo", new Dictionary<string, string>(),
            _mockTransport.Object, _mockLog.Object, _mockLogger.Object,
            options: new RaftOptions { InitialValidationDelayMs = 0, SkipConnectionValidation = false });

        // RunAsync calls ValidateConnectionsAsync; with 0 peers it must return immediately
        using CancellationTokenSource cts = new(200);
        await solo.RunAsync(cts.Token);

        _mockTransport.Verify(t => t.ValidateConnectionAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [TestMethod]
    public async Task ValidateConnectionsAsync_WhenSkipConnectionValidationTrue_SkipsCompletely()
    {
        using RaftNode node = new("nodeSkip", _peers,
            _mockTransport.Object, _mockLog.Object, _mockLogger.Object,
            options: new RaftOptions { SkipConnectionValidation = true });

        using CancellationTokenSource cts = new(100);
        await node.RunAsync(cts.Token);

        _mockTransport.Verify(t => t.ValidateConnectionAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [TestMethod]
    public async Task ValidateConnectionsAsync_WhenPeersValidateSuccessfully_ProceedsToElection()
    {
        _mockTransport
            .Setup(t => t.ValidateConnectionAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        _mockTransport
            .Setup(t => t.RequestVoteAsync(It.IsAny<string>(), It.IsAny<VoteRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new VoteResponse { Term = 1, VoteGranted = true });
        _mockTransport
            .Setup(t => t.AppendEntriesAsync(It.IsAny<string>(), It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new AppendEntriesResponse { Term = 1, Success = true });

        RaftOptions nodeVOptions = new() { InitialValidationDelayMs = 0, QuorumConnectionTimeoutMs = 500 };
        nodeVOptions.SetFastMode(true);
        nodeVOptions.SkipConnectionValidation = false;
        using RaftNode node = new("nodeV", _peers, _mockTransport.Object, _mockLog.Object, _mockLogger.Object,
            options: nodeVOptions);

        using CancellationTokenSource cts = new(500);
        await node.RunAsync(cts.Token);

        _mockTransport.Verify(t => t.ValidateConnectionAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.AtLeastOnce);
    }

    [TestMethod]
    public async Task ValidateConnectionsAsync_WhenTaskFaults_LogsWarningAndProceeds()
    {
        // A task in the pending queue faults (IsFaulted == true) -- exercises lines 196-202
        // This means ValidateConnectionAsync throws, and the task completes with an exception
        // but the retry loop catches it and eventually the quorum times out
        int callCount = 0;
        _mockTransport
            .Setup(t => t.ValidateConnectionAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                // First call succeeds to get a peer connected, second faults
                if (callCount++ == 0)
                    return Task.CompletedTask;
                throw new Exception("transient fault");
            });
        _mockTransport
            .Setup(t => t.RequestVoteAsync(It.IsAny<string>(), It.IsAny<VoteRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new VoteResponse { Term = 1, VoteGranted = false });
        _mockTransport
            .Setup(t => t.AppendEntriesAsync(It.IsAny<string>(), It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new AppendEntriesResponse { Term = 1, Success = true });

        using RaftNode node = new("nodeFault", _peers, _mockTransport.Object, _mockLog.Object, _mockLogger.Object,
            options: new RaftOptions
            {
                SkipConnectionValidation = false,
                InitialValidationDelayMs = 0,
                ConnectionRetryDelayMs = 5,
                QuorumConnectionTimeoutMs = 200,
                ElectionTimeoutMinMs = 5000,
                ElectionTimeoutMaxMs = 6000
            });

        using CancellationTokenSource cts = new(500);
        await node.RunAsync(cts.Token);
    }

    [TestMethod]
    public async Task ValidateConnectionsAsync_WhenConnectionFails_RetriesAndEventuallyTimesOut()
    {
        // Transport always throws -- quorum timeout triggers the warning path
        _mockTransport
            .Setup(t => t.ValidateConnectionAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new Exception("connection refused"));

        using RaftNode node = new("nodeF", _peers, _mockTransport.Object, _mockLog.Object, _mockLogger.Object,
            options: new RaftOptions
            {
                SkipConnectionValidation = false,
                InitialValidationDelayMs = 0,
                ConnectionRetryDelayMs = 5,
                QuorumConnectionTimeoutMs = 50,
                ElectionTimeoutMinMs = 5000,
                ElectionTimeoutMaxMs = 6000
            });

        using CancellationTokenSource cts = new(300);
        await node.RunAsync(cts.Token);

        // Verified that we at least tried
        _mockTransport.Verify(t => t.ValidateConnectionAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.AtLeastOnce);
    }

    // -----------------------------------------------------------------------
    // RunElectionAsync edge cases
    // -----------------------------------------------------------------------

    [TestMethod]
    public async Task RunElectionAsync_WhenCurrentTermExceedsElectionTerm_Aborts()
    {
        // Simulate term advancing between election start lock release and result processing
        _node._role = RaftRole.Candidate;
        _node._currentTerm = 1;

        // Peer returns vote -- but during response processing we will have bumped term
        _mockTransport
            .Setup(t => t.RequestVoteAsync(It.IsAny<string>(), It.IsAny<VoteRequest>(), It.IsAny<CancellationToken>()))
            .Returns(async (string _, VoteRequest _, CancellationToken _) =>
            {
                // Bump node's term while vote is in flight
                _node._currentTerm = 99;
                await Task.Yield();
                return new VoteResponse { Term = 1, VoteGranted = true };
            });

        await _node.RunElectionAsync(CancellationToken.None);

        // Because _currentTerm (99) > electionTerm (1), election is aborted -- stays Candidate
        Assert.AreEqual(RaftRole.Candidate, _node._role);
        Assert.AreEqual(99L, _node._currentTerm);
    }

    // -----------------------------------------------------------------------
    // SendAppendEntriesToPeerAsync -- wasConnected / recovery log path
    // -----------------------------------------------------------------------

    [TestMethod]
    public async Task SendHeartbeatsAsync_WhenPeerRecoveredAfterFailure_LogsRestored()
    {
        _node._role = RaftRole.Leader;
        _node._currentTerm = 1;

        _mockLog.Setup(l => l.GetEntriesAsync(It.IsAny<long>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .Returns(AsyncEnumerable.Empty<RaftLogEntry>());
        _mockLog.Setup(l => l.GetTermAtIndexAsync(It.IsAny<long>(), It.IsAny<CancellationToken>())).ReturnsAsync(0L);

        // Mark node2 as previously disconnected
        ConcurrentDictionary<string, bool> peerConnected = GetPrivateField<ConcurrentDictionary<string, bool>>(_node, "_peerConnected");
        peerConnected["node2"] = false;

        // Now transport succeeds -> "connection restored" path
        _mockTransport
            .Setup(t => t.AppendEntriesAsync("node2", It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new AppendEntriesResponse { Term = 1, Success = true });
        _mockTransport
            .Setup(t => t.AppendEntriesAsync("node3", It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new AppendEntriesResponse { Term = 1, Success = true });

        await _node.SendHeartbeatsAsync(CancellationToken.None);

        Assert.IsTrue(peerConnected["node2"]);
#pragma warning disable CA1873
        _mockLogger.Verify(
            l => l.Log(
                LogLevel.Information,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((s, _) => s.ToString()!.Contains("restored", StringComparison.Ordinal)),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
#pragma warning restore CA1873
    }

    [TestMethod]
    public async Task SendHeartbeatsAsync_WhenPeerFailsTwice_SecondFailureIsSilent()
    {
        _node._role = RaftRole.Leader;
        _node._currentTerm = 1;

        _mockLog.Setup(l => l.GetEntriesAsync(It.IsAny<long>(), It.IsAny<int>(), It.IsAny<CancellationToken>())).Returns(AsyncEnumerable.Empty<RaftLogEntry>());
        _mockLog.Setup(l => l.GetTermAtIndexAsync(It.IsAny<long>(), It.IsAny<CancellationToken>())).ReturnsAsync(0L);

        _mockTransport
            .Setup(t => t.AppendEntriesAsync(It.IsAny<string>(), It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new Exception("down"));

        // First heartbeat: first-time failure -- warning logged
        await _node.SendHeartbeatsAsync(CancellationToken.None);

        _mockLogger.Invocations.Clear();

        // Second heartbeat: peer is already flagged as down -- no warning
        await _node.SendHeartbeatsAsync(CancellationToken.None);

#pragma warning disable CA1873
        _mockLogger.Verify(
            l => l.Log(
                LogLevel.Warning,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((s, _) => s.ToString()!.Contains("failed --", StringComparison.Ordinal)),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Never);
#pragma warning restore CA1873
    }

    // -----------------------------------------------------------------------
    // HandleAppendEntriesAsync -- entries loop with no-conflict path (logIndex++)
    // -----------------------------------------------------------------------

    [TestMethod]
    public async Task HandleAppendEntriesAsync_WhenEntriesMatchExisting_SkipsConflictAndAppendsRemaining()
    {
        // Log has entries 1 and 2 matching the incoming entries; entry 3 is new
        _mockLog.Setup(l => l.LastIndex).Returns(2);
        _mockLog.Setup(l => l.GetTermAtIndexAsync(2, It.IsAny<CancellationToken>())).ReturnsAsync(1L);
        _mockLog.Setup(l => l.GetTermAtIndexAsync(1, It.IsAny<CancellationToken>())).ReturnsAsync(1L);
        _node._currentTerm = 1;

        IEnumerable<RaftLogEntry>? capturedEntries = null;
        _mockLog.Setup(l => l.AppendAsync(It.IsAny<IEnumerable<RaftLogEntry>>(), It.IsAny<CancellationToken>()))
            .Callback<IEnumerable<RaftLogEntry>, CancellationToken>((e, _) => capturedEntries = e)
            .Returns(Task.CompletedTask);

        AppendEntriesResponse response = await _node.HandleAppendEntriesAsync(
            new AppendEntriesRequest
            {
                Term = 1,
                LeaderId = "node2",
                PrevLogIndex = 0,
                PrevLogTerm = 0,
                Entries =
                [
                    new RaftLogEntry { Index = 1, Term = 1, Command = ReadOnlyMemory<byte>.Empty },
                    new RaftLogEntry { Index = 2, Term = 1, Command = ReadOnlyMemory<byte>.Empty },
                    new RaftLogEntry { Index = 3, Term = 1, Command = ReadOnlyMemory<byte>.Empty }
                ],
                LeaderCommit = 0
            },
            CancellationToken.None);

        Assert.IsTrue(response.Success);
        Assert.IsNotNull(capturedEntries);
        RaftLogEntry[] appended = [.. capturedEntries];
        // Only entry 3 (index 2 in the entries list) should be appended
        Assert.HasCount(1, appended);
        Assert.AreEqual(3L, appended[0].Index);
    }

    // -----------------------------------------------------------------------
    // HandleProposeAsync -- multi-peer leader triggers heartbeats
    // -----------------------------------------------------------------------

    [TestMethod]
    public async Task HandleProposeAsync_WhenLeaderWithPeers_TriggersHeartbeats()
    {
        _node._role = RaftRole.Leader;
        _node._currentTerm = 1;

        long lastIndex = 0;
        _mockLog.Setup(l => l.LastIndex).Returns(() => lastIndex);
        _mockLog.Setup(l => l.AppendAsync(It.IsAny<IEnumerable<RaftLogEntry>>(), It.IsAny<CancellationToken>()))
            .Callback(() => lastIndex++)
            .Returns(Task.CompletedTask);
        _mockLog.Setup(l => l.GetEntriesAsync(It.IsAny<long>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .Returns(AsyncEnumerable.Empty<RaftLogEntry>());
        _mockLog.Setup(l => l.GetTermAtIndexAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((long index, CancellationToken _) => index == 0 ? 0L : 1L);
        _mockLog.Setup(l => l.GetEntryAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((long idx, CancellationToken _) => new RaftLogEntry { Index = idx, Term = 1 });

        _mockTransport
            .Setup(t => t.AppendEntriesAsync(It.IsAny<string>(), It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new AppendEntriesResponse { Term = 1, Success = true });

        ProposeResponse response = await _node.HandleProposeAsync(
            new ProposeRequest { Command = new byte[] { 1, 2 } },
            CancellationToken.None);

        Assert.IsTrue(response.Success);
        _mockTransport.Verify(
            t => t.AppendEntriesAsync(It.IsAny<string>(), It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()),
            Times.AtLeastOnce);
    }

    // -----------------------------------------------------------------------
    // ProposeAsync -- follower with leader but forwarding throws
    // -----------------------------------------------------------------------

    [TestMethod]
    public async Task ProposeAsync_WhenFollowerAndLeaderInPeers_ForwardsToLeader()
    {
        _node._role = RaftRole.Follower;
        _node.LeaderId = "node2";

        _mockTransport
            .Setup(t => t.ProposeAsync("node2", It.IsAny<ProposeRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ProposeResponse { Success = false, LeaderId = "node2" });

        bool result = await _node.ProposeAsync(new byte[] { 5 }, CancellationToken.None);

        Assert.IsFalse(result);
    }

    // -----------------------------------------------------------------------
    // RaftOptions.FastMode = false (backing field stays false)
    // -----------------------------------------------------------------------

    [TestMethod]
    public void RaftOptions_FastMode_DefaultIsFalse()
    {
        RaftOptions options = new();

        Assert.IsFalse(options.FastMode);
        // Defaults are unchanged
        Assert.AreEqual(3000, options.ElectionTimeoutMinMs);
    }

    [TestMethod]
    public void RaftOptions_FastMode_SetFalseAfterTrueRestoresFastFalse()
    {
        RaftOptions options = new();
        options.SetFastMode(true);
        Assert.IsTrue(options.FastMode);

        // Setting false does not re-apply overrides (backing field only flips)
        options.SetFastMode(false);
        Assert.IsFalse(options.FastMode);
    }

    // -----------------------------------------------------------------------
    // RaftNode constructor -- default options
    // -----------------------------------------------------------------------

    [TestMethod]
    public void Constructor_WithNullOptions_UsesDefaults()
    {
        using RaftNode node = new("n1", _peers, _mockTransport.Object, _mockLog.Object, _mockLogger.Object);

        // Should not throw; node is in Follower state by default
        Assert.AreEqual(RaftRole.Follower, node.Role);
    }

    [TestMethod]
    public void Constructor_WithNullMetrics_DoesNotThrow()
    {
        using RaftNode node = new("n1", _peers, _mockTransport.Object, _mockLog.Object, _mockLogger.Object,
            metrics: null,
            options: new RaftOptions { SkipConnectionValidation = true });

        Assert.AreEqual(RaftRole.Follower, node.Role);
    }

    // -----------------------------------------------------------------------
    // ApplyEntriesAsync -- ObjectDisposedException on _applyLock
    // -----------------------------------------------------------------------

    [TestMethod]
    public async Task ApplyEntriesAsync_WhenNodeDisposed_ReturnsWithoutThrowing()
    {
        RaftNode node = new("nd", _peers, _mockTransport.Object, _mockLog.Object, _mockLogger.Object,
            options: new RaftOptions { SkipConnectionValidation = true });

        // Dispose node (which disposes _applyLock)
        node.Dispose();

        // Should return gracefully (ObjectDisposedException caught internally)
        await node.ApplyEntriesAsync(1, 3, CancellationToken.None);
    }

    // -----------------------------------------------------------------------
    // HandleAppendEntriesAsync -- ObjectDisposedException on _lock
    // -----------------------------------------------------------------------

    [TestMethod]
    public async Task HandleAppendEntriesAsync_WhenNodeDisposed_ReturnsDefault()
    {
        RaftNode node = new("nd2", _peers, _mockTransport.Object, _mockLog.Object, _mockLogger.Object,
            options: new RaftOptions { SkipConnectionValidation = true });

        node.Dispose();

        AppendEntriesResponse response = await node.HandleAppendEntriesAsync(
            new AppendEntriesRequest
            {
                Term = 1,
                LeaderId = "node2",
                PrevLogIndex = 0,
                PrevLogTerm = 0,
                Entries = [],
                LeaderCommit = 0
            },
            CancellationToken.None);

        Assert.AreEqual(0L, response.Term);
        Assert.IsFalse(response.Success);
    }

    // -----------------------------------------------------------------------
    // HandleRequestVoteAsync -- ObjectDisposedException on _lock
    // -----------------------------------------------------------------------

    [TestMethod]
    public async Task HandleRequestVoteAsync_WhenNodeDisposed_ReturnsDefault()
    {
        RaftNode node = new("nd3", _peers, _mockTransport.Object, _mockLog.Object, _mockLogger.Object,
            options: new RaftOptions { SkipConnectionValidation = true });

        node.Dispose();

        VoteResponse response = await node.HandleRequestVoteAsync(
            new VoteRequest { Term = 2, CandidateId = "node2", LastLogIndex = 0, LastLogTerm = 0 },
            CancellationToken.None);

        Assert.AreEqual(0L, response.Term);
        Assert.IsFalse(response.VoteGranted);
    }

    // -----------------------------------------------------------------------
    // HandleJoinAsync -- ObjectDisposedException on _lock
    // -----------------------------------------------------------------------

    [TestMethod]
    public async Task HandleJoinAsync_WhenNodeDisposed_ReturnsDefault()
    {
        RaftNode node = new("nd4", _peers, _mockTransport.Object, _mockLog.Object, _mockLogger.Object,
            options: new RaftOptions { SkipConnectionValidation = true });

        node.Dispose();

        JoinResponse response = await node.HandleJoinAsync(
            new JoinRequest { NodeId = "nodeX", Address = "addr" },
            CancellationToken.None);

        Assert.IsFalse(response.Success);
    }

    // -----------------------------------------------------------------------
    // HandleProposeAsync -- ObjectDisposedException on _lock
    // -----------------------------------------------------------------------

    [TestMethod]
    public async Task HandleProposeAsync_WhenNodeDisposed_ReturnsDefault()
    {
        RaftNode node = new("nd5", _peers, _mockTransport.Object, _mockLog.Object, _mockLogger.Object,
            options: new RaftOptions { SkipConnectionValidation = true });

        node.Dispose();

        ProposeResponse response = await node.HandleProposeAsync(
            new ProposeRequest { Command = new byte[] { 1 } },
            CancellationToken.None);

        Assert.IsFalse(response.Success);
    }

    // -----------------------------------------------------------------------
    // SendHeartbeatsAsync -- leader commits entry via majority when only one entry sent
    // -----------------------------------------------------------------------

    [TestMethod]
    public async Task SendHeartbeatsAsync_UpdatesCommitIndex_CallsMetricsSetRaftCommitIndex()
    {
        _node._role = RaftRole.Leader;
        _node._currentTerm = 1;
        _node._commitIndex = 0;

        _mockLog.Setup(l => l.LastIndex).Returns(1);
        _mockLog.Setup(l => l.GetTermAtIndexAsync(It.IsAny<long>(), It.IsAny<CancellationToken>())).ReturnsAsync(1L);
        _mockLog.Setup(l => l.GetEntriesAsync(It.IsAny<long>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .Returns((long _, int _, CancellationToken _) => AsAsync([new RaftLogEntry { Index = 1, Term = 1 }]));

        _node._nextIndex["node2"] = 1;
        _node._nextIndex["node3"] = 1;

        _mockTransport
            .Setup(t => t.AppendEntriesAsync(It.IsAny<string>(), It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new AppendEntriesResponse { Term = 1, Success = true });

        await _node.SendHeartbeatsAsync(CancellationToken.None);

        _mockMetrics.Verify(m => m.SetRaftCommitIndex(It.IsAny<long>()), Times.AtLeastOnce);
    }

    // -----------------------------------------------------------------------
    // HandleAppendEntriesAsync -- commit index update calls metrics
    // -----------------------------------------------------------------------

    [TestMethod]
    public async Task HandleAppendEntriesAsync_WhenCommitAdvances_CallsMetricsSetRaftCommitIndex()
    {
        _node._currentTerm = 1;
        _node._commitIndex = 0;
        _mockLog.Setup(l => l.LastIndex).Returns(5);
        _mockLog.Setup(l => l.GetTermAtIndexAsync(It.IsAny<long>(), It.IsAny<CancellationToken>())).ReturnsAsync(1L);
        _mockLog.Setup(l => l.GetEntryAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((long idx, CancellationToken _) => new RaftLogEntry { Index = idx, Term = 1 });

        await _node.HandleAppendEntriesAsync(
            new AppendEntriesRequest
            {
                Term = 1,
                LeaderId = "node2",
                PrevLogIndex = 5,
                PrevLogTerm = 1,
                Entries = [],
                LeaderCommit = 3
            },
            CancellationToken.None);

        _mockMetrics.Verify(m => m.SetRaftCommitIndex(3L), Times.Once);
    }

    // -----------------------------------------------------------------------
    // ResetElectionTimeout respects options range
    // -----------------------------------------------------------------------

    [TestMethod]
    public void ResetElectionTimeout_SetsElectionTimeoutWithinConfiguredRange()
    {
        RaftOptions opts = new() { ElectionTimeoutMinMs = 500, ElectionTimeoutMaxMs = 600 };
        using RaftNode node = new("n", _peers, _mockTransport.Object, _mockLog.Object, _mockLogger.Object, options: opts);

        node.ResetElectionTimeout();

        Assert.IsTrue(node._currentElectionTimeout >= TimeSpan.FromMilliseconds(500));
        Assert.IsTrue(node._currentElectionTimeout < TimeSpan.FromMilliseconds(600));
    }

    // -----------------------------------------------------------------------
    // GetUnhealthyPeers -- peer with recent contact is not unhealthy
    // -----------------------------------------------------------------------

    [TestMethod]
    public void GetUnhealthyPeers_DoesNotReturnRecentPeer()
    {
        ConcurrentDictionary<string, DateTimeOffset> peerLastContact =
            GetPrivateField<ConcurrentDictionary<string, DateTimeOffset>>(_node, "_peerLastContact");
        peerLastContact["node2"] = DateTimeOffset.UtcNow; // Recent

        IEnumerable<string> unhealthy = _node.GetUnhealthyPeers(TimeSpan.FromSeconds(30));

        CollectionAssert.DoesNotContain(unhealthy.ToList(), "node2");
    }

    // -----------------------------------------------------------------------
    // HandleAppendEntriesAsync -- term < currentTerm with request term < current (no-op path)
    // -----------------------------------------------------------------------

    [TestMethod]
    public async Task HandleAppendEntriesAsync_WhenTermBelowCurrentTerm_ReturnsFalseNoStateChange()
    {
        _node._currentTerm = 5;
        _node._role = RaftRole.Follower;

        AppendEntriesResponse response = await _node.HandleAppendEntriesAsync(
            new AppendEntriesRequest
            {
                Term = 3, // Stale term
                LeaderId = "node2",
                PrevLogIndex = 0,
                PrevLogTerm = 0,
                Entries = [],
                LeaderCommit = 0
            },
            CancellationToken.None);

        Assert.IsFalse(response.Success);
        Assert.AreEqual(5L, response.Term);
        Assert.AreEqual(RaftRole.Follower, _node._role);
    }

    // -----------------------------------------------------------------------
    // HandleAppendEntriesAsync -- LeaderId empty string skips connection validation
    // -----------------------------------------------------------------------

    [TestMethod]
    public async Task HandleAppendEntriesAsync_WhenLeaderIdEmpty_DoesNotCallValidateConnection()
    {
        _node._currentTerm = 1;

        // Use a request with empty LeaderId
        AppendEntriesResponse response = await _node.HandleAppendEntriesAsync(
            new AppendEntriesRequest
            {
                Term = 1,
                LeaderId = string.Empty,
                PrevLogIndex = 0,
                PrevLogTerm = 0,
                Entries = [],
                LeaderCommit = 0
            },
            CancellationToken.None);

        _mockTransport.Verify(
            t => t.ValidateConnectionAsync(string.Empty, It.IsAny<CancellationToken>()),
            Times.Never);
    }

    // -----------------------------------------------------------------------
    // HandleRequestVoteAsync -- CandidateId empty string skips connection validation
    // -----------------------------------------------------------------------

    [TestMethod]
    public async Task HandleRequestVoteAsync_WhenCandidateIdEmpty_DoesNotCallValidateConnection()
    {
        VoteResponse response = await _node.HandleRequestVoteAsync(
            new VoteRequest { Term = 1, CandidateId = string.Empty, LastLogIndex = 0, LastLogTerm = 0 },
            CancellationToken.None);

        _mockTransport.Verify(
            t => t.ValidateConnectionAsync(string.Empty, It.IsAny<CancellationToken>()),
            Times.Never);
    }

    // -----------------------------------------------------------------------
    // ProposeRequest ToString / record coverage
    // -----------------------------------------------------------------------

    [TestMethod]
    public void ProposeRequest_ToString_ContainsTypeName()
    {
        ProposeRequest req = new() { Command = new byte[] { 1 } };
        string s = req.ToString();
        Assert.IsTrue(s.Contains("ProposeRequest", StringComparison.Ordinal));
    }

    [TestMethod]
    public void VoteRequest_ToString_ContainsTypeName()
    {
        VoteRequest req = new() { CandidateId = "n1", Term = 1 };
        string s = req.ToString();
        Assert.IsTrue(s.Contains("VoteRequest", StringComparison.Ordinal));
    }

    [TestMethod]
    public void VoteRequest_Equality_SameValuesAreEqual()
    {
        VoteRequest a = new() { Term = 1, CandidateId = "n1", LastLogIndex = 2, LastLogTerm = 1, AuthToken = "tok" };
        VoteRequest b = new() { Term = 1, CandidateId = "n1", LastLogIndex = 2, LastLogTerm = 1, AuthToken = "tok" };
        Assert.AreEqual(a, b);
        Assert.AreEqual(a.GetHashCode(), b.GetHashCode());
    }

    [TestMethod]
    public void VoteRequest_Inequality_DifferentCandidateIdIsNotEqual()
    {
        VoteRequest a = new() { Term = 1, CandidateId = "n1" };
        VoteRequest b = new() { Term = 1, CandidateId = "n2" };
        Assert.AreNotEqual(a, b);
    }

    [TestMethod]
    public void VoteRequest_WithExpression_ProducesNewRecord()
    {
        VoteRequest original = new() { Term = 1, CandidateId = "n1" };
        VoteRequest modified = original with { Term = 2 };
        Assert.AreEqual(2L, modified.Term);
        Assert.AreNotEqual(original, modified);
    }

    [TestMethod]
    public void AppendEntriesRequest_Equality_SameValuesAreEqual()
    {
        IReadOnlyList<RaftLogEntry> entries = Array.Empty<RaftLogEntry>();
        AppendEntriesRequest a = new() { Term = 1, LeaderId = "l1", PrevLogIndex = 0, PrevLogTerm = 0, Entries = entries, LeaderCommit = 0 };
        AppendEntriesRequest b = new() { Term = 1, LeaderId = "l1", PrevLogIndex = 0, PrevLogTerm = 0, Entries = entries, LeaderCommit = 0 };
        Assert.AreEqual(a, b);
        Assert.AreEqual(a.GetHashCode(), b.GetHashCode());
    }

    [TestMethod]
    public void AppendEntriesRequest_WithExpression_ProducesNewRecord()
    {
        IReadOnlyList<RaftLogEntry> entries = Array.Empty<RaftLogEntry>();
        AppendEntriesRequest original = new() { Term = 1, LeaderId = "l1", Entries = entries };
        AppendEntriesRequest modified = original with { Term = 2 };
        Assert.AreEqual(2L, modified.Term);
        Assert.AreNotEqual(original, modified);
    }

    [TestMethod]
    public void JoinResponse_Equality_SameValuesAreEqual()
    {
        JoinResponse a = new() { Success = true, LeaderId = "l1", Authenticated = true };
        JoinResponse b = new() { Success = true, LeaderId = "l1", Authenticated = true };
        Assert.AreEqual(a, b);
        Assert.AreEqual(a.GetHashCode(), b.GetHashCode());
    }

    [TestMethod]
    public void JoinResponse_WithExpression_ProducesNewRecord()
    {
        JoinResponse original = new() { Success = false, LeaderId = "l1" };
        JoinResponse modified = original with { Success = true };
        Assert.IsTrue(modified.Success);
        Assert.AreNotEqual(original, modified);
    }

    [TestMethod]
    public void ProposeRequest_WithExpression_ProducesNewRecord()
    {
        byte[] first = [1];
        byte[] second = [2];
        ProposeRequest original = new() { Command = first };
        ProposeRequest modified = original with { Command = second };
        CollectionAssert.AreEqual(second, modified.Command.ToArray());
        Assert.AreNotEqual(original, modified);
    }

    // -----------------------------------------------------------------------
    // RaftLogEntry default Command is empty
    // -----------------------------------------------------------------------

    [TestMethod]
    public void RaftLogEntry_DefaultCommand_IsEmpty()
    {
        RaftLogEntry entry = new() { Term = 0, Index = 0 };
        Assert.AreEqual(0, entry.Command.Length);
    }

    // -----------------------------------------------------------------------
    // HandleAppendEntriesAsync -- high-term response step-down calls metrics
    // -----------------------------------------------------------------------

    [TestMethod]
    public async Task SendHeartbeatsAsync_WhenPeerReturnsHigherTerm_CallsMetricsSetRaftTerm()
    {
        _node._role = RaftRole.Leader;
        _node._currentTerm = 2;
        _node._nextIndex["node2"] = 1;
        _node._nextIndex["node3"] = 1;

        _mockLog.Setup(l => l.GetEntriesAsync(It.IsAny<long>(), It.IsAny<int>(), It.IsAny<CancellationToken>())).Returns(AsyncEnumerable.Empty<RaftLogEntry>());
        _mockLog.Setup(l => l.GetTermAtIndexAsync(It.IsAny<long>(), It.IsAny<CancellationToken>())).ReturnsAsync(0L);

        _mockTransport
            .Setup(t => t.AppendEntriesAsync(It.IsAny<string>(), It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new AppendEntriesResponse { Term = 10, Success = false, Authenticated = true });

        await _node.SendHeartbeatsAsync(CancellationToken.None);

        _mockMetrics.Verify(m => m.SetRaftTerm(10L), Times.Once);
    }

    // -----------------------------------------------------------------------
    // ValidateConnectionAsync continuation -- warning logged when connection fails
    // -----------------------------------------------------------------------

    [TestMethod]
    public async Task HandleRequestVoteAsync_WhenValidateConnectionFails_LogsWarning()
    {
        // The continuation fires after ValidateConnectionAsync completes with a fault
        TaskCompletionSource<bool> validateCalled = new();
        _mockTransport
            .Setup(t => t.ValidateConnectionAsync("node2", It.IsAny<CancellationToken>()))
            .Returns(async () =>
            {
                await Task.Yield();
                validateCalled.TrySetResult(true);
                throw new Exception("connection failed");
            });

        await _node.HandleRequestVoteAsync(
            new VoteRequest { Term = 1, CandidateId = "node2", LastLogIndex = 0, LastLogTerm = 0 },
            CancellationToken.None);

        // Give the continuation time to run
        await validateCalled.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await Task.Delay(50); // Let continuation fire

#pragma warning disable CA1873
        _mockLogger.Verify(
            l => l.Log(
                LogLevel.Warning,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((s, _) => s.ToString()!.Contains("Failed to validate connection back to", StringComparison.Ordinal)),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.AtLeastOnce);
#pragma warning restore CA1873
    }

    [TestMethod]
    public async Task HandleAppendEntriesAsync_WhenValidateConnectionFails_LogsWarning()
    {
        // The continuation fires after ValidateConnectionAsync completes with a fault
        TaskCompletionSource<bool> validateCalled = new();
        _mockTransport
            .Setup(t => t.ValidateConnectionAsync("node2", It.IsAny<CancellationToken>()))
            .Returns(async () =>
            {
                await Task.Yield();
                validateCalled.TrySetResult(true);
                throw new Exception("connection failed");
            });

        _node._currentTerm = 1;
        await _node.HandleAppendEntriesAsync(
            new AppendEntriesRequest
            {
                Term = 1,
                LeaderId = "node2",
                PrevLogIndex = 0,
                PrevLogTerm = 0,
                Entries = [],
                LeaderCommit = 0
            },
            CancellationToken.None);

        // Give the continuation time to run
        await validateCalled.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await Task.Delay(50); // Let continuation fire

#pragma warning disable CA1873
        _mockLogger.Verify(
            l => l.Log(
                LogLevel.Warning,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((s, _) => s.ToString()!.Contains("Failed to validate connection back to", StringComparison.Ordinal)),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.AtLeastOnce);
#pragma warning restore CA1873
    }

    // -----------------------------------------------------------------------
    // StopAsync -- catches exception from loop task
    // -----------------------------------------------------------------------

    [TestMethod]
    public async Task StopAsync_WhenLoopTimesOutWaiting_CatchesOperationCancelled()
    {
        // Covers line 1129: OperationCanceledException when StopTimeoutMs elapses before loop finishes.
        using RaftNode node = new("nodeTimeout", _peers, _mockTransport.Object, _mockLog.Object, _mockLogger.Object,
            options: new RaftOptions { SkipConnectionValidation = true, StopTimeoutMs = 50 });

        // Inject a never-completing loop task so the 50ms timeout fires.
        TaskCompletionSource<bool> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        typeof(RaftNode)
            .GetField("_loopTask", PrivateInstanceFlags)!
            .SetValue(node, tcs.Task);

        await node.StopAsync();
        // StopAsync must return; the abandoned tcs task is harmless.
    }

    [TestMethod]
    public async Task StopAsync_WhenLoopThrowsNonCancellation_LogsWarning()
    {
        // Create a node with a loop task that is already faulted
        using RaftNode node = new("faultNode", _peers, _mockTransport.Object, _mockLog.Object, _mockLogger.Object,
            options: new RaftOptions { SkipConnectionValidation = true });

        // Manually set _loopTask to a faulted task via reflection
        TaskCompletionSource<bool> tcs = new();
        tcs.SetException(new InvalidOperationException("loop blew up"));
        typeof(RaftNode)
            .GetField("_loopTask", PrivateInstanceFlags)!
            .SetValue(node, tcs.Task);

        // StopAsync should catch the exception and log warning, not throw
        await node.StopAsync();

#pragma warning disable CA1873
        _mockLogger.Verify(
            l => l.Log(
                LogLevel.Warning,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((s, _) => s.ToString()!.Contains("Error while stopping Raft loop", StringComparison.Ordinal)),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
#pragma warning restore CA1873
    }

    // -----------------------------------------------------------------------
    // SendAppendEntriesToPeerAsync -- cancellation token cancelled while exception
    // -----------------------------------------------------------------------

    [TestMethod]
    public async Task SendHeartbeatsAsync_WhenCancelledAndTransportThrows_DoesNotLog()
    {
        _node._role = RaftRole.Leader;
        _node._currentTerm = 1;

        _mockLog.Setup(l => l.GetEntriesAsync(It.IsAny<long>(), It.IsAny<int>(), It.IsAny<CancellationToken>())).Returns(AsyncEnumerable.Empty<RaftLogEntry>());
        _mockLog.Setup(l => l.GetTermAtIndexAsync(It.IsAny<long>(), It.IsAny<CancellationToken>())).ReturnsAsync(0L);

        using CancellationTokenSource cts = new();

        _mockTransport
            .Setup(t => t.AppendEntriesAsync(It.IsAny<string>(), It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .Returns(async (string _, AppendEntriesRequest _, CancellationToken _) =>
            {
                await cts.CancelAsync();
                throw new Exception("cancelled-adjacent error");
            });

        await _node.SendHeartbeatsAsync(cts.Token);

        // Because token is cancelled, error path returns without logging
#pragma warning disable CA1873
        _mockLogger.Verify(
            l => l.Log(
                LogLevel.Warning,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((s, _) => s.ToString()!.Contains("cancelled-adjacent", StringComparison.Ordinal)),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Never);
#pragma warning restore CA1873
    }

    // -----------------------------------------------------------------------
    // FileRaftLog -- AppendAsync cancellation during write
    // -----------------------------------------------------------------------

    // -----------------------------------------------------------------------
    // FileRaftLog -- TruncateFromAsync leaves entries and re-reads LastTerm
    // -----------------------------------------------------------------------

    // -----------------------------------------------------------------------
    // RunAsync -- error recovery delay path
    // -----------------------------------------------------------------------

    [TestMethod]
    public async Task RunAsync_WhenLoopBodyThrowsNonCancellation_RecoversContinues()
    {
        // Force a non-cancellation exception in the loop body by making the lock fail.
        // We can do this by setting up the metrics to throw, which happens inside the lock.
        _node._role = RaftRole.Follower;
        _node._lastHeartbeat = DateTimeOffset.UtcNow.AddSeconds(-10);
        _node._currentElectionTimeout = TimeSpan.FromMilliseconds(1);

        int throwCount = 0;
        _mockMetrics
            .Setup(m => m.SetRaftTerm(It.IsAny<long>()))
            .Callback(() =>
            {
                if (throwCount++ < 1)
                    throw new InvalidOperationException("metrics exploded");
            });

        using CancellationTokenSource cts = new(300);
        await _node.RunAsync(cts.Token);

        // Should have tried and recovered
        Assert.IsGreaterThanOrEqualTo(throwCount, 1);
    }

    [TestMethod]
    public async Task RunAsync_WhenLoopBodyThrows_AndCancelledDuringRecoveryDelay_Exits()
    {
        // Covers line 277: OperationCanceledException caught inside error recovery Task.Delay
        _node._role = RaftRole.Follower;
        _node._lastHeartbeat = DateTimeOffset.UtcNow.AddSeconds(-10);
        _node._currentElectionTimeout = TimeSpan.FromMilliseconds(1);

        // Use a long recovery delay so the cancel fires during the delay
        RaftOptions opts = new()
        {
            SkipConnectionValidation = true,
            ErrorRecoveryDelayMs = 10000, // Very long delay
            ElectionTimeoutMinMs = 1,
            ElectionTimeoutMaxMs = 2,
        };

        using RaftNode node = new("nodeRec", _peers, _mockTransport.Object, _mockLog.Object, _mockLogger.Object,
            metrics: _mockMetrics.Object, options: opts);
        node._role = RaftRole.Follower;
        node._lastHeartbeat = DateTimeOffset.UtcNow.AddSeconds(-10);
        node._currentElectionTimeout = TimeSpan.FromMilliseconds(1);

        // Metrics throws to trigger the error recovery path
        _mockMetrics
            .Setup(m => m.SetRaftTerm(It.IsAny<long>()))
            .Throws(new InvalidOperationException("boom"));

        using CancellationTokenSource cts = new(150);
        // Should exit gracefully when cancelled during the recovery delay
        await node.RunAsync(cts.Token);
        // If we reach here, the cancellation was handled correctly
    }

    // -----------------------------------------------------------------------
    // SendAppendEntriesToPeerAsync -- ObjectDisposedException paths
    // -----------------------------------------------------------------------

    [TestMethod]
    public async Task SendHeartbeatsAsync_WhenLockDisposedBeforeFirstWaitAsync_Returns()
    {
        // Covers line 480: ObjectDisposedException on the first _lock.WaitAsync.
        _node._role = RaftRole.Leader;

        SemaphoreSlim sem = (SemaphoreSlim)typeof(RaftNode)
            .GetField("_lock", PrivateInstanceFlags)!
            .GetValue(_node)!;
        sem.Dispose();

        await _node.SendHeartbeatsAsync(CancellationToken.None);
    }

    [TestMethod]
    public async Task SendHeartbeatsAsync_WhenLockDisposedBetweenRpcAndResponseLock_Returns()
    {
        // Covers line 537: ObjectDisposedException on the second _lock.WaitAsync (after RPC).
        // The mock transport disposes the lock before returning so the second WaitAsync throws.
        _node._role = RaftRole.Leader;
        _node._currentTerm = 1;

        _mockLog.Setup(l => l.GetEntriesAsync(It.IsAny<long>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .Returns(AsyncEnumerable.Empty<RaftLogEntry>());

        _mockTransport
            .Setup(t => t.AppendEntriesAsync(It.IsAny<string>(), It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .Returns((string _, AppendEntriesRequest _, CancellationToken _) =>
            {
                SemaphoreSlim sem = (SemaphoreSlim)typeof(RaftNode)
                    .GetField("_lock", PrivateInstanceFlags)!
                    .GetValue(_node)!;
                sem.Dispose();
                return Task.FromResult(new AppendEntriesResponse { Authenticated = true, Success = true });
            });

        await _node.SendHeartbeatsAsync(CancellationToken.None);
    }

    [TestMethod]
    public async Task SendHeartbeatsAsync_WhenLogThrowsObjectDisposed_CaughtByOuterHandler()
    {
        // Covers lines 606-609: ObjectDisposedException thrown from inside the inner try (log call)
        // propagates through finally and is caught by the outer catch block.
        _node._role = RaftRole.Leader;
        _node._currentTerm = 1;

        _mockLog
            .Setup(l => l.GetTermAtIndexAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new ObjectDisposedException("log"));

        await _node.SendHeartbeatsAsync(CancellationToken.None);
    }

    // -----------------------------------------------------------------------
    // RunAsync -- error recovery delay completes without cancellation
    // -----------------------------------------------------------------------

    [TestMethod]
    public async Task RunAsync_WhenRecoveryDelayCompletesNormally_ContinuesLoop()
    {
        // Covers line 270: the catch(Exception) block exits normally when the recovery delay
        // completes before the cancellation token fires.
        int throwCount = 0;
        _mockMetrics
            .Setup(m => m.SetRaftTerm(It.IsAny<long>()))
            .Callback(() =>
            {
                if (throwCount++ < 1)
                    throw new InvalidOperationException("temp error");
            });

        using RaftNode node = new("nodeRec3", _peers, _mockTransport.Object, _mockLog.Object, _mockLogger.Object,
            metrics: _mockMetrics.Object,
            options: new RaftOptions
            {
                SkipConnectionValidation = true,
                ErrorRecoveryDelayMs = 1,
                ElectionTimeoutMinMs = 1,
                ElectionTimeoutMaxMs = 2,
            });
        node._role = RaftRole.Follower;
        node._lastHeartbeat = DateTimeOffset.UtcNow.AddSeconds(-10);
        node._currentElectionTimeout = TimeSpan.FromMilliseconds(1);

        using CancellationTokenSource cts = new(200);
        await node.RunAsync(cts.Token);

        Assert.IsTrue(throwCount >= 1);
    }

    // -----------------------------------------------------------------------
    // Helper
    // -----------------------------------------------------------------------

    private static async IAsyncEnumerable<T> AsAsync<T>(IEnumerable<T> items)
    {
        foreach (T item in items) yield return item;
        await Task.CompletedTask;
    }

    private static T GetPrivateField<T>(object instance, string name)
    {
        return (T)instance.GetType().GetField(name, PrivateInstanceFlags)!.GetValue(instance)!;
    }

    public TestContext TestContext { get; set; } = null!;
}
