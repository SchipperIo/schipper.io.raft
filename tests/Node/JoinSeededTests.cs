// IonStream License 1.0
// Copyright (c) 2026 SCHIPPER.IO LLC - All Rights Reserved
// Licensor: SCHIPPER.IO LLC - Contact: contact@schipper.io

using Microsoft.Extensions.Logging;
using Moq;
using Schipper.Io.Raft.Protocol;

namespace Schipper.Io.Raft.Tests.Node;

/// <summary>
/// Seeded join (<see cref="JoinRequest.LastLogIndex"/>), the below-FirstIndex replication guard,
/// and <see cref="RaftNode.GetSafeCompactionIndex"/>.
/// </summary>
[TestClass]
public class JoinSeededTests : IDisposable
{
    private readonly Mock<IRaftTransport> _mockTransport;
    private readonly Mock<IRaftLog> _mockLog;
    private readonly Mock<ILogger<RaftNode>> _mockLogger;
    private readonly Dictionary<string, string> _peers;
    private readonly RaftNode _node;

    public JoinSeededTests()
    {
        _mockTransport = new Mock<IRaftTransport>();
        _mockLog = new Mock<IRaftLog>();
        _mockLogger = new Mock<ILogger<RaftNode>>();
        _peers = new Dictionary<string, string> { { "node2", "localhost:5002" }, { "node3", "localhost:5003" } };

        _mockLog.Setup(l => l.LastIndex).Returns(0);
        _mockLog.Setup(l => l.LastTerm).Returns(0);
        _mockLog.Setup(l => l.GetTermAtIndexAsync(It.IsAny<long>(), It.IsAny<CancellationToken>())).ReturnsAsync(0);
        _mockLog.Setup(l => l.GetEntriesAsync(It.IsAny<long>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .Returns(AsyncEnumerable.Empty<RaftLogEntry>());

        _node = new RaftNode("node1", _peers, _mockTransport.Object, _mockLog.Object, _mockLogger.Object,
            options: new RaftOptions { InitialValidationDelayMs = 0 });
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

    private static async IAsyncEnumerable<T> AsAsync<T>(IEnumerable<T> items)
    {
        foreach (T item in items) yield return item;
        await Task.CompletedTask;
    }

    // ------------------------------------------------------------------ seeded join

    [TestMethod]
    public async Task HandleJoinAsync_WithLastLogIndex_ReplicatesOnlyTheTail()
    {
        _node._role = RaftRole.Leader;
        _node._currentTerm = 1;
        _mockLog.Setup(l => l.LastIndex).Returns(10);
        _mockLog.Setup(l => l.GetTermAtIndexAsync(It.IsAny<long>(), It.IsAny<CancellationToken>())).ReturnsAsync(1);
        _mockLog.Setup(l => l.GetEntriesAsync(8, It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .Returns((long _, int _, CancellationToken _) => AsAsync(new[]
            {
                new RaftLogEntry { Index = 8, Term = 1 },
                new RaftLogEntry { Index = 9, Term = 1 },
                new RaftLogEntry { Index = 10, Term = 1 }
            }));

        JoinResponse response = await _node.HandleJoinAsync(
            new JoinRequest { NodeId = "node4", Address = "localhost:5004", LastLogIndex = 7 },
            CancellationToken.None);

        Assert.IsTrue(response.Success);
        Assert.AreEqual(8L, _node._nextIndex["node4"]);

        Dictionary<string, AppendEntriesRequest> captured = [];
        _mockTransport.Setup(t => t.AppendEntriesAsync(It.IsAny<string>(), It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .Callback<string, AppendEntriesRequest, CancellationToken>((peer, request, _) => captured[peer] = request)
            .ReturnsAsync(new AppendEntriesResponse { Term = 1, Success = true });

        await _node.SendHeartbeatsAsync(CancellationToken.None);

        // The joining node receives only the tail: prev = 7, entries 8..10.
        Assert.IsTrue(captured.TryGetValue("node4", out AppendEntriesRequest? toJoiner));
        Assert.AreEqual(7L, toJoiner.PrevLogIndex);
        Assert.HasCount(3, toJoiner.Entries);
        Assert.AreEqual(8L, toJoiner.Entries[0].Index);
        Assert.AreEqual(10L, toJoiner.Entries[2].Index);
    }

    [TestMethod]
    public async Task HandleJoinAsync_DefaultLastLogIndex_KeepsFullReplay()
    {
        _node._role = RaftRole.Leader;
        _node._currentTerm = 1;
        _mockLog.Setup(l => l.LastIndex).Returns(10);
        _mockLog.Setup(l => l.GetTermAtIndexAsync(It.IsAny<long>(), It.IsAny<CancellationToken>())).ReturnsAsync(1);
        _mockLog.Setup(l => l.GetEntriesAsync(1, It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .Returns((long _, int _, CancellationToken _) => AsAsync(new[] { new RaftLogEntry { Index = 1, Term = 1 } }));

        JoinResponse response = await _node.HandleJoinAsync(
            new JoinRequest { NodeId = "node4", Address = "localhost:5004" },
            CancellationToken.None);

        Assert.IsTrue(response.Success);
        Assert.AreEqual(1L, _node._nextIndex["node4"]);

        AppendEntriesRequest? toJoiner = null;
        _mockTransport.Setup(t => t.AppendEntriesAsync("node4", It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .Callback<string, AppendEntriesRequest, CancellationToken>((_, request, _) => toJoiner = request)
            .ReturnsAsync(new AppendEntriesResponse { Term = 1, Success = true });
        _mockTransport.Setup(t => t.AppendEntriesAsync(It.IsIn("node2", "node3"), It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new AppendEntriesResponse { Term = 1, Success = true });

        await _node.SendHeartbeatsAsync(CancellationToken.None);

        Assert.IsNotNull(toJoiner);
        Assert.AreEqual(0L, toJoiner.PrevLogIndex); // full replay from index 1
        Assert.AreEqual(1L, toJoiner.Entries[0].Index);
    }

    // ------------------------------------------------------------------ below-FirstIndex guard

    [TestMethod]
    public async Task SendHeartbeats_PeerBelowFirstIndex_IsSkippedWithWarning()
    {
        _mockLogger.Setup(l => l.IsEnabled(LogLevel.Warning)).Returns(true);

        Mock<ICompactableRaftLog> compactableLog = new();
        compactableLog.Setup(l => l.FirstIndex).Returns(50);
        compactableLog.Setup(l => l.LastIndex).Returns(100);
        compactableLog.Setup(l => l.LastTerm).Returns(1);
        compactableLog.Setup(l => l.GetTermAtIndexAsync(It.IsAny<long>(), It.IsAny<CancellationToken>())).ReturnsAsync(1);
        compactableLog.Setup(l => l.GetEntriesAsync(It.IsAny<long>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .Returns(AsyncEnumerable.Empty<RaftLogEntry>());

        using RaftNode node = new("node1", _peers, _mockTransport.Object, compactableLog.Object, _mockLogger.Object,
            options: new RaftOptions { InitialValidationDelayMs = 0 });
        node._role = RaftRole.Leader;
        node._currentTerm = 1;
        node._nextIndex["node2"] = 10; // below FirstIndex 50 -> needs a seed transfer
        node._nextIndex["node3"] = 60; // fine

        _mockTransport.Setup(t => t.AppendEntriesAsync(It.IsAny<string>(), It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new AppendEntriesResponse { Term = 1, Success = true });

        await node.SendHeartbeatsAsync(CancellationToken.None);

        _mockTransport.Verify(t => t.AppendEntriesAsync("node2", It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()), Times.Never);
        _mockTransport.Verify(t => t.AppendEntriesAsync("node3", It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()), Times.Once);

#pragma warning disable CA1873
        _mockLogger.Verify(
            l => l.Log(
                LogLevel.Warning,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((state, _) => state.ToString()!.Contains("requires seed transfer", StringComparison.Ordinal)),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
#pragma warning restore CA1873

        // Subsequent rounds keep skipping but do not repeat the warning.
        await node.SendHeartbeatsAsync(CancellationToken.None);
        _mockTransport.Verify(t => t.AppendEntriesAsync("node2", It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()), Times.Never);
#pragma warning disable CA1873
        _mockLogger.Verify(
            l => l.Log(
                LogLevel.Warning,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((state, _) => state.ToString()!.Contains("requires seed transfer", StringComparison.Ordinal)),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
#pragma warning restore CA1873
    }

    [TestMethod]
    public async Task SendHeartbeats_PeerAtFirstIndex_IsServed()
    {
        Mock<ICompactableRaftLog> compactableLog = new();
        compactableLog.Setup(l => l.FirstIndex).Returns(50);
        compactableLog.Setup(l => l.LastIndex).Returns(100);
        compactableLog.Setup(l => l.LastTerm).Returns(1);
        compactableLog.Setup(l => l.GetTermAtIndexAsync(It.IsAny<long>(), It.IsAny<CancellationToken>())).ReturnsAsync(1);
        compactableLog.Setup(l => l.GetEntriesAsync(It.IsAny<long>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .Returns(AsyncEnumerable.Empty<RaftLogEntry>());

        using RaftNode node = new("node1", _peers, _mockTransport.Object, compactableLog.Object, _mockLogger.Object,
            options: new RaftOptions { InitialValidationDelayMs = 0 });
        node._role = RaftRole.Leader;
        node._currentTerm = 1;
        node._nextIndex["node2"] = 50; // exactly FirstIndex: prevTerm(49) is available from the segment header
        node._nextIndex["node3"] = 60;

        AppendEntriesRequest? toNode2 = null;
        _mockTransport.Setup(t => t.AppendEntriesAsync("node2", It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .Callback<string, AppendEntriesRequest, CancellationToken>((_, request, _) => toNode2 = request)
            .ReturnsAsync(new AppendEntriesResponse { Term = 1, Success = true });
        _mockTransport.Setup(t => t.AppendEntriesAsync("node3", It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new AppendEntriesResponse { Term = 1, Success = true });

        await node.SendHeartbeatsAsync(CancellationToken.None);

        Assert.IsNotNull(toNode2);
        Assert.AreEqual(49L, toNode2.PrevLogIndex);
    }

    // ------------------------------------------------------------------ GetSafeCompactionIndex

    [TestMethod]
    public void GetSafeCompactionIndex_Follower_ReturnsCommitIndex()
    {
        _node._role = RaftRole.Follower;
        _node._commitIndex = 42;

        Assert.AreEqual(42L, _node.GetSafeCompactionIndex());
    }

    [TestMethod]
    public void GetSafeCompactionIndex_Leader_ReturnsMinOfMatchIndexAndCommit()
    {
        _node._role = RaftRole.Leader;
        _node._commitIndex = 7;
        _node._matchIndex["node2"] = 5;
        _node._matchIndex["node3"] = 8;

        Assert.AreEqual(5L, _node.GetSafeCompactionIndex());
    }

    [TestMethod]
    public void GetSafeCompactionIndex_Leader_CommitIndexIsTheCeiling()
    {
        _node._role = RaftRole.Leader;
        _node._commitIndex = 4;
        _node._matchIndex["node2"] = 9;
        _node._matchIndex["node3"] = 9;

        Assert.AreEqual(4L, _node.GetSafeCompactionIndex());
    }

    [TestMethod]
    public void GetSafeCompactionIndex_Leader_PeerWithoutMatchIndexCountsAsZero()
    {
        _node._role = RaftRole.Leader;
        _node._commitIndex = 7;
        _node._matchIndex["node2"] = 5;
        // node3 has no matchIndex entry yet -> treated as 0

        Assert.AreEqual(0L, _node.GetSafeCompactionIndex());
    }

    [TestMethod]
    public async Task GetSafeCompactionIndex_Leader_IncludesReadonlyPeers()
    {
        _node._role = RaftRole.Leader;
        _node._commitIndex = 7;
        _node._matchIndex["node2"] = 6;
        _node._matchIndex["node3"] = 6;

        JoinResponse response = await _node.HandleJoinAsync(
            new JoinRequest { NodeId = "node4", Address = "localhost:5004", LastLogIndex = 2 },
            CancellationToken.None);
        Assert.IsTrue(response.Success);
        _node._matchIndex["node4"] = 2;

        Assert.AreEqual(2L, _node.GetSafeCompactionIndex());
    }

    [TestMethod]
    public void GetSafeCompactionIndex_LeaderWithNoPeers_ReturnsCommitIndex()
    {
        using RaftNode single = new("node1", new Dictionary<string, string>(), _mockTransport.Object, _mockLog.Object,
            _mockLogger.Object, options: new RaftOptions { InitialValidationDelayMs = 0 });
        single._role = RaftRole.Leader;
        single._commitIndex = 11;

        Assert.AreEqual(11L, single.GetSafeCompactionIndex());
    }
}
