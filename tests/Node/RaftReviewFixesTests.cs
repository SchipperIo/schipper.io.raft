// IonStream License 1.0
// Copyright (c) 2026 SCHIPPER.IO LLC - All Rights Reserved
// Licensor: SCHIPPER.IO LLC - Contact: contact@schipper.io

using Microsoft.Extensions.Logging;
using Moq;
using Schipper.Io.Raft.Protocol;

namespace Schipper.Io.Raft.Tests.Node;

[TestClass]
public class RaftReviewFixesTests
{
    private static RaftNode CreateNode(
        IDictionary<string, string> peers,
        Mock<IRaftTransport> transport,
        Mock<IRaftLog> log,
        IRaftClock? clock = null,
        Func<int, int, int>? draw = null)
    {
        Mock<ILogger<RaftNode>> logger = new();
        return new RaftNode(
            "node1",
            peers,
            transport.Object,
            log.Object,
            logger.Object,
            options: new RaftOptions { InitialValidationDelayMs = 0, SkipConnectionValidation = true },
            clock: clock,
            drawElectionTimeoutMs: draw);
    }

    private static Mock<IRaftLog> CreateLog(long lastIndex, long lastTerm)
    {
        Mock<IRaftLog> log = new();
        long index = lastIndex;
        log.Setup(l => l.LastIndex).Returns(() => index);
        log.Setup(l => l.LastTerm).Returns(lastTerm);
        log.Setup(l => l.GetTermAtIndexAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((long i, CancellationToken _) => i == 0 ? 0L : lastTerm);
        log.Setup(l => l.AppendAsync(It.IsAny<IEnumerable<RaftLogEntry>>(), It.IsAny<CancellationToken>()))
            .Callback<IEnumerable<RaftLogEntry>, CancellationToken>((entries, _) =>
            {
                foreach (RaftLogEntry entry in entries)
                    index = entry.Index;
            })
            .Returns(Task.CompletedTask);
        log.Setup(l => l.GetEntriesAsync(It.IsAny<long>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .Returns((long start, int _, CancellationToken _) =>
                start <= index
                    ? AsAsync([new RaftLogEntry { Index = index, Term = lastTerm }])
                    : AsyncEnumerable.Empty<RaftLogEntry>());
        log.Setup(l => l.GetEntryAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((long i, CancellationToken _) => new RaftLogEntry { Index = i, Term = lastTerm });
        return log;
    }

    [TestMethod]
    public async Task TwoVoters_CommitAdvances_WhenBackupMatchIsZero()
    {
        Mock<IRaftTransport> transport = new();
        transport.Setup(t => t.AppendEntriesAsync(It.IsAny<string>(), It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new IOException("backup down"));
        Mock<IRaftLog> log = CreateLog(0, 1);
        using RaftNode node = CreateNode(new Dictionary<string, string> { ["node2"] = "a" }, transport, log);
        node._role = RaftRole.Leader;
        node._currentTerm = 1;

        ProposeResponse response = await node.HandleProposeAsync(new ProposeRequest { Command = new byte[] { 1 } }, CancellationToken.None);

        Assert.IsTrue(response.Success);
        Assert.AreEqual(1L, node.CommitIndex);
        Assert.AreEqual(0L, node._matchIndex.GetValueOrDefault("node2"));
    }

    [TestMethod]
    public async Task FourVoters_DoesNotCommit_WhenOnlyLeaderAndOneFollowerHaveN()
    {
        Mock<IRaftTransport> transport = new();
        transport.Setup(t => t.AppendEntriesAsync("node2", It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new AppendEntriesResponse { Term = 1, Success = true });
        transport.Setup(t => t.AppendEntriesAsync(It.Is<string>(id => id != "node2"), It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new AppendEntriesResponse { Term = 1, Success = false });
        Mock<IRaftLog> log = CreateLog(5, 1);
        Dictionary<string, string> peers = new()
        {
            ["node2"] = "a",
            ["node3"] = "b",
            ["node4"] = "c",
        };
        using RaftNode node = CreateNode(peers, transport, log);
        node._role = RaftRole.Leader;
        node._currentTerm = 1;
        node._nextIndex["node2"] = 5;
        node._nextIndex["node3"] = 5;
        node._nextIndex["node4"] = 5;

        await node.SendHeartbeatsAsync(CancellationToken.None);

        Assert.AreEqual(0L, node.CommitIndex);
    }

    [TestMethod]
    public async Task LearnerMatchDoesNotCountTowardVotingCommit()
    {
        Mock<IRaftTransport> transport = new();
        transport.Setup(t => t.AppendEntriesAsync("learner", It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new AppendEntriesResponse { Term = 1, Success = true });
        transport.Setup(t => t.AppendEntriesAsync(It.Is<string>(id => id != "learner"), It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new AppendEntriesResponse { Term = 1, Success = false });
        Mock<IRaftLog> log = CreateLog(7, 1);
        using RaftNode node = CreateNode(
            new Dictionary<string, string> { ["node2"] = "a", ["node3"] = "b" },
            transport,
            log);
        node._role = RaftRole.Leader;
        node._currentTerm = 1;
        node._nextIndex["node2"] = 7;
        node._nextIndex["node3"] = 7;

        JoinResponse joined = await node.HandleJoinAsync(new JoinRequest { NodeId = "learner", Address = "c", LastLogIndex = 6 }, CancellationToken.None);
        Assert.IsTrue(joined.Success);

        await node.SendHeartbeatsAsync(CancellationToken.None);

        Assert.AreEqual(0L, node.CommitIndex);
        Assert.AreEqual(7L, node._matchIndex["learner"]);
    }

    [TestMethod]
    public async Task EmptyHeartbeat_UpdatesMatchIndex_AndSafeCompaction()
    {
        Mock<IRaftTransport> transport = new();
        transport.Setup(t => t.AppendEntriesAsync(It.IsAny<string>(), It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new AppendEntriesResponse { Term = 1, Success = true });
        Mock<IRaftLog> log = CreateLog(4, 1);
        using RaftNode node = CreateNode(
            new Dictionary<string, string> { ["node2"] = "a", ["node3"] = "b" },
            transport,
            log);
        node._role = RaftRole.Leader;
        node._currentTerm = 1;
        node._commitIndex = 4;
        node._nextIndex["node2"] = 5;
        node._nextIndex["node3"] = 5;
        node._matchIndex["node2"] = 0;
        node._matchIndex["node3"] = 0;

        await node.SendHeartbeatsAsync(CancellationToken.None);

        Assert.AreEqual(4L, node._matchIndex["node2"]);
        Assert.AreEqual(4L, node._matchIndex["node3"]);
        Assert.AreEqual(4L, node.GetSafeCompactionIndex());
    }

    [TestMethod]
    public async Task Propose_ReturnsFalse_WhenMajorityDoesNotAck()
    {
        Mock<IRaftTransport> transport = new();
        transport.Setup(t => t.AppendEntriesAsync(It.IsAny<string>(), It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new AppendEntriesResponse { Term = 1, Success = false });
        Mock<IRaftLog> log = CreateLog(0, 1);
        using RaftNode node = CreateNode(
            new Dictionary<string, string> { ["node2"] = "a", ["node3"] = "b" },
            transport,
            log);
        node._role = RaftRole.Leader;
        node._currentTerm = 1;

        ProposeResponse response = await node.HandleProposeAsync(new ProposeRequest { Command = new byte[] { 9 } }, CancellationToken.None);

        Assert.IsFalse(response.Success);
        Assert.AreEqual(0L, node.CommitIndex);
    }

    [TestMethod]
    public async Task Propose_ReturnsTrue_AfterOnCommitSeesTheIndex()
    {
        Mock<IRaftTransport> transport = new();
        transport.Setup(t => t.AppendEntriesAsync(It.IsAny<string>(), It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new AppendEntriesResponse { Term = 1, Success = true });
        Mock<IRaftLog> log = CreateLog(0, 1);
        using RaftNode node = CreateNode(
            new Dictionary<string, string> { ["node2"] = "a", ["node3"] = "b" },
            transport,
            log);
        node._role = RaftRole.Leader;
        node._currentTerm = 1;
        long seen = 0;
        node.OnCommit += e =>
        {
            seen = e.Index;
            return Task.CompletedTask;
        };

        ProposeResponse response = await node.HandleProposeAsync(new ProposeRequest { Command = new byte[] { 9 } }, CancellationToken.None);

        Assert.IsTrue(response.Success);
        Assert.AreEqual(1L, seen);
        Assert.AreEqual(1L, node.CommitIndex);
    }

    [TestMethod]
    public async Task StaleAppendEntriesSuccess_DoesNotRewriteMatchFromPreviousTerm()
    {
        TaskCompletionSource<AppendEntriesResponse> pending = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int appendGeneration = 1;
        Mock<IRaftTransport> transport = new();
        transport.Setup(t => t.AppendEntriesAsync(It.IsAny<string>(), It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .Returns(() => appendGeneration == 1
                ? pending.Task
                : Task.FromResult(new AppendEntriesResponse { Term = 2, Success = true }));
        transport.Setup(t => t.RequestVoteAsync(It.IsAny<string>(), It.IsAny<VoteRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new VoteResponse { Term = 2, VoteGranted = true });
        Mock<IRaftLog> log = CreateLog(3, 2);
        using RaftNode node = CreateNode(
            new Dictionary<string, string> { ["node2"] = "a", ["node3"] = "b" },
            transport,
            log);
        node._role = RaftRole.Leader;
        node._currentTerm = 1;
        node._nextIndex["node2"] = 4;
        node._nextIndex["node3"] = 4;

        Task heartbeats = node.SendHeartbeatsAsync(CancellationToken.None);
        await node.HandleAppendEntriesAsync(
            new AppendEntriesRequest { Term = 2, LeaderId = "node2", PrevLogIndex = 0, PrevLogTerm = 0, Entries = [], LeaderCommit = 0 },
            CancellationToken.None);
        appendGeneration = 2;
        node._role = RaftRole.Candidate;
        node._electionStarted = false;
        node._currentTerm = 2;
        await node.RunElectionAsync(CancellationToken.None);

        Assert.AreEqual(RaftRole.Leader, node._role);
        Assert.AreEqual(0L, node._matchIndex["node2"]);
        long nextAfterWin = node._nextIndex["node2"];

        pending.SetResult(new AppendEntriesResponse { Term = 1, Success = true });
        await heartbeats;

        Assert.AreEqual(0L, node._matchIndex["node2"]);
        Assert.AreEqual(nextAfterWin, node._nextIndex["node2"]);
    }

    [TestMethod]
    public async Task SameTermOlderSuccess_DoesNotDecreaseMatchIndex()
    {
        Mock<IRaftTransport> transport = new();
        TaskCompletionSource<AppendEntriesResponse> slow = new(TaskCreationOptions.RunContinuationsAsynchronously);
        transport.Setup(t => t.AppendEntriesAsync("node2", It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .Returns(slow.Task);
        transport.Setup(t => t.AppendEntriesAsync("node3", It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new AppendEntriesResponse { Term = 1, Success = true });
        Mock<IRaftLog> log = CreateLog(10, 1);
        log.Setup(l => l.GetEntriesAsync(1, It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .Returns(AsAsync([
                new RaftLogEntry { Index = 1, Term = 1 },
                new RaftLogEntry { Index = 2, Term = 1 },
                new RaftLogEntry { Index = 3, Term = 1 },
            ]));
        using RaftNode node = CreateNode(
            new Dictionary<string, string> { ["node2"] = "a", ["node3"] = "b" },
            transport,
            log);
        node._role = RaftRole.Leader;
        node._currentTerm = 1;
        node._nextIndex["node2"] = 1;
        node._nextIndex["node3"] = 11;
        node._matchIndex["node2"] = 10;

        Task heartbeats = node.SendHeartbeatsAsync(CancellationToken.None);
        for (int i = 0; i < 50 && node._matchIndex.GetValueOrDefault("node3") != 10L; i++)
            await Task.Delay(10);

        slow.SetResult(new AppendEntriesResponse { Term = 1, Success = true });
        await heartbeats;

        Assert.AreEqual(10L, node._matchIndex["node2"]);
        Assert.IsGreaterThanOrEqualTo(11L, node._nextIndex["node2"]);
    }

    [TestMethod]
    public async Task ApplyEntries_RetriesIndexAfterOnCommitThrows()
    {
        Mock<IRaftTransport> transport = new();
        Mock<IRaftLog> log = CreateLog(3, 1);
        using RaftNode node = CreateNode(new Dictionary<string, string>(), transport, log);
        int callsForTwo = 0;
        node.OnCommit += e =>
        {
            if (e.Index == 2)
            {
                callsForTwo++;
                if (callsForTwo == 1)
                    throw new InvalidOperationException("fail 2");
            }
            return Task.CompletedTask;
        };

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => node.ApplyEntriesAsync(1, 3, CancellationToken.None));
        Assert.AreEqual(1L, node._lastApplied);

        await node.ApplyEntriesAsync(1, 3, CancellationToken.None);
        Assert.AreEqual(2, callsForTwo);
        Assert.AreEqual(3L, node._lastApplied);
    }

    [TestMethod]
    public async Task FakeClock_DoesNotCampaignUntilAdvancedPastTimeout()
    {
        FrozenClock clock = new() { UtcNow = new DateTimeOffset(2020, 1, 1, 0, 0, 0, TimeSpan.Zero) };
        int draws = 0;
        int Draw(int min, int max)
        {
            draws++;
            Assert.AreEqual(3000, min);
            Assert.AreEqual(6000, max);
            return 50;
        }

        Mock<IRaftTransport> transport = new();
        Mock<IRaftLog> log = CreateLog(0, 0);
        using RaftNode node = CreateNode(
            new Dictionary<string, string> { ["node2"] = "a" },
            transport,
            log,
            clock,
            Draw);

        Assert.AreEqual(1, draws);
        Assert.AreEqual(0L, node._currentTerm);

        await node.CheckElectionTimeoutAsync(CancellationToken.None);
        Assert.AreEqual(0L, node._currentTerm);

        clock.UtcNow = clock.UtcNow.AddMilliseconds(51);
        await node.CheckElectionTimeoutAsync(CancellationToken.None);

        Assert.AreEqual(1L, node._currentTerm);
        Assert.AreEqual(RaftRole.Candidate, node._role);
        Assert.AreEqual("node1", node._votedFor);
        Assert.AreEqual(2, draws);
    }

    [TestMethod]
    public async Task OnLeaderChanged_RunsWithoutNodeLock()
    {
        Mock<IRaftTransport> transport = new();
        transport.Setup(t => t.ProposeAsync(It.IsAny<string>(), It.IsAny<ProposeRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ProposeResponse { Success = false, LeaderId = "node2" });
        Mock<IRaftLog> log = CreateLog(0, 1);
        using RaftNode node = CreateNode(new Dictionary<string, string> { ["node2"] = "a" }, transport, log);
        bool ran = false;
        node.OnLeaderChanged += (_, _) =>
        {
            _ = node.GetSafeCompactionIndex();
            node.ProposeAsync(new byte[] { 1 }, CancellationToken.None).GetAwaiter().GetResult();
            ran = true;
        };

        AppendEntriesResponse response = await node.HandleAppendEntriesAsync(
            new AppendEntriesRequest { Term = 1, LeaderId = "node2", PrevLogIndex = 0, PrevLogTerm = 0, Entries = [], LeaderCommit = 0 },
            CancellationToken.None);

        Assert.IsTrue(response.Success);
        Assert.IsTrue(ran);
        Assert.AreEqual("node2", node.LeaderId);
    }

    [TestMethod]
    public async Task WinningElection_ResetsReadonlyPeerCursors()
    {
        Mock<IRaftTransport> transport = new();
        transport.Setup(t => t.RequestVoteAsync(It.IsAny<string>(), It.IsAny<VoteRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new VoteResponse { Term = 1, VoteGranted = true });
        TaskCompletionSource<AppendEntriesResponse> hanging = new(TaskCreationOptions.RunContinuationsAsynchronously);
        transport.Setup(t => t.AppendEntriesAsync(It.IsAny<string>(), It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .Returns(hanging.Task);
        Mock<IRaftLog> log = CreateLog(8, 1);
        using RaftNode node = CreateNode(
            new Dictionary<string, string> { ["node2"] = "a", ["node3"] = "b" },
            transport,
            log);
        node._role = RaftRole.Leader;
        node._currentTerm = 1;
        await node.HandleJoinAsync(new JoinRequest { NodeId = "learner", Address = "c", LastLogIndex = 8 }, CancellationToken.None);
        node._matchIndex["learner"] = 8;
        node._nextIndex["learner"] = 9;

        node._role = RaftRole.Candidate;
        node._electionStarted = false;
        Task election = node.RunElectionAsync(CancellationToken.None);
        for (int i = 0; i < 50 && node._role != RaftRole.Leader; i++)
            await Task.Delay(10);

        Assert.AreEqual(RaftRole.Leader, node._role);
        Assert.AreEqual(0L, node._matchIndex["learner"]);
        Assert.AreEqual(9L, node._nextIndex["learner"]);
        Assert.AreEqual(0L, node._matchIndex["node2"]);
        Assert.AreEqual(9L, node._nextIndex["node2"]);

        hanging.SetResult(new AppendEntriesResponse { Term = 1, Success = true });
        await election;
    }

    private static async IAsyncEnumerable<T> AsAsync<T>(IEnumerable<T> items)
    {
        foreach (T item in items)
            yield return item;
        await Task.CompletedTask;
    }

    private sealed class FrozenClock : IRaftClock
    {
        public DateTimeOffset UtcNow { get; set; }

        public Task Delay(TimeSpan delay, CancellationToken cancellationToken = default) => Task.CompletedTask;
    }
}
