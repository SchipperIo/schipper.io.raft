// IonStream License 1.0
// Copyright (c) 2026 SCHIPPER.IO LLC - All Rights Reserved
// Licensor: SCHIPPER.IO LLC - Contact: contact@schipper.io

using Schipper.Io.Raft.Protocol;
using Schipper.Io.Raft.Storage;

namespace Schipper.Io.Raft.Tests.Storage;

[TestClass]
public class FileRaftLogTests
{
    private string _tempDir = string.Empty;

    private static readonly RaftLogEntry[] SampleEntries =
    [
        new() { Term = 1, Index = 1, Command = new byte[] { 1 } },
        new() { Term = 2, Index = 2, Command = new byte[] { 2, 3 } },
        new() { Term = 3, Index = 3, Command = new byte[] { 4, 5, 6 } }
    ];

    [TestInitialize]
    public void Setup()
    {
        // Use a unique GUID-based path per test instance for parallel safety
        _tempDir = Path.Combine(Path.GetTempPath(), "FileRaftLogTests_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
    }

    [TestCleanup]
    public void Cleanup()
    {
        if (Directory.Exists(_tempDir))
        {
            Directory.Delete(_tempDir, true);
        }
    }

    [TestMethod]
    public void Constructor_InitializesEmptyLog_AndCommitIndexRoundTrips()
    {
        using FileRaftLog log = new(_tempDir);

        log.CommitIndex = 7;

        Assert.AreEqual(0L, log.LastIndex);
        Assert.AreEqual(0L, log.LastTerm);
        Assert.AreEqual(7L, log.CommitIndex);
    }

    [TestMethod]
    public async Task AppendAndReadEntries_RoundTripState()
    {
        using FileRaftLog log = new(_tempDir);
        await log.AppendAsync(SampleEntries, CancellationToken.None);

        RaftLogEntry? second = await log.GetEntryAsync(2, CancellationToken.None);
        IReadOnlyList<RaftLogEntry> entries = await CollectAsync(log.GetEntriesAsync(1, 5, CancellationToken.None));
        long termAtSecond = await log.GetTermAtIndexAsync(2, CancellationToken.None);
        long termAtMissing = await log.GetTermAtIndexAsync(99, CancellationToken.None);

        Assert.AreEqual(3L, log.LastIndex);
        Assert.AreEqual(3L, log.LastTerm);
        Assert.IsNotNull(second);
        Assert.AreEqual(2L, second.Index);
        Assert.AreEqual(2L, second.Term);
        CollectionAssert.AreEqual(new byte[] { 2, 3 }, second.Command.ToArray());
        Assert.HasCount(3, entries);
        Assert.AreEqual(2L, termAtSecond);
        Assert.AreEqual(0L, termAtMissing);
    }

    [TestMethod]
    public async Task TruncateFromAsync_UpdatesIndicesAndLastTerm()
    {
        using FileRaftLog log = new(_tempDir);
        await log.AppendAsync(SampleEntries, CancellationToken.None);

        await log.TruncateFromAsync(3, CancellationToken.None);

        Assert.AreEqual(2L, log.LastIndex);
        Assert.AreEqual(2L, log.LastTerm);
        Assert.IsNull(await log.GetEntryAsync(3, CancellationToken.None));

        await log.TruncateFromAsync(1, CancellationToken.None);

        Assert.AreEqual(0L, log.LastIndex);
        Assert.AreEqual(0L, log.LastTerm);
        Assert.IsNull(await log.GetEntryAsync(1, CancellationToken.None));
    }

    [TestMethod]
    public async Task Constructor_RecoversAndTruncatesPartialWrites()
    {
        string logPath = Path.Combine(_tempDir, "raft.log");

        using (FileRaftLog writer = new(_tempDir))
        {
            await writer.AppendAsync(SampleEntries[..2], CancellationToken.None);
        }

        long validLength = new FileInfo(logPath).Length;
        await File.AppendAllBytesAsync(logPath, new byte[] { 1, 2, 3, 4, 5 });
        long corruptedLength = new FileInfo(logPath).Length;

        using FileRaftLog recovered = new(_tempDir);

        Assert.AreEqual(2L, recovered.LastIndex);
        Assert.AreEqual(2L, recovered.LastTerm);
        Assert.AreNotEqual(validLength, corruptedLength);
        Assert.AreEqual(validLength, new FileInfo(logPath).Length);
    }

    [TestMethod]
    public async Task GetEntryAsync_ReturnsNull_WhenIndexIsZero()
    {
        using FileRaftLog log = new(_tempDir);
        await log.AppendAsync(SampleEntries, CancellationToken.None);

        RaftLogEntry? result = await log.GetEntryAsync(0, CancellationToken.None);

        Assert.IsNull(result);
    }

    [TestMethod]
    public async Task GetEntryAsync_ReturnsNull_WhenIndexIsNegative()
    {
        using FileRaftLog log = new(_tempDir);
        await log.AppendAsync(SampleEntries, CancellationToken.None);

        RaftLogEntry? result = await log.GetEntryAsync(-1, CancellationToken.None);

        Assert.IsNull(result);
    }

    [TestMethod]
    public async Task GetEntryAsync_ReturnsNull_WhenIndexExceedsCount()
    {
        using FileRaftLog log = new(_tempDir);
        await log.AppendAsync(SampleEntries, CancellationToken.None);

        RaftLogEntry? result = await log.GetEntryAsync(99, CancellationToken.None);

        Assert.IsNull(result);
    }

    [TestMethod]
    public async Task GetTermAtIndexAsync_ReturnsZero_WhenIndexIsZero()
    {
        using FileRaftLog log = new(_tempDir);
        await log.AppendAsync(SampleEntries, CancellationToken.None);

        long term = await log.GetTermAtIndexAsync(0, CancellationToken.None);

        Assert.AreEqual(0L, term);
    }

    [TestMethod]
    public async Task GetTermAtIndexAsync_ReturnsZero_WhenIndexExceedsCount()
    {
        using FileRaftLog log = new(_tempDir);
        await log.AppendAsync(SampleEntries, CancellationToken.None);

        long term = await log.GetTermAtIndexAsync(100, CancellationToken.None);

        Assert.AreEqual(0L, term);
    }

    [TestMethod]
    public async Task TruncateFromAsync_IsNoOp_WhenIndexExceedsCount()
    {
        using FileRaftLog log = new(_tempDir);
        await log.AppendAsync(SampleEntries, CancellationToken.None);

        // TruncateFrom beyond last index should be a no-op
        await log.TruncateFromAsync(100, CancellationToken.None);

        Assert.AreEqual(3L, log.LastIndex);
        Assert.AreEqual(3L, log.LastTerm);
    }

    [TestMethod]
    public async Task TruncateFromAsync_Index1_ClearsEntireLog()
    {
        using FileRaftLog log = new(_tempDir);
        await log.AppendAsync(SampleEntries, CancellationToken.None);

        await log.TruncateFromAsync(1, CancellationToken.None);

        Assert.AreEqual(0L, log.LastIndex);
        Assert.AreEqual(0L, log.LastTerm);
        Assert.IsNull(await log.GetEntryAsync(1, CancellationToken.None));
    }

    [TestMethod]
    public async Task GetEntriesAsync_ReturnsEmpty_WhenStartIndexExceedsCount()
    {
        using FileRaftLog log = new(_tempDir);
        await log.AppendAsync(SampleEntries, CancellationToken.None);

        List<RaftLogEntry> result = await CollectAsync(log.GetEntriesAsync(100, 5, CancellationToken.None));

        Assert.AreEqual(0, result.Count);
    }

    [TestMethod]
    public async Task GetEntriesAsync_ReturnsSubset_WhenCountExceedsAvailableEntries()
    {
        using FileRaftLog log = new(_tempDir);
        await log.AppendAsync(SampleEntries, CancellationToken.None);

        // Ask for 10 starting at index 2; only 2 exist (indices 2 and 3)
        List<RaftLogEntry> result = await CollectAsync(log.GetEntriesAsync(2, 10, CancellationToken.None));

        Assert.HasCount(2, result);
        Assert.AreEqual(2L, result[0].Index);
        Assert.AreEqual(3L, result[1].Index);
    }

    [TestMethod]
    public async Task AppendAsync_ThrowsArgumentNull_WhenEntriesIsNull()
    {
        using FileRaftLog log = new(_tempDir);

        bool threw = false;
        try
        {
            await log.AppendAsync(null!, CancellationToken.None);
        }
        catch (ArgumentNullException)
        {
            threw = true;
        }
        Assert.IsTrue(threw);
    }

    [TestMethod]
    public async Task TruncateFromAsync_SetsLastTermToZero_WhenAllEntriesRemoved()
    {
        // Truncate everything leaving an empty log (index <= 1 path)
        using FileRaftLog log = new(_tempDir);
        await log.AppendAsync(SampleEntries[..1], CancellationToken.None); // 1 entry

        await log.TruncateFromAsync(1, CancellationToken.None);

        Assert.AreEqual(0L, log.LastTerm);
        Assert.AreEqual(0L, log.LastIndex);
    }

    [TestMethod]
    public async Task Constructor_CreatesDirectoryIfNotExists()
    {
        string newDir = Path.Combine(_tempDir, "subdir_" + Guid.NewGuid().ToString("N"));
        Assert.IsFalse(Directory.Exists(newDir));

        using FileRaftLog log = new(newDir);

        Assert.IsTrue(Directory.Exists(newDir));
        Assert.AreEqual(0L, log.LastIndex);
    }

    [TestMethod]
    public async Task Constructor_RecoverFromExistingLog_RestoresState()
    {
        // Write entries, close, reopen and verify state is restored
        using (FileRaftLog writer = new(_tempDir))
        {
            await writer.AppendAsync(SampleEntries, CancellationToken.None);
        }

        using FileRaftLog reader = new(_tempDir);

        Assert.AreEqual(3L, reader.LastIndex);
        Assert.AreEqual(3L, reader.LastTerm);

        RaftLogEntry? entry = await reader.GetEntryAsync(2, CancellationToken.None);
        Assert.IsNotNull(entry);
        Assert.AreEqual(2L, entry.Index);
        CollectionAssert.AreEqual(new byte[] { 2, 3 }, entry.Command.ToArray());
    }

    [TestMethod]
    public async Task Recover_DoesNotTruncate_WhenFileIsClean()
    {
        string logPath = Path.Combine(_tempDir, "raft.log");

        using (FileRaftLog writer = new(_tempDir))
        {
            await writer.AppendAsync(SampleEntries[..1], CancellationToken.None);
        }

        long lengthBefore = new FileInfo(logPath).Length;

        // Reopen -- file is clean, no truncation should occur
        using FileRaftLog recovered = new(_tempDir);
        long lengthAfter = new FileInfo(logPath).Length;

        Assert.AreEqual(lengthBefore, lengthAfter);
        Assert.AreEqual(1L, recovered.LastIndex);
    }

    [TestMethod]
    public async Task GetEntriesAsync_CancellationToken_ThrowsOperationCanceled()
    {
        using FileRaftLog log = new(_tempDir);
        await log.AppendAsync(SampleEntries, CancellationToken.None);

        using CancellationTokenSource cts = new();
        await cts.CancelAsync();

        bool threw = false;
        try
        {
            await CollectAsync(log.GetEntriesAsync(1, 3, cts.Token), cts.Token);
        }
        catch (OperationCanceledException)
        {
            threw = true;
        }
        Assert.IsTrue(threw);
    }

    [TestMethod]
    public async Task AppendAsync_ThrowsOnSecondEntry_DoesNotPublishBatch()
    {
        using (FileRaftLog log = new(_tempDir))
        {
            await log.AppendAsync(SampleEntries[..1], CancellationToken.None);
            IOException thrown = await Assert.ThrowsExactlyAsync<IOException>(
                () => log.AppendAsync(YieldThenThrow(SampleEntries[1], new IOException("write")), CancellationToken.None));
            Assert.AreEqual("write", thrown.Message);
            Assert.AreEqual(1L, log.LastIndex);
            Assert.IsNotNull(await log.GetEntryAsync(1, CancellationToken.None));
            Assert.IsNull(await log.GetEntryAsync(2, CancellationToken.None));
        }

        using FileRaftLog reopened = new(_tempDir);
        Assert.AreEqual(1L, reopened.LastIndex);
        Assert.IsNull(await reopened.GetEntryAsync(2, CancellationToken.None));
    }

    [TestMethod]
    public async Task TruncateFromAsync_SurvivesReopenWithoutExtraFlush()
    {
        using (FileRaftLog log = new(_tempDir))
        {
            await log.AppendAsync(SampleEntries, CancellationToken.None);
            await log.TruncateFromAsync(3, CancellationToken.None);
        }

        using FileRaftLog reopened = new(_tempDir);
        Assert.AreEqual(2L, reopened.LastIndex);
        Assert.IsNull(await reopened.GetEntryAsync(3, CancellationToken.None));
    }

    private static IEnumerable<RaftLogEntry> YieldThenThrow(RaftLogEntry first, Exception exception)
    {
        yield return first;
        throw exception;
    }

    private static async Task<List<T>> CollectAsync<T>(IAsyncEnumerable<T> source, CancellationToken cancellationToken = default)
    {
        List<T> list = [];
        await foreach (T item in source.WithCancellation(cancellationToken))
            list.Add(item);
        return list;
    }
}
