// IonStream License 1.0
// Copyright (c) 2026 SCHIPPER.IO LLC - All Rights Reserved
// Licensor: SCHIPPER.IO LLC - Contact: contact@schipper.io

using Schipper.Io.Raft.Protocol;
using Schipper.Io.Raft.Storage;

namespace Schipper.Io.Raft.Tests.Storage;

[TestClass]
public class SegmentedRaftLogTests
{
    // Fixed 100-byte payloads make the on-disk layout deterministic:
    // segment header = 22 bytes, frame = 20 (header) + 100 (payload) + 4 (crc) = 124 bytes.
    // With SegmentBytes = 1024 and one-entry batches, a roll happens after entry 9 of a segment
    // (22 + 9 * 124 = 1138 >= 1024), so segments hold indices 1-9, 10-18, 19-27, ...
    private const int PayloadSize = 100;
    private const int SmallSegmentBytes = 1024;
    private const int SegmentHeaderSize = 22;
    private const int FrameSize = 20 + PayloadSize + 4;

    private string _tempDir = string.Empty;

    [TestInitialize]
    public void Setup()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "SegmentedRaftLogTests_" + Guid.NewGuid().ToString("N"));
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

    private static SegmentedRaftLogOptions FastOptions(int segmentBytes = SmallSegmentBytes) => new()
    {
        FsyncOnAppend = false,
        SegmentBytes = segmentBytes,
        CommitIndexFlushInterval = TimeSpan.FromMilliseconds(50)
    };

    private static RaftLogEntry MakeEntry(long index)
    {
        byte[] payload = new byte[PayloadSize];
        Array.Fill(payload, (byte)(index & 0xFF));
        return new RaftLogEntry { Term = index, Index = index, Command = payload };
    }

    /// <summary>Appends entries one batch per entry so segment rolls happen at deterministic indices.</summary>
    private static async Task AppendOneByOneAsync(SegmentedRaftLog log, long fromIndex, long toIndex)
    {
        for (long i = fromIndex; i <= toIndex; i++)
            await log.AppendAsync([MakeEntry(i)], CancellationToken.None);
    }

    private string[] SegmentFiles() =>
        [.. Directory.GetFiles(Path.Combine(_tempDir, "segments"), "*.seg").OrderBy(f => f, StringComparer.Ordinal)];

    private static async Task<List<RaftLogEntry>> CollectAsync(IAsyncEnumerable<RaftLogEntry> source)
    {
        List<RaftLogEntry> list = [];
        await foreach (RaftLogEntry entry in source)
            list.Add(entry);
        return list;
    }

    // ------------------------------------------------------------------ basics

    [TestMethod]
    public void EmptyLog_HasZeroIndices()
    {
        using SegmentedRaftLog log = new(_tempDir, FastOptions());

        Assert.AreEqual(0L, log.LastIndex);
        Assert.AreEqual(0L, log.LastTerm);
        Assert.AreEqual(0L, log.FirstIndex);
        Assert.AreEqual(0L, log.CommitIndex);
    }

    [TestMethod]
    public async Task AppendAndRead_RoundTrip()
    {
        using SegmentedRaftLog log = new(_tempDir, FastOptions());
        await log.AppendAsync([MakeEntry(1), MakeEntry(2), MakeEntry(3)], CancellationToken.None);

        Assert.AreEqual(3L, log.LastIndex);
        Assert.AreEqual(3L, log.LastTerm);
        Assert.AreEqual(1L, log.FirstIndex);

        RaftLogEntry? second = await log.GetEntryAsync(2, CancellationToken.None);
        Assert.IsNotNull(second);
        Assert.AreEqual(2L, second.Index);
        Assert.AreEqual(2L, second.Term);
        Assert.AreEqual((byte)2, second.Command.Span[0]);
        Assert.AreEqual(PayloadSize, second.Command.Length);

        List<RaftLogEntry> all = await CollectAsync(log.GetEntriesAsync(1, 10, CancellationToken.None));
        Assert.HasCount(3, all);
        Assert.AreEqual(1L, all[0].Index);
        Assert.AreEqual(3L, all[2].Index);

        Assert.AreEqual(2L, await log.GetTermAtIndexAsync(2, CancellationToken.None));
        Assert.AreEqual(0L, await log.GetTermAtIndexAsync(0, CancellationToken.None));
        Assert.AreEqual(0L, await log.GetTermAtIndexAsync(99, CancellationToken.None));
    }

    [TestMethod]
    public async Task GetEntry_OutOfRange_ReturnsNull()
    {
        using SegmentedRaftLog log = new(_tempDir, FastOptions());
        await AppendOneByOneAsync(log, 1, 3);

        Assert.IsNull(await log.GetEntryAsync(0, CancellationToken.None));
        Assert.IsNull(await log.GetEntryAsync(-1, CancellationToken.None));
        Assert.IsNull(await log.GetEntryAsync(4, CancellationToken.None));
    }

    [TestMethod]
    public async Task AppendAsync_ThrowsArgumentNull_WhenEntriesIsNull()
    {
        using SegmentedRaftLog log = new(_tempDir, FastOptions());

        await Assert.ThrowsExactlyAsync<ArgumentNullException>(
            async () => await log.AppendAsync(null!, CancellationToken.None));
    }

    // ------------------------------------------------------------------ segment rolling

    [TestMethod]
    public async Task MultiSegment_RollsAndReadsAcrossSegments()
    {
        using SegmentedRaftLog log = new(_tempDir, FastOptions());
        await AppendOneByOneAsync(log, 1, 25);

        // Entries 1-9 sealed, 10-18 sealed, 19-25 in the active segment.
        string[] segFiles = SegmentFiles();
        Assert.HasCount(3, segFiles);
        Assert.AreEqual(25L, log.LastIndex);
        Assert.AreEqual(25L, log.LastTerm);
        Assert.AreEqual(1L, log.FirstIndex);

        List<RaftLogEntry> all = await CollectAsync(log.GetEntriesAsync(1, 100, CancellationToken.None));
        Assert.HasCount(25, all);
        for (int i = 0; i < 25; i++)
        {
            Assert.AreEqual(i + 1, all[i].Index);
            Assert.AreEqual((byte)(i + 1), all[i].Command.Span[0]);
        }

        // A read range spanning a segment boundary
        List<RaftLogEntry> spanning = await CollectAsync(log.GetEntriesAsync(8, 4, CancellationToken.None));
        Assert.HasCount(4, spanning);
        Assert.AreEqual(8L, spanning[0].Index);
        Assert.AreEqual(11L, spanning[3].Index);
    }

    // ------------------------------------------------------------------ recovery

    [TestMethod]
    public async Task Reopen_RestoresState()
    {
        using (SegmentedRaftLog writer = new(_tempDir, FastOptions()))
        {
            await AppendOneByOneAsync(writer, 1, 25);
        }

        using SegmentedRaftLog log = new(_tempDir, FastOptions());
        Assert.AreEqual(25L, log.LastIndex);
        Assert.AreEqual(25L, log.LastTerm);
        Assert.AreEqual(1L, log.FirstIndex);

        RaftLogEntry? sealedEntry = await log.GetEntryAsync(5, CancellationToken.None);
        RaftLogEntry? activeEntry = await log.GetEntryAsync(25, CancellationToken.None);
        Assert.IsNotNull(sealedEntry);
        Assert.IsNotNull(activeEntry);
        Assert.AreEqual((byte)5, sealedEntry.Command.Span[0]);
        Assert.AreEqual((byte)25, activeEntry.Command.Span[0]);

        // Appending after recovery continues the sequence
        await log.AppendAsync([MakeEntry(26)], CancellationToken.None);
        Assert.AreEqual(26L, log.LastIndex);
    }

    [TestMethod]
    public async Task Reopen_TrustsSealedIdx_CorruptSealedBodyOnlyFailsThatRead()
    {
        using (SegmentedRaftLog writer = new(_tempDir, FastOptions()))
        {
            await AppendOneByOneAsync(writer, 1, 25);
        }

        // Corrupt one payload byte of entry 3 inside the first (sealed) segment.
        string sealedSeg = SegmentFiles()[0];
        long corruptOffset = SegmentHeaderSize + (2 * FrameSize) + 20 + 10; // entry 3, payload byte 10
        using (FileStream fs = new(sealedSeg, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
        {
            fs.Position = corruptOffset;
            int original = fs.ReadByte();
            fs.Position = corruptOffset;
            fs.WriteByte((byte)(original ^ 0xFF));
        }

        // Reopen succeeds because sealed segments are never scanned (the idx footer is trusted).
        using SegmentedRaftLog log = new(_tempDir, FastOptions());
        Assert.AreEqual(25L, log.LastIndex);

        // Reading the corrupted entry fails the CRC check...
        await Assert.ThrowsExactlyAsync<InvalidDataException>(
            async () => await log.GetEntryAsync(3, CancellationToken.None));

        // ...but entries around it (same sealed segment, located via the trusted idx) still read fine.
        RaftLogEntry? before = await log.GetEntryAsync(2, CancellationToken.None);
        RaftLogEntry? after = await log.GetEntryAsync(5, CancellationToken.None);
        Assert.IsNotNull(before);
        Assert.IsNotNull(after);
        Assert.AreEqual((byte)5, after.Command.Span[0]);
    }

    [TestMethod]
    public async Task Reopen_TruncatesTornTail_OnActiveSegment()
    {
        using (SegmentedRaftLog writer = new(_tempDir, FastOptions()))
        {
            await AppendOneByOneAsync(writer, 1, 5);
        }

        string activeSeg = SegmentFiles()[^1];
        long cleanLength = new FileInfo(activeSeg).Length;
        await File.AppendAllBytesAsync(activeSeg, new byte[] { 1, 2, 3, 4, 5, 6, 7 });

        using SegmentedRaftLog log = new(_tempDir, FastOptions());
        Assert.AreEqual(5L, log.LastIndex);
        Assert.AreEqual(5L, log.LastTerm);
        Assert.AreEqual(cleanLength, new FileInfo(activeSeg).Length);

        await log.AppendAsync([MakeEntry(6)], CancellationToken.None);
        RaftLogEntry? appended = await log.GetEntryAsync(6, CancellationToken.None);
        Assert.IsNotNull(appended);
    }

    [TestMethod]
    public async Task Reopen_RebuildsSealedIdx_WhenFooterMissing()
    {
        using (SegmentedRaftLog writer = new(_tempDir, FastOptions()))
        {
            await AppendOneByOneAsync(writer, 1, 12);
        }

        // Destroy the sealed segment's idx; recovery must fall back to scanning and rebuild it.
        string sealedIdx = Path.ChangeExtension(SegmentFiles()[0], ".idx");
        File.Delete(sealedIdx);

        using SegmentedRaftLog log = new(_tempDir, FastOptions());
        Assert.AreEqual(12L, log.LastIndex);
        RaftLogEntry? entry = await log.GetEntryAsync(4, CancellationToken.None);
        Assert.IsNotNull(entry);
        Assert.AreEqual((byte)4, entry.Command.Span[0]);
        Assert.IsTrue(File.Exists(sealedIdx));
    }

    // ------------------------------------------------------------------ reverse reads

    [TestMethod]
    public async Task GetEntriesReverse_OrdersPagesAndClamps()
    {
        using SegmentedRaftLog log = new(_tempDir, FastOptions());
        await AppendOneByOneAsync(log, 1, 25);

        // Descending order, crossing the 19|18 segment boundary
        List<RaftLogEntry> page = await CollectAsync(log.GetEntriesReverseAsync(20, 5, CancellationToken.None));
        Assert.HasCount(5, page);
        CollectionAssert.AreEqual(new long[] { 20, 19, 18, 17, 16 }, page.Select(e => e.Index).ToArray());

        // From beyond LastIndex clamps down to LastIndex
        List<RaftLogEntry> clampedHigh = await CollectAsync(log.GetEntriesReverseAsync(100, 3, CancellationToken.None));
        CollectionAssert.AreEqual(new long[] { 25, 24, 23 }, clampedHigh.Select(e => e.Index).ToArray());

        // Count larger than what is available clamps at FirstIndex
        List<RaftLogEntry> clampedLow = await CollectAsync(log.GetEntriesReverseAsync(3, 10, CancellationToken.None));
        CollectionAssert.AreEqual(new long[] { 3, 2, 1 }, clampedLow.Select(e => e.Index).ToArray());

        // Zero/negative count yields nothing
        Assert.IsEmpty(await CollectAsync(log.GetEntriesReverseAsync(10, 0, CancellationToken.None)));
    }

    [TestMethod]
    public async Task GetEntriesReverse_EmptyLog_YieldsNothing()
    {
        using SegmentedRaftLog log = new(_tempDir, FastOptions());

        Assert.IsEmpty(await CollectAsync(log.GetEntriesReverseAsync(10, 5, CancellationToken.None)));
    }

    // ------------------------------------------------------------------ truncation

    [TestMethod]
    public async Task TruncateFrom_WithinActiveSegment()
    {
        using SegmentedRaftLog log = new(_tempDir, FastOptions());
        await AppendOneByOneAsync(log, 1, 25);

        await log.TruncateFromAsync(22, CancellationToken.None);

        Assert.AreEqual(21L, log.LastIndex);
        Assert.AreEqual(21L, log.LastTerm);
        Assert.IsNull(await log.GetEntryAsync(22, CancellationToken.None));
        Assert.IsNotNull(await log.GetEntryAsync(21, CancellationToken.None));

        await log.AppendAsync([MakeEntry(22)], CancellationToken.None);
        Assert.AreEqual(22L, log.LastIndex);
    }

    [TestMethod]
    public async Task TruncateFrom_AcrossSegmentBoundary()
    {
        using SegmentedRaftLog log = new(_tempDir, FastOptions());
        await AppendOneByOneAsync(log, 1, 25);

        // 12 lands inside the second segment (10-18): the third segment is deleted whole,
        // the second is truncated in place and becomes the active segment.
        await log.TruncateFromAsync(12, CancellationToken.None);

        Assert.AreEqual(11L, log.LastIndex);
        Assert.AreEqual(11L, log.LastTerm);
        Assert.IsNull(await log.GetEntryAsync(12, CancellationToken.None));
        RaftLogEntry? kept = await log.GetEntryAsync(11, CancellationToken.None);
        Assert.IsNotNull(kept);
        Assert.AreEqual((byte)11, kept.Command.Span[0]);
        Assert.HasCount(2, SegmentFiles());

        await log.AppendAsync([MakeEntry(12)], CancellationToken.None);
        Assert.AreEqual(12L, log.LastIndex);
    }

    [TestMethod]
    public async Task TruncateFrom_AtSegmentStart_ReopensPreviousSegmentWhole()
    {
        using SegmentedRaftLog log = new(_tempDir, FastOptions());
        await AppendOneByOneAsync(log, 1, 25);

        await log.TruncateFromAsync(10, CancellationToken.None);

        Assert.AreEqual(9L, log.LastIndex);
        Assert.AreEqual(9L, log.LastTerm);
        Assert.IsNotNull(await log.GetEntryAsync(9, CancellationToken.None));
        Assert.IsNull(await log.GetEntryAsync(10, CancellationToken.None));
    }

    [TestMethod]
    public async Task TruncateFrom_BelowFirstIndex_ClearsEverything()
    {
        using SegmentedRaftLog log = new(_tempDir, FastOptions());
        await AppendOneByOneAsync(log, 1, 25);

        await log.TruncateFromAsync(1, CancellationToken.None);

        Assert.AreEqual(0L, log.LastIndex);
        Assert.AreEqual(0L, log.LastTerm);
        Assert.AreEqual(0L, log.FirstIndex);
        Assert.IsNull(await log.GetEntryAsync(1, CancellationToken.None));

        await log.AppendAsync([MakeEntry(1)], CancellationToken.None);
        Assert.AreEqual(1L, log.LastIndex);
        Assert.AreEqual(1L, log.FirstIndex);
    }

    [TestMethod]
    public async Task TruncateFrom_BeyondLastIndex_IsNoOp()
    {
        using SegmentedRaftLog log = new(_tempDir, FastOptions());
        await AppendOneByOneAsync(log, 1, 5);

        await log.TruncateFromAsync(100, CancellationToken.None);

        Assert.AreEqual(5L, log.LastIndex);
        Assert.AreEqual(5L, log.LastTerm);
    }

    [TestMethod]
    public async Task TruncateFrom_SurvivesReopen()
    {
        using (SegmentedRaftLog writer = new(_tempDir, FastOptions()))
        {
            await AppendOneByOneAsync(writer, 1, 25);
            await writer.TruncateFromAsync(12, CancellationToken.None);
        }

        using SegmentedRaftLog log = new(_tempDir, FastOptions());
        Assert.AreEqual(11L, log.LastIndex);
        Assert.AreEqual(11L, log.LastTerm);
        Assert.IsNull(await log.GetEntryAsync(12, CancellationToken.None));
    }

    // ------------------------------------------------------------------ compaction

    [TestMethod]
    public async Task CompactTo_DropsWholePrefixSegments_NeverSplits()
    {
        using SegmentedRaftLog log = new(_tempDir, FastOptions());
        await AppendOneByOneAsync(log, 1, 25);

        // 15 is inside the second segment (10-18); only the first segment (1-9) qualifies.
        await log.CompactToAsync(15, CancellationToken.None);

        Assert.AreEqual(10L, log.FirstIndex);
        Assert.AreEqual(25L, log.LastIndex);
        Assert.HasCount(2, SegmentFiles());

        Assert.IsNull(await log.GetEntryAsync(5, CancellationToken.None));
        Assert.IsNull(await log.GetEntryAsync(9, CancellationToken.None));
        RaftLogEntry? retained = await log.GetEntryAsync(10, CancellationToken.None);
        Assert.IsNotNull(retained);
        Assert.AreEqual((byte)10, retained.Command.Span[0]);

        // Term of the entry just below FirstIndex comes from the segment header's prevTerm.
        Assert.AreEqual(9L, await log.GetTermAtIndexAsync(9, CancellationToken.None));
        Assert.AreEqual(0L, await log.GetTermAtIndexAsync(5, CancellationToken.None));

        // Reads below FirstIndex yield nothing
        Assert.IsEmpty(await CollectAsync(log.GetEntriesAsync(5, 3, CancellationToken.None)));
        // Reverse reads clamp at FirstIndex
        List<RaftLogEntry> reverse = await CollectAsync(log.GetEntriesReverseAsync(11, 10, CancellationToken.None));
        CollectionAssert.AreEqual(new long[] { 11, 10 }, reverse.Select(e => e.Index).ToArray());
    }

    [TestMethod]
    public async Task CompactTo_BeyondLastIndex_KeepsActiveSegment()
    {
        using SegmentedRaftLog log = new(_tempDir, FastOptions());
        await AppendOneByOneAsync(log, 1, 25);

        await log.CompactToAsync(1000, CancellationToken.None);

        Assert.AreEqual(19L, log.FirstIndex); // active segment is never dropped
        Assert.AreEqual(25L, log.LastIndex);
        Assert.IsNotNull(await log.GetEntryAsync(19, CancellationToken.None));
        Assert.AreEqual(18L, await log.GetTermAtIndexAsync(18, CancellationToken.None)); // prevTerm of active
    }

    [TestMethod]
    public async Task CompactTo_SurvivesReopen()
    {
        using (SegmentedRaftLog writer = new(_tempDir, FastOptions()))
        {
            await AppendOneByOneAsync(writer, 1, 25);
            await writer.CompactToAsync(15, CancellationToken.None);
        }

        using SegmentedRaftLog log = new(_tempDir, FastOptions());
        Assert.AreEqual(10L, log.FirstIndex);
        Assert.AreEqual(25L, log.LastIndex);
        Assert.IsNull(await log.GetEntryAsync(9, CancellationToken.None));
        Assert.IsNotNull(await log.GetEntryAsync(10, CancellationToken.None));
        Assert.AreEqual(9L, await log.GetTermAtIndexAsync(9, CancellationToken.None));
    }

    // ------------------------------------------------------------------ commit index + hard state

    [TestMethod]
    public async Task CommitIndex_PersistsAcrossReopen_ViaDisposeFlush()
    {
        using (SegmentedRaftLog writer = new(_tempDir, FastOptions()))
        {
            await AppendOneByOneAsync(writer, 1, 3);
            writer.CommitIndex = 3;
        } // Dispose flushes the latest commit index

        using SegmentedRaftLog log = new(_tempDir, FastOptions());
        Assert.AreEqual(3L, log.CommitIndex);
    }

    [TestMethod]
    public async Task CommitIndex_DebouncedFlush_WritesMetaWithoutDispose()
    {
        string metaPath = Path.Combine(_tempDir, "meta.bin");
        using SegmentedRaftLog log = new(_tempDir, FastOptions());
        await log.AppendAsync([MakeEntry(1)], CancellationToken.None);

        log.CommitIndex = 1;

        // The debounced write (50 ms interval) must land without disposing the log.
        bool flushed = false;
        for (int i = 0; i < 100 && !flushed; i++)
        {
            await Task.Delay(50);
            flushed = RaftMeta.Load(metaPath).CommitIndex == 1;
        }
        Assert.IsTrue(flushed, "Debounced commit-index flush did not reach meta.bin.");
    }

    [TestMethod]
    public async Task HardState_SaveLoad_RoundTripsAcrossReopen()
    {
        using (SegmentedRaftLog writer = new(_tempDir, FastOptions()))
        {
            await writer.SaveHardStateAsync(5, "node2", CancellationToken.None);
        }

        using (SegmentedRaftLog log = new(_tempDir, FastOptions()))
        {
            (long term, string? votedFor) = log.LoadHardState();
            Assert.AreEqual(5L, term);
            Assert.AreEqual("node2", votedFor);

            await log.SaveHardStateAsync(6, null, CancellationToken.None);
        }

        using SegmentedRaftLog reopened = new(_tempDir, FastOptions());
        (long term2, string? votedFor2) = reopened.LoadHardState();
        Assert.AreEqual(6L, term2);
        Assert.IsNull(votedFor2);
    }

    [TestMethod]
    public void HardState_DefaultsToZeroNull_WhenMetaMissing()
    {
        using SegmentedRaftLog log = new(_tempDir, FastOptions());

        (long term, string? votedFor) = log.LoadHardState();
        Assert.AreEqual(0L, term);
        Assert.IsNull(votedFor);
    }

    [TestMethod]
    public async Task Meta_CorruptCurrentFile_FallsBackToPreviousGeneration()
    {
        using (SegmentedRaftLog writer = new(_tempDir, FastOptions()))
        {
            await writer.SaveHardStateAsync(9, "node3", CancellationToken.None);
        }

        string metaPath = Path.Combine(_tempDir, "meta.bin");
        byte[] data = await File.ReadAllBytesAsync(metaPath);
        data[^1] ^= 0xFF;
        await File.WriteAllBytesAsync(metaPath, data);

        using SegmentedRaftLog log = new(_tempDir, FastOptions());
        (long term, string? votedFor) = log.LoadHardState();
        Assert.AreEqual(9L, term);
        Assert.AreEqual("node3", votedFor);
    }

    [TestMethod]
    public async Task AppendAsync_ThrowsOnSecondEntry_DoesNotPublishBatch()
    {
        using (SegmentedRaftLog log = new(_tempDir, FastOptions()))
        {
            await log.AppendAsync([MakeEntry(1)], CancellationToken.None);
            await Assert.ThrowsExactlyAsync<IOException>(
                () => log.AppendAsync(YieldThenThrow(MakeEntry(2), new IOException("write")), CancellationToken.None));
            Assert.AreEqual(1L, log.LastIndex);
            Assert.IsNotNull(await log.GetEntryAsync(1, CancellationToken.None));
            Assert.IsNull(await log.GetEntryAsync(2, CancellationToken.None));
        }

        using SegmentedRaftLog reopened = new(_tempDir, FastOptions());
        Assert.AreEqual(1L, reopened.LastIndex);
        Assert.IsNull(await reopened.GetEntryAsync(2, CancellationToken.None));
    }

    [TestMethod]
    public async Task GetTermAtIndexAsync_Throws_WhenPayloadCrcIsWrong()
    {
        using SegmentedRaftLog log = new(_tempDir, FastOptions());
        await AppendOneByOneAsync(log, 1, 10);

        string seg = SegmentFiles()[0];
        byte[] bytes = await File.ReadAllBytesAsync(seg);
        bytes[SegmentHeaderSize + 20] ^= 0xFF;
        await File.WriteAllBytesAsync(seg, bytes);

        await Assert.ThrowsExactlyAsync<InvalidDataException>(() => log.GetTermAtIndexAsync(1, CancellationToken.None));
    }

    [TestMethod]
    public async Task GetTermAtIndexAsync_Throws_WhenTermByteIsFlipped()
    {
        using SegmentedRaftLog log = new(_tempDir, FastOptions());
        await AppendOneByOneAsync(log, 1, 10);

        string seg = SegmentFiles()[0];
        byte[] bytes = await File.ReadAllBytesAsync(seg);
        bytes[SegmentHeaderSize + 4] ^= 0xFF;
        await File.WriteAllBytesAsync(seg, bytes);

        await Assert.ThrowsExactlyAsync<InvalidDataException>(() => log.GetTermAtIndexAsync(1, CancellationToken.None));
    }

    [TestMethod]
    public async Task GetTermAtIndexAsync_ReturnsTerm_WhenFrameIsValid()
    {
        using SegmentedRaftLog log = new(_tempDir, FastOptions());
        await log.AppendAsync([MakeEntry(1)], CancellationToken.None);
        Assert.AreEqual(1L, await log.GetTermAtIndexAsync(1, CancellationToken.None));
    }

    [TestMethod]
    public async Task HardState_SurvivesDebouncedCommitRewrite()
    {
        using SegmentedRaftLog log = new(_tempDir, FastOptions());
        await log.SaveHardStateAsync(5, "node2", CancellationToken.None);
        log.CommitIndex = 1;

        bool flushed = false;
        for (int i = 0; i < 100 && !flushed; i++)
        {
            await Task.Delay(50);
            flushed = RaftMeta.Load(Path.Combine(_tempDir, "meta.bin")).CommitIndex == 1;
        }
        Assert.IsTrue(flushed);
        (long term, string? votedFor) = log.LoadHardState();
        Assert.AreEqual(5L, term);
        Assert.AreEqual("node2", votedFor);
        RaftMetaState disk = RaftMeta.Load(Path.Combine(_tempDir, "meta.bin"));
        Assert.AreEqual(5L, disk.Term);
        Assert.AreEqual("node2", disk.VotedFor);
    }

    [TestMethod]
    public async Task HardState_LoadKeepsVote_WhenDebouncedFlushFailsBeforeReplace()
    {
        using SegmentedRaftLog log = new(_tempDir, FastOptions());
        await log.SaveHardStateAsync(5, "node2", CancellationToken.None);
        try
        {
            RaftMeta.AfterNonFsyncFlush = (fsync, path) =>
            {
                if (!fsync && path.StartsWith(_tempDir, StringComparison.Ordinal))
                    throw new IOException("disk failed after Flush(false)");
            };
            log.CommitIndex = 1;
            await Task.Delay(200);
        }
        finally
        {
            RaftMeta.AfterNonFsyncFlush = null;
        }

        RaftMetaState loaded = RaftMeta.Load(Path.Combine(_tempDir, "meta.bin"));
        Assert.AreEqual(5L, loaded.Term);
        Assert.AreEqual("node2", loaded.VotedFor);
    }

    private static IEnumerable<RaftLogEntry> YieldThenThrow(RaftLogEntry first, Exception exception)
    {
        yield return first;
        throw exception;
    }
}
