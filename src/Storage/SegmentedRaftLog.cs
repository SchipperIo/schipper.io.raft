// IonStream License 1.0
// Copyright (c) 2026 SCHIPPER.IO LLC - All Rights Reserved
// Licensor: SCHIPPER.IO LLC - Contact: contact@schipper.io

using System.Buffers.Binary;
using System.Globalization;
using System.Runtime.CompilerServices;
using Schipper.Io.Raft.Protocol;

namespace Schipper.Io.Raft.Storage;

/// <summary>
/// Options for <see cref="SegmentedRaftLog"/>.
/// </summary>
public sealed class SegmentedRaftLogOptions
{
    /// <summary>Fsync the segment file once per append batch. Default true.</summary>
    public bool FsyncOnAppend { get; init; } = true;

    /// <summary>Roll to a new segment once the active segment reaches this size. Default 4 MB.</summary>
    public int SegmentBytes { get; init; } = 4 * 1024 * 1024;

    /// <summary>Debounce interval for persisting <see cref="IRaftLog.CommitIndex"/> to meta.bin. Default 2 s.</summary>
    public TimeSpan CommitIndexFlushInterval { get; init; } = TimeSpan.FromSeconds(2);
}

/// <summary>
/// Segmented, compactable Raft log with CRC-protected frames and durable hard state.
///
/// On-disk layout under the given directory:
/// <list type="bullet">
/// <item><c>meta.bin</c> — commit index + hard state (term/votedFor), written atomically; the
/// commit index is persisted debounced (a lagging persisted commit index is safe), hard state is
/// fsynced on every save.</item>
/// <item><c>segments/{firstIndex:D20}.seg</c> — header (magic "RSEG", version, firstIndex,
/// prevTerm) followed by frames <c>len u32 | term u64 | index u64 | payload | crc32</c> where the
/// CRC covers term+index+payload.</item>
/// <item><c>{firstIndex:D20}.idx</c> — sidecar with one u64 file offset per entry. A sealed
/// segment's idx ends with a footer (marker + entry count) and is trusted on recovery, so startup
/// never reads sealed segment contents; the active segment's idx is best-effort and rebuilt by
/// scanning on recovery (torn tails are truncated).</item>
/// </list>
/// </summary>
public sealed class SegmentedRaftLog : ICompactableRaftLog, IRaftHardStateStore, IDisposable
{
    private const ushort FormatVersion = 1;
    private const int SegmentHeaderSize = 22; // magic(4) + ver(2) + firstIndex(8) + prevTerm(8)
    private const int IndexHeaderSize = 6;    // magic(4) + ver(2)
    private const int IndexFooterSize = 16;   // marker(8) + entryCount(8)
    private const int FrameHeaderSize = 20;   // len(4) + term(8) + index(8)
    private const int FrameCrcSize = 4;
    private const ulong IndexFooterMarker = 0xFFFFFFFFFFFFFFFF;
    private const int SealedOffsetsCacheCapacity = 4;

    private static ReadOnlySpan<byte> SegmentMagic => "RSEG"u8;
    private static ReadOnlySpan<byte> IndexMagic => "RIDX"u8;

    private readonly SegmentedRaftLogOptions _options;
    private readonly string _metaPath;
    private readonly string _segmentsDir;
    private readonly SemaphoreSlim _lock = new(1, 1);
    private readonly object _metaLock = new();

    // All segments in index order; the last one is always the active (writable) segment.
    private readonly List<LogSegment> _segments = [];
    private FileStream _activeSeg = null!;
    private FileStream _activeIdx = null!;
    private List<long> _activeOffsets = [];
    private long _activePrevTerm;

    // Lazily loaded offset arrays for sealed segments, keyed by FirstIndex (small LRU).
    private readonly Dictionary<long, long[]> _sealedOffsetsCache = [];
    private readonly List<long> _sealedOffsetsLru = []; // most recently used last

    private long _lastIndex;
    private long _lastTerm;
    private long _firstSegmentIndex;
    private long _commitIndex;
    private long _hardStateTerm;
    private string? _hardStateVotedFor;
    private long _persistedCommitIndex;
    private int _commitFlushScheduled;
    private volatile bool _disposed;

    public long LastIndex => Volatile.Read(ref _lastIndex);
    public long LastTerm => Volatile.Read(ref _lastTerm);

    /// <inheritdoc />
    public long FirstIndex => Volatile.Read(ref _lastIndex) == 0 ? 0 : Volatile.Read(ref _firstSegmentIndex);

    public long CommitIndex
    {
        get => Volatile.Read(ref _commitIndex);
        set
        {
            Volatile.Write(ref _commitIndex, value);
            ScheduleCommitIndexFlush();
        }
    }

    public SegmentedRaftLog(string directory, SegmentedRaftLogOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(directory);
        _options = options ?? new SegmentedRaftLogOptions();

        Directory.CreateDirectory(directory);
        _segmentsDir = Path.Combine(directory, "segments");
        Directory.CreateDirectory(_segmentsDir);
        _metaPath = Path.Combine(directory, "meta.bin");

        RaftMetaState meta = RaftMeta.Load(_metaPath);
        _commitIndex = meta.CommitIndex;
        _persistedCommitIndex = meta.CommitIndex;
        _hardStateTerm = meta.Term;
        _hardStateVotedFor = meta.VotedFor;

        Recover();
    }

    // ------------------------------------------------------------------ recovery

    private void Recover()
    {
        List<string> segFiles = [];
        foreach (string file in Directory.GetFiles(_segmentsDir, "*.seg"))
        {
            if (long.TryParse(Path.GetFileNameWithoutExtension(file), NumberStyles.None, CultureInfo.InvariantCulture, out _))
                segFiles.Add(file);
        }
        segFiles.Sort(StringComparer.Ordinal);

        // Choose the active segment: the last file with a readable header. A file with a torn
        // header can only be the remnant of a crash while rolling; it holds no entries, drop it.
        int activeIndex = -1;
        for (int i = segFiles.Count - 1; i >= 0; i--)
        {
            if (TryReadSegmentHeader(segFiles[i], out _))
            {
                activeIndex = i;
                break;
            }
            File.Delete(segFiles[i]);
            DeleteIfExists(Path.ChangeExtension(segFiles[i], ".idx"));
        }

        if (activeIndex < 0)
        {
            CreateActiveSegment(firstIndex: 1, prevTerm: 0);
            _lastIndex = 0;
            _lastTerm = 0;
            _firstSegmentIndex = 1;
            return;
        }

        // Sealed segments: trust the idx footer; never read the .seg contents. Fall back to a
        // full scan (and idx rewrite) only when the footer is missing or corrupt.
        for (int i = 0; i < activeIndex; i++)
        {
            long firstIndex = ParseFirstIndex(segFiles[i]);
            long nextFirst = ParseFirstIndex(segFiles[i + 1]);
            long expectedCount = nextFirst - firstIndex;
            string idxPath = Path.ChangeExtension(segFiles[i], ".idx");

            if (!SealedIndexIsValid(idxPath, expectedCount))
            {
                long[] offsets = ScanSealedSegment(segFiles[i], firstIndex);
                WriteIndexFile(idxPath, offsets, offsets.Length, sealedFooter: true, fsync: true);
            }

            _segments.Add(new LogSegment
            {
                FirstIndex = firstIndex,
                LastIndex = nextFirst - 1,
                SegPath = segFiles[i],
                IdxPath = idxPath
            });
        }

        OpenActiveSegment(segFiles[activeIndex]);
        _firstSegmentIndex = _segments[0].FirstIndex;
    }

    private static bool TryReadSegmentHeader(string segPath, out (long FirstIndex, long PrevTerm) header)
    {
        header = default;
        using FileStream fs = new(segPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        if (fs.Length < SegmentHeaderSize) return false;

        Span<byte> buffer = stackalloc byte[SegmentHeaderSize];
        fs.ReadExactly(buffer);
        if (!buffer[..4].SequenceEqual(SegmentMagic)) return false;
        if (BinaryPrimitives.ReadUInt16LittleEndian(buffer[4..]) != FormatVersion) return false;

        long firstIndex = BinaryPrimitives.ReadInt64LittleEndian(buffer[6..]);
        long prevTerm = BinaryPrimitives.ReadInt64LittleEndian(buffer[14..]);
        if (firstIndex != ParseFirstIndex(segPath)) return false;

        header = (firstIndex, prevTerm);
        return true;
    }

    private static bool SealedIndexIsValid(string idxPath, long expectedCount)
    {
        if (!File.Exists(idxPath)) return false;

        using FileStream fs = new(idxPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        long expectedLength = IndexHeaderSize + (expectedCount * sizeof(long)) + IndexFooterSize;
        if (fs.Length != expectedLength) return false;

        Span<byte> header = stackalloc byte[IndexHeaderSize];
        fs.ReadExactly(header);
        if (!header[..4].SequenceEqual(IndexMagic)) return false;
        if (BinaryPrimitives.ReadUInt16LittleEndian(header[4..]) != FormatVersion) return false;

        Span<byte> footer = stackalloc byte[IndexFooterSize];
        fs.Position = fs.Length - IndexFooterSize;
        fs.ReadExactly(footer);
        return BinaryPrimitives.ReadUInt64LittleEndian(footer) == IndexFooterMarker
            && BinaryPrimitives.ReadInt64LittleEndian(footer[8..]) == expectedCount;
    }

    /// <summary>Scans a sealed segment's frames to rebuild its idx (corrupt-footer fallback only).</summary>
    private static long[] ScanSealedSegment(string segPath, long firstIndex)
    {
        using FileStream fs = new(segPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        List<long> offsets = ScanFrames(fs, firstIndex, out _);
        return [.. offsets];
    }

    /// <summary>
    /// Scans frames from the segment header onward, stopping at the first torn or corrupt frame.
    /// Returns the frame start offsets; <paramref name="lastTerm"/> is the term of the last valid
    /// frame (0 when none).
    /// </summary>
    private static List<long> ScanFrames(FileStream fs, long firstIndex, out long lastTerm)
    {
        List<long> offsets = [];
        lastTerm = 0;
        long pos = SegmentHeaderSize;
        byte[] header = new byte[FrameHeaderSize];
        byte[] crcBuffer = new byte[FrameCrcSize];
        byte[] payload = [];

        while (pos + FrameHeaderSize + FrameCrcSize <= fs.Length)
        {
            fs.Position = pos;
            fs.ReadExactly(header, 0, FrameHeaderSize);
            uint length = BinaryPrimitives.ReadUInt32LittleEndian(header);
            long term = BinaryPrimitives.ReadInt64LittleEndian(header.AsSpan(4));
            long index = BinaryPrimitives.ReadInt64LittleEndian(header.AsSpan(12));

            if (length > int.MaxValue - FrameHeaderSize - FrameCrcSize) break;
            if (pos + FrameHeaderSize + length + FrameCrcSize > fs.Length) break; // torn tail
            if (index != firstIndex + offsets.Count) break; // non-monotonic index

            int len = (int)length;
            if (payload.Length < len) payload = new byte[len];
            fs.ReadExactly(payload, 0, len);
            fs.ReadExactly(crcBuffer, 0, FrameCrcSize);

            uint expectedCrc = BinaryPrimitives.ReadUInt32LittleEndian(crcBuffer);
            uint actualCrc = Crc32.Finish(Crc32.Append(Crc32.Append(Crc32.Begin(), header.AsSpan(4, 16)), payload.AsSpan(0, len)));
            if (actualCrc != expectedCrc) break;

            offsets.Add(pos);
            lastTerm = term;
            pos += FrameHeaderSize + len + FrameCrcSize;
        }

        return offsets;
    }

    /// <summary>Opens the last segment as active: scans it, truncates any torn tail, rebuilds its idx.</summary>
    private void OpenActiveSegment(string segPath)
    {
        long firstIndex = ParseFirstIndex(segPath);
        FileStream seg = new(segPath, FileMode.Open, FileAccess.ReadWrite, FileShare.Read);

        Span<byte> header = stackalloc byte[SegmentHeaderSize];
        seg.ReadExactly(header);
        long prevTerm = BinaryPrimitives.ReadInt64LittleEndian(header[14..]);

        List<long> offsets = ScanFrames(seg, firstIndex, out long lastTerm);
        long validLength = offsets.Count == 0
            ? SegmentHeaderSize
            : LastFrameEnd(seg, offsets[^1]);
        if (validLength != seg.Length)
        {
            seg.SetLength(validLength);
            seg.Flush(true);
        }

        _activeSeg = seg;
        _activeOffsets = offsets;
        _activePrevTerm = prevTerm;
        _lastIndex = firstIndex - 1 + offsets.Count;
        _lastTerm = offsets.Count == 0 ? prevTerm : lastTerm;

        string idxPath = Path.ChangeExtension(segPath, ".idx");
        WriteIndexFile(idxPath, offsets, offsets.Count, sealedFooter: false, fsync: false);
        _activeIdx = new FileStream(idxPath, FileMode.Open, FileAccess.ReadWrite, FileShare.Read);
        _activeIdx.Position = _activeIdx.Length;

        _segments.Add(new LogSegment
        {
            FirstIndex = firstIndex,
            LastIndex = _lastIndex,
            SegPath = segPath,
            IdxPath = idxPath,
            PrevTermCache = prevTerm
        });
    }

    private static long LastFrameEnd(FileStream fs, long frameOffset)
    {
        fs.Position = frameOffset;
        Span<byte> lengthBytes = stackalloc byte[4];
        fs.ReadExactly(lengthBytes);
        uint length = BinaryPrimitives.ReadUInt32LittleEndian(lengthBytes);
        return frameOffset + FrameHeaderSize + length + FrameCrcSize;
    }

    // ------------------------------------------------------------------ segment lifecycle

    private static long ParseFirstIndex(string segPath) =>
        long.Parse(Path.GetFileNameWithoutExtension(segPath), NumberStyles.None, CultureInfo.InvariantCulture);

    private string SegmentPathFor(long firstIndex) =>
        Path.Combine(_segmentsDir, firstIndex.ToString("D20", CultureInfo.InvariantCulture) + ".seg");

    private void CreateActiveSegment(long firstIndex, long prevTerm)
    {
        string segPath = SegmentPathFor(firstIndex);
        string idxPath = Path.ChangeExtension(segPath, ".idx");

        _activeSeg = new FileStream(segPath, FileMode.Create, FileAccess.ReadWrite, FileShare.Read);
        Span<byte> header = stackalloc byte[SegmentHeaderSize];
        SegmentMagic.CopyTo(header);
        BinaryPrimitives.WriteUInt16LittleEndian(header[4..], FormatVersion);
        BinaryPrimitives.WriteInt64LittleEndian(header[6..], firstIndex);
        BinaryPrimitives.WriteInt64LittleEndian(header[14..], prevTerm);
        _activeSeg.Write(header);
        _activeSeg.Flush(true);

        _activeIdx = new FileStream(idxPath, FileMode.Create, FileAccess.ReadWrite, FileShare.Read);
        Span<byte> idxHeader = stackalloc byte[IndexHeaderSize];
        IndexMagic.CopyTo(idxHeader);
        BinaryPrimitives.WriteUInt16LittleEndian(idxHeader[4..], FormatVersion);
        _activeIdx.Write(idxHeader);
        _activeIdx.Flush();

        _activeOffsets = [];
        _activePrevTerm = prevTerm;
        _segments.Add(new LogSegment
        {
            FirstIndex = firstIndex,
            LastIndex = firstIndex - 1,
            SegPath = segPath,
            IdxPath = idxPath,
            PrevTermCache = prevTerm
        });
        _firstSegmentIndex = _segments[0].FirstIndex;
    }

    /// <summary>Seals the active segment (fsync seg, rewrite idx with footer, fsync idx) and opens the next.</summary>
    private void RollActiveSegment()
    {
        LogSegment sealing = _segments[^1];
        _activeSeg.Flush(true);
        _activeSeg.Dispose();
        _activeIdx.Dispose();

        long[] sealedOffsets = [.. _activeOffsets];
        WriteIndexFile(sealing.IdxPath, sealedOffsets, sealedOffsets.Length, sealedFooter: true, fsync: true);
        CacheSealedOffsets(sealing.FirstIndex, sealedOffsets);

        CreateActiveSegment(Volatile.Read(ref _lastIndex) + 1, Volatile.Read(ref _lastTerm));
    }

    private static void WriteIndexFile(string idxPath, IReadOnlyList<long> offsets, int count, bool sealedFooter, bool fsync)
    {
        using FileStream fs = new(idxPath, FileMode.Create, FileAccess.Write, FileShare.Read);
        byte[] buffer = new byte[IndexHeaderSize + (count * sizeof(long)) + (sealedFooter ? IndexFooterSize : 0)];
        IndexMagic.CopyTo(buffer);
        BinaryPrimitives.WriteUInt16LittleEndian(buffer.AsSpan(4), FormatVersion);
        for (int i = 0; i < count; i++)
            BinaryPrimitives.WriteInt64LittleEndian(buffer.AsSpan(IndexHeaderSize + (i * sizeof(long))), offsets[i]);
        if (sealedFooter)
        {
            BinaryPrimitives.WriteUInt64LittleEndian(buffer.AsSpan(buffer.Length - IndexFooterSize), IndexFooterMarker);
            BinaryPrimitives.WriteInt64LittleEndian(buffer.AsSpan(buffer.Length - 8), count);
        }
        fs.Write(buffer);
        fs.Flush(fsync);
    }

    // ------------------------------------------------------------------ sealed offset cache

    private long[] GetSealedOffsets(LogSegment segment)
    {
        if (_sealedOffsetsCache.TryGetValue(segment.FirstIndex, out long[]? cached))
        {
            _sealedOffsetsLru.Remove(segment.FirstIndex);
            _sealedOffsetsLru.Add(segment.FirstIndex);
            return cached;
        }

        long count = segment.LastIndex - segment.FirstIndex + 1;
        long[] offsets = new long[count];
        using (FileStream fs = new(segment.IdxPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
        {
            byte[] buffer = new byte[count * sizeof(long)];
            fs.Position = IndexHeaderSize;
            fs.ReadExactly(buffer, 0, buffer.Length);
            for (int i = 0; i < offsets.Length; i++)
                offsets[i] = BinaryPrimitives.ReadInt64LittleEndian(buffer.AsSpan(i * sizeof(long)));
        }

        CacheSealedOffsets(segment.FirstIndex, offsets);
        return offsets;
    }

    private void CacheSealedOffsets(long firstIndex, long[] offsets)
    {
        _sealedOffsetsCache[firstIndex] = offsets;
        _sealedOffsetsLru.Remove(firstIndex);
        _sealedOffsetsLru.Add(firstIndex);
        while (_sealedOffsetsLru.Count > SealedOffsetsCacheCapacity)
        {
            _sealedOffsetsCache.Remove(_sealedOffsetsLru[0]);
            _sealedOffsetsLru.RemoveAt(0);
        }
    }

    private void EvictSealedOffsets(long firstIndex)
    {
        _sealedOffsetsCache.Remove(firstIndex);
        _sealedOffsetsLru.Remove(firstIndex);
    }

    // ------------------------------------------------------------------ append

    public async Task AppendAsync(IEnumerable<RaftLogEntry> entries, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entries);
        ObjectDisposedException.ThrowIf(_disposed, this);

        await _lock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            AppendUnderLock(entries, cancellationToken);
        }
        finally
        {
            _lock.Release();
        }
    }

    private void AppendUnderLock(IEnumerable<RaftLogEntry> entries, CancellationToken cancellationToken)
    {
        long rollbackLength = _activeSeg.Length;
        int rollbackOffsetCount = _activeOffsets.Count;
        long rollbackLastIndex = _lastIndex;
        long rollbackLastTerm = _lastTerm;
        List<long> batchOffsets = [];
        long batchLastIndex = _lastIndex;
        long batchLastTerm = _lastTerm;
        bool wroteAny = false;

        try
        {
            _activeSeg.Seek(0, SeekOrigin.End);
            byte[] frameHeader = new byte[FrameHeaderSize];
            byte[] trailer = new byte[FrameCrcSize];

            foreach (RaftLogEntry entry in entries)
            {
                cancellationToken.ThrowIfCancellationRequested();
                long offset = _activeSeg.Position;

                BinaryPrimitives.WriteUInt32LittleEndian(frameHeader.AsSpan(0), (uint)entry.Command.Length);
                BinaryPrimitives.WriteInt64LittleEndian(frameHeader.AsSpan(4), entry.Term);
                BinaryPrimitives.WriteInt64LittleEndian(frameHeader.AsSpan(12), entry.Index);
                uint crc = Crc32.Finish(Crc32.Append(Crc32.Append(Crc32.Begin(), frameHeader.AsSpan(4, 16)), entry.Command.Span));
                BinaryPrimitives.WriteUInt32LittleEndian(trailer, crc);

                _activeSeg.Write(frameHeader, 0, FrameHeaderSize);
                _activeSeg.Write(entry.Command.Span);
                _activeSeg.Write(trailer, 0, FrameCrcSize);

                batchOffsets.Add(offset);
                batchLastIndex = entry.Index;
                batchLastTerm = entry.Term;
                wroteAny = true;
            }

            if (!wroteAny) return;

            _activeSeg.Flush(_options.FsyncOnAppend);

            byte[] offsetBytes = new byte[sizeof(long)];
            foreach (long offset in batchOffsets)
            {
                _activeOffsets.Add(offset);
                BinaryPrimitives.WriteInt64LittleEndian(offsetBytes, offset);
                _activeIdx.Write(offsetBytes, 0, sizeof(long));
            }

            Volatile.Write(ref _lastIndex, batchLastIndex);
            Volatile.Write(ref _lastTerm, batchLastTerm);
            _activeIdx.Flush();
            _segments[^1].LastIndex = _lastIndex;
        }
        catch
        {
            _activeSeg.SetLength(rollbackLength);
            _activeSeg.Flush(true);
            if (_activeOffsets.Count > rollbackOffsetCount)
                _activeOffsets.RemoveRange(rollbackOffsetCount, _activeOffsets.Count - rollbackOffsetCount);
            Volatile.Write(ref _lastIndex, rollbackLastIndex);
            Volatile.Write(ref _lastTerm, rollbackLastTerm);
            throw;
        }

        if (_activeSeg.Length >= _options.SegmentBytes)
            RollActiveSegment();
    }

    // ------------------------------------------------------------------ reads

    public async Task<RaftLogEntry?> GetEntryAsync(long index, CancellationToken cancellationToken = default)
    {
        if (index <= 0) return null;

        await _lock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (index > _lastIndex || index < _segments[0].FirstIndex) return null;
            List<RaftLogEntry> result = new(1);
            ReadRangeUnderLock(index, index, result, cancellationToken);
            return result[0];
        }
        finally
        {
            _lock.Release();
        }
    }

    public async IAsyncEnumerable<RaftLogEntry> GetEntriesAsync(long startIndex, int count,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        List<RaftLogEntry> result = [];
        await _lock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            long end = count > 0 ? Math.Min(_lastIndex, startIndex + count - 1) : 0;
            if (startIndex > 0 && startIndex >= _segments[0].FirstIndex && startIndex <= end)
                ReadRangeUnderLock(startIndex, end, result, cancellationToken);
        }
        finally
        {
            _lock.Release();
        }

        foreach (RaftLogEntry entry in result)
            yield return entry;
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<RaftLogEntry> GetEntriesReverseAsync(long fromIndexInclusive, int count,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        List<RaftLogEntry> result = [];
        await _lock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            long from = Math.Min(fromIndexInclusive, _lastIndex);
            long first = Math.Max(_segments[0].FirstIndex, 1);
            if (count > 0 && _lastIndex > 0 && from >= first)
            {
                long to = Math.Max(first, from - count + 1);
                ReadRangeUnderLock(to, from, result, cancellationToken);
                result.Reverse();
            }
        }
        finally
        {
            _lock.Release();
        }

        foreach (RaftLogEntry entry in result)
            yield return entry;
    }

    /// <summary>Reads entries [start, end] (both within the retained range) into <paramref name="result"/>.</summary>
    private void ReadRangeUnderLock(long start, long end, List<RaftLogEntry> result, CancellationToken cancellationToken)
    {
        long index = start;
        while (index <= end)
        {
            LogSegment segment = FindSegment(index)
                ?? throw new InvalidDataException($"No segment covers log index {index}.");
            long segmentEnd = Math.Min(end, segment.LastIndex);
            bool isActive = ReferenceEquals(segment, _segments[^1]);

            if (isActive)
            {
                for (; index <= segmentEnd; index++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    result.Add(ReadFrame(_activeSeg, _activeOffsets[(int)(index - segment.FirstIndex)]));
                }
            }
            else
            {
                long[] offsets = GetSealedOffsets(segment);
                using FileStream fs = new(segment.SegPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                for (; index <= segmentEnd; index++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    int offsetIndex = (int)(index - segment.FirstIndex);
                    if (offsetIndex >= offsets.Length)
                        throw new InvalidDataException($"Segment '{segment.SegPath}' is damaged: no offset for index {index}.");
                    result.Add(ReadFrame(fs, offsets[offsetIndex]));
                }
            }
        }
    }

    private static RaftLogEntry ReadFrame(FileStream fs, long offset)
    {
        fs.Position = offset;
        Span<byte> header = stackalloc byte[FrameHeaderSize];
        fs.ReadExactly(header);
        uint length = BinaryPrimitives.ReadUInt32LittleEndian(header);
        long term = BinaryPrimitives.ReadInt64LittleEndian(header[4..]);
        long index = BinaryPrimitives.ReadInt64LittleEndian(header[12..]);

        if (length > int.MaxValue - FrameHeaderSize - FrameCrcSize
            || offset + FrameHeaderSize + length + FrameCrcSize > fs.Length)
        {
            throw new InvalidDataException($"Corrupt frame length at offset {offset} in '{fs.Name}'.");
        }

        byte[] payload = new byte[length];
        fs.ReadExactly(payload, 0, (int)length);
        Span<byte> trailer = stackalloc byte[FrameCrcSize];
        fs.ReadExactly(trailer);

        uint expectedCrc = BinaryPrimitives.ReadUInt32LittleEndian(trailer);
        uint actualCrc = Crc32.Finish(Crc32.Append(Crc32.Append(Crc32.Begin(), header[4..]), payload));
        if (actualCrc != expectedCrc)
            throw new InvalidDataException($"CRC mismatch for log entry {index} at offset {offset} in '{fs.Name}'.");

        return new RaftLogEntry { Term = term, Index = index, Command = payload };
    }

    private static long ReadFrameTerm(FileStream fs, long offset) => ReadFrame(fs, offset).Term;

    private LogSegment? FindSegment(long index)
    {
        int lo = 0;
        int hi = _segments.Count - 1;
        while (lo <= hi)
        {
            int mid = lo + ((hi - lo) / 2);
            LogSegment segment = _segments[mid];
            if (index < segment.FirstIndex) hi = mid - 1;
            else if (index > segment.LastIndex) lo = mid + 1;
            else return segment;
        }
        return null;
    }

    public async Task<long> GetTermAtIndexAsync(long index, CancellationToken cancellationToken = default)
    {
        if (index <= 0) return 0L;

        await _lock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (index > _lastIndex) return 0L;

            long first = _segments[0].FirstIndex;
            if (index == first - 1) return GetSegmentPrevTermUnderLock(_segments[0]);
            if (index < first) return 0L;

            LogSegment segment = FindSegment(index)!;
            if (ReferenceEquals(segment, _segments[^1]))
                return ReadFrameTerm(_activeSeg, _activeOffsets[(int)(index - segment.FirstIndex)]);

            long[] offsets = GetSealedOffsets(segment);
            int offsetIndex = (int)(index - segment.FirstIndex);
            if (offsetIndex >= offsets.Length)
                throw new InvalidDataException($"Segment '{segment.SegPath}' is damaged: no offset for index {index}.");
            using FileStream fs = new(segment.SegPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            return ReadFrameTerm(fs, offsets[offsetIndex]);
        }
        finally
        {
            _lock.Release();
        }
    }

    private long GetSegmentPrevTermUnderLock(LogSegment segment)
    {
        if (ReferenceEquals(segment, _segments[^1])) return _activePrevTerm;
        if (segment.PrevTermCache is long cached) return cached;

        // Lazy 22-byte header read of a sealed segment (never done at startup).
        long prevTerm = TryReadSegmentHeader(segment.SegPath, out (long FirstIndex, long PrevTerm) header)
            ? header.PrevTerm
            : 0L;
        segment.PrevTermCache = prevTerm;
        return prevTerm;
    }

    // ------------------------------------------------------------------ truncate / compact

    public async Task TruncateFromAsync(long index, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await _lock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            TruncateFromUnderLock(index);
        }
        finally
        {
            _lock.Release();
        }
    }

    private void TruncateFromUnderLock(long index)
    {
        if (index > _lastIndex) return;

        if (index <= _segments[0].FirstIndex)
        {
            // Clear everything and start over from index 1.
            _activeSeg.Dispose();
            _activeIdx.Dispose();
            foreach (LogSegment segment in _segments)
            {
                File.Delete(segment.SegPath);
                DeleteIfExists(segment.IdxPath);
            }
            _segments.Clear();
            _sealedOffsetsCache.Clear();
            _sealedOffsetsLru.Clear();
            CreateActiveSegment(firstIndex: 1, prevTerm: 0);
            Volatile.Write(ref _lastIndex, 0);
            Volatile.Write(ref _lastTerm, 0);
            return;
        }

        // Drop whole suffix segments whose first index is at or past the truncation point.
        bool activeDeleted = false;
        while (_segments[^1].FirstIndex >= index)
        {
            LogSegment doomed = _segments[^1];
            if (!activeDeleted)
            {
                _activeSeg.Dispose();
                _activeIdx.Dispose();
                activeDeleted = true;
            }
            _segments.RemoveAt(_segments.Count - 1);
            EvictSealedOffsets(doomed.FirstIndex);
            File.Delete(doomed.SegPath);
            DeleteIfExists(doomed.IdxPath);
        }

        LogSegment boundary = _segments[^1]; // boundary.FirstIndex < index
        int keepCount = (int)(index - boundary.FirstIndex);

        if (!activeDeleted)
        {
            // The boundary segment is still the open active segment; truncate it in place.
            long newLength = _activeOffsets[keepCount];
            _activeSeg.SetLength(newLength);
            _activeSeg.Flush(true);
            _activeOffsets.RemoveRange(keepCount, _activeOffsets.Count - keepCount);
            _activeIdx.Dispose();
            WriteIndexFile(boundary.IdxPath, _activeOffsets, _activeOffsets.Count, sealedFooter: false, fsync: false);
            _activeIdx = new FileStream(boundary.IdxPath, FileMode.Open, FileAccess.ReadWrite, FileShare.Read);
            _activeIdx.Position = _activeIdx.Length;
        }
        else
        {
            // Reopen the (previously sealed) boundary segment as the active one.
            long[] offsets = GetSealedOffsets(boundary);
            EvictSealedOffsets(boundary.FirstIndex);
            keepCount = Math.Min(keepCount, offsets.Length);

            _activeSeg = new FileStream(boundary.SegPath, FileMode.Open, FileAccess.ReadWrite, FileShare.Read);
            if (keepCount < offsets.Length)
            {
                _activeSeg.SetLength(offsets[keepCount]);
                _activeSeg.Flush(true);
            }
            _activeOffsets = [.. offsets[..keepCount]];
            _activePrevTerm = GetSegmentPrevTermFromHeader(boundary);
            WriteIndexFile(boundary.IdxPath, _activeOffsets, _activeOffsets.Count, sealedFooter: false, fsync: false);
            _activeIdx = new FileStream(boundary.IdxPath, FileMode.Open, FileAccess.ReadWrite, FileShare.Read);
            _activeIdx.Position = _activeIdx.Length;
        }

        Volatile.Write(ref _lastIndex, index - 1);
        boundary.LastIndex = index - 1;
        Volatile.Write(ref _lastTerm, _activeOffsets.Count == 0
            ? _activePrevTerm
            : ReadFrameTerm(_activeSeg, _activeOffsets[^1]));
    }

    private static long GetSegmentPrevTermFromHeader(LogSegment segment)
    {
        if (segment.PrevTermCache is long cached) return cached;
        long prevTerm = TryReadSegmentHeader(segment.SegPath, out (long FirstIndex, long PrevTerm) header) ? header.PrevTerm : 0L;
        segment.PrevTermCache = prevTerm;
        return prevTerm;
    }

    /// <inheritdoc />
    public async Task CompactToAsync(long index, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await _lock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // Drop whole prefix segments; never split, never drop the active segment.
            while (_segments.Count > 1 && _segments[0].LastIndex < index)
            {
                LogSegment doomed = _segments[0];
                _segments.RemoveAt(0);
                EvictSealedOffsets(doomed.FirstIndex);
                File.Delete(doomed.SegPath);
                DeleteIfExists(doomed.IdxPath);
            }
            Volatile.Write(ref _firstSegmentIndex, _segments[0].FirstIndex);
        }
        finally
        {
            _lock.Release();
        }
    }

    // ------------------------------------------------------------------ hard state / meta

    /// <inheritdoc />
    public (long Term, string? VotedFor) LoadHardState()
    {
        lock (_metaLock)
        {
            return (_hardStateTerm, _hardStateVotedFor);
        }
    }

    /// <inheritdoc />
    public Task SaveHardStateAsync(long term, string? votedFor, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        lock (_metaLock)
        {
            _hardStateTerm = term;
            _hardStateVotedFor = votedFor;
            SaveMetaLocked(fsync: true);
        }
        return Task.CompletedTask;
    }

    private void SaveMetaLocked(bool fsync)
    {
        long commitIndex = Volatile.Read(ref _commitIndex);
        RaftMeta.Save(_metaPath, new RaftMetaState(commitIndex, _hardStateTerm, _hardStateVotedFor), fsync);
        _persistedCommitIndex = commitIndex;
    }

    private void ScheduleCommitIndexFlush()
    {
        if (_disposed) return;
        if (Interlocked.CompareExchange(ref _commitFlushScheduled, 1, 0) != 0) return;
        _ = FlushCommitIndexAfterDelayAsync();
    }

    private async Task FlushCommitIndexAfterDelayAsync()
    {
        await Task.Delay(_options.CommitIndexFlushInterval).ConfigureAwait(false);
        Interlocked.Exchange(ref _commitFlushScheduled, 0);
        if (_disposed) return;
        try
        {
            lock (_metaLock)
            {
                if (!_disposed && Volatile.Read(ref _commitIndex) != _persistedCommitIndex)
                    SaveMetaLocked(fsync: false);
            }
        }
        catch (IOException)
        {
            // Best effort: a lagging persisted commit index is safe.
        }
        catch (UnauthorizedAccessException)
        {
        }
    }

    // ------------------------------------------------------------------ dispose

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path)) File.Delete(path);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        lock (_metaLock)
        {
            if (Volatile.Read(ref _commitIndex) != _persistedCommitIndex)
            {
                try
                {
                    SaveMetaLocked(fsync: true);
                }
                catch (IOException)
                {
                }
                catch (UnauthorizedAccessException)
                {
                }
            }
        }

        _activeSeg?.Dispose();
        _activeIdx?.Dispose();
        _lock.Dispose();
    }
}
