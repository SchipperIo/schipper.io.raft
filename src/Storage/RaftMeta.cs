// IonStream License 1.0
// Copyright (c) 2026 SCHIPPER.IO LLC - All Rights Reserved
// Licensor: SCHIPPER.IO LLC - Contact: contact@schipper.io

using System.Buffers.Binary;
using System.Text;

namespace Schipper.Io.Raft.Storage;

/// <summary>
/// Snapshot of the state persisted in <c>meta.bin</c>: the debounced commit index plus the Raft
/// hard state (current term and vote).
/// </summary>
internal readonly record struct RaftMetaState(long CommitIndex, long Term, string? VotedFor);

/// <summary>
/// Codec for <c>meta.bin</c>. Layout: magic "RMET"(4) | version u16 | commitIndex u64 | term u64 |
/// votedFor as u16 length-prefixed UTF-8 (0xFFFF = null) | crc32 over everything after the magic.
/// Written atomically (tmp file + move). A fsynced save keeps <c>meta.bin.prev</c> as the last
/// durable generation so a later unsynced commit-index rewrite cannot erase the vote. Load uses
/// that previous file when <c>meta.bin</c> is missing or corrupt.
/// </summary>
internal static class RaftMeta
{
    private const ushort FormatVersion = 1;
    private const ushort NullVotedForMarker = 0xFFFF;
    private const int FixedSize = 4 + 2 + 8 + 8 + 2 + 4; // magic + ver + commit + term + len + crc

    private static ReadOnlySpan<byte> Magic => "RMET"u8;

    /// <summary>Test hook invoked after <c>Flush</c> on the temp file and before replace.</summary>
    internal static Action<bool, string>? AfterNonFsyncFlush;

    public static void Save(string path, RaftMetaState state, bool fsync)
    {
        byte[] votedForBytes = state.VotedFor is null ? [] : Encoding.UTF8.GetBytes(state.VotedFor);
        if (votedForBytes.Length >= NullVotedForMarker)
            throw new ArgumentException("VotedFor is too long to persist.", nameof(state));

        byte[] data = new byte[FixedSize + votedForBytes.Length];
        Magic.CopyTo(data);
        BinaryPrimitives.WriteUInt16LittleEndian(data.AsSpan(4), FormatVersion);
        BinaryPrimitives.WriteInt64LittleEndian(data.AsSpan(6), state.CommitIndex);
        BinaryPrimitives.WriteInt64LittleEndian(data.AsSpan(14), state.Term);
        BinaryPrimitives.WriteUInt16LittleEndian(data.AsSpan(22),
            state.VotedFor is null ? NullVotedForMarker : (ushort)votedForBytes.Length);
        votedForBytes.CopyTo(data.AsSpan(24));
        uint crc = Crc32.Compute(data.AsSpan(4, data.Length - 8));
        BinaryPrimitives.WriteUInt32LittleEndian(data.AsSpan(data.Length - 4), crc);

        string tmpPath = path + ".tmp";
        string prevPath = path + ".prev";
        using (FileStream tmp = new(tmpPath, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            tmp.Write(data);
            tmp.Flush(fsync);
        }

        AfterNonFsyncFlush?.Invoke(fsync, path);

        if (fsync)
        {
            File.Move(tmpPath, path, overwrite: true);
            FsyncDirectory(path);
            File.Copy(path, prevPath, overwrite: true);
            using FileStream prev = new(prevPath, FileMode.Open, FileAccess.ReadWrite, FileShare.Read);
            prev.Flush(true);
        }
        else
        {
            File.Move(tmpPath, path, overwrite: true);
        }
    }

    public static RaftMetaState Load(string path)
    {
        if (TryLoad(path, out RaftMetaState state))
            return state;
        if (TryLoad(path + ".prev", out state))
            return state;
        return default;
    }

    private static bool TryLoad(string path, out RaftMetaState state)
    {
        state = default;
        byte[] data;
        try
        {
            data = File.ReadAllBytes(path);
        }
        catch (IOException)
        {
            return false;
        }
        catch (UnauthorizedAccessException)
        {
            return false;
        }

        if (data.Length < FixedSize || !data.AsSpan(0, 4).SequenceEqual(Magic))
            return false;

        uint storedCrc = BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(data.Length - 4));
        if (Crc32.Compute(data.AsSpan(4, data.Length - 8)) != storedCrc)
            return false;

        ushort version = BinaryPrimitives.ReadUInt16LittleEndian(data.AsSpan(4));
        if (version != FormatVersion)
            return false;

        long commitIndex = BinaryPrimitives.ReadInt64LittleEndian(data.AsSpan(6));
        long term = BinaryPrimitives.ReadInt64LittleEndian(data.AsSpan(14));
        ushort votedForLength = BinaryPrimitives.ReadUInt16LittleEndian(data.AsSpan(22));

        string? votedFor = null;
        if (votedForLength != NullVotedForMarker)
        {
            if (data.Length != FixedSize + votedForLength)
                return false;
            votedFor = Encoding.UTF8.GetString(data, 24, votedForLength);
        }
        else if (data.Length != FixedSize)
        {
            return false;
        }

        state = new RaftMetaState(commitIndex, term, votedFor);
        return true;
    }

    private static void FsyncDirectory(string filePath)
    {
        string? dir = Path.GetDirectoryName(filePath);
        if (string.IsNullOrEmpty(dir)) return;
        try
        {
            using FileStream directory = new(dir, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            directory.Flush(true);
        }
        catch (IOException)
        {
        }
        catch (UnauthorizedAccessException)
        {
        }
        catch (PlatformNotSupportedException)
        {
        }
        catch (ArgumentException)
        {
        }
    }
}

/// <summary>
/// Table-based CRC-32 (ISO-HDLC: reflected polynomial 0xEDB88320, init/xorout 0xFFFFFFFF),
/// matching the common crc32 used by zip/png.
/// </summary>
internal static class Crc32
{
    private static readonly uint[] Table = BuildTable();

    private static uint[] BuildTable()
    {
        uint[] table = new uint[256];
        for (uint i = 0; i < 256; i++)
        {
            uint c = i;
            for (int k = 0; k < 8; k++)
                c = (c & 1) != 0 ? 0xEDB88320u ^ (c >> 1) : c >> 1;
            table[i] = c;
        }
        return table;
    }

    public static uint Begin() => 0xFFFFFFFFu;

    public static uint Append(uint crc, ReadOnlySpan<byte> data)
    {
        foreach (byte b in data)
            crc = Table[(crc ^ b) & 0xFF] ^ (crc >> 8);
        return crc;
    }

    public static uint Finish(uint crc) => crc ^ 0xFFFFFFFFu;

    public static uint Compute(ReadOnlySpan<byte> data) => Finish(Append(Begin(), data));
}
