// IonStream License 1.0
// Copyright (c) 2026 SCHIPPER.IO LLC - All Rights Reserved
// Licensor: SCHIPPER.IO LLC - Contact: contact@schipper.io

using System.Buffers;
using System.Buffers.Binary;
using Schipper.Io.Raft.Protocol;

namespace Schipper.Io.Raft.Storage;

public class FileRaftLog : IRaftLog, IDisposable
{
    private readonly string _path;
    private readonly FileStream _file;
    private readonly List<long> _entryPositions = []; // Index -> FilePosition. Index 1 is at _entryPositions[0]
    private readonly SemaphoreSlim _lock = new(1, 1);

    public long LastIndex => _entryPositions.Count;
    public long LastTerm { get; private set; }
    public long CommitIndex { get; set; }

    public FileRaftLog(string directory)
    {
        if (!Directory.Exists(directory)) Directory.CreateDirectory(directory);
        _path = Path.Combine(directory, "raft.log");
        // FileShare.Read allows concurrent readers (e.g. diagnostics tools) but means a reader
        // may observe a partial write if it reads between the header and payload flush.
        _file = new FileStream(_path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);

        Recover();
    }

    private void Recover()
    {
        _file.Position = 0;
        _entryPositions.Clear();
        LastTerm = 0;

        Span<byte> buffer = stackalloc byte[20]; // Term(8) + Index(8) + Len(4)
        long lastValidPosition = 0;

        while (_file.Position < _file.Length)
        {
            long pos = _file.Position;
            if (_file.Length - pos < 20) break; // Incomplete header

            _file.ReadExactly(buffer);

            long term = BinaryPrimitives.ReadInt64LittleEndian(buffer);
            int len = BinaryPrimitives.ReadInt32LittleEndian(buffer[16..]);

            // Validate entry: len must be non-negative and within file bounds
            if (len < 0 || _file.Position + len > _file.Length) break; // Incomplete or corrupt data

            _entryPositions.Add(pos);
            LastTerm = term;
            _file.Position += len;
            lastValidPosition = _file.Position;
        }

        // Truncate any partial writes - use tracked valid position
        // to ensure _entryPositions and file length are consistent
        if (lastValidPosition != _file.Length)
        {
            _file.SetLength(lastValidPosition);
            _file.Flush(true);
        }
    }

    public async Task AppendAsync(IEnumerable<RaftLogEntry> entries, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entries);

        await _lock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            long rollbackLength = _file.Length;
            int rollbackCount = _entryPositions.Count;
            long rollbackTerm = LastTerm;
            List<long> batchPositions = [];
            long batchTerm = LastTerm;
            try
            {
                _file.Seek(0, SeekOrigin.End);
                byte[] header = new byte[20];
                foreach (RaftLogEntry entry in entries)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    batchPositions.Add(_file.Position);

                    BinaryPrimitives.WriteInt64LittleEndian(header.AsSpan(0, 8), entry.Term);
                    BinaryPrimitives.WriteInt64LittleEndian(header.AsSpan(8, 8), entry.Index);
                    BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(16, 4), entry.Command.Length);

                    await _file.WriteAsync(header.AsMemory(0, 20), cancellationToken).ConfigureAwait(false);
                    await _file.WriteAsync(entry.Command, cancellationToken).ConfigureAwait(false);

                    batchTerm = entry.Term;
                }

                _file.Flush(true);
                _entryPositions.AddRange(batchPositions);
                LastTerm = batchTerm;
            }
            catch
            {
                _file.SetLength(rollbackLength);
                _file.Flush(true);
                if (_entryPositions.Count > rollbackCount)
                    _entryPositions.RemoveRange(rollbackCount, _entryPositions.Count - rollbackCount);
                LastTerm = rollbackTerm;
                throw;
            }
        }
        finally
        {
            _lock.Release();
        }
    }

    public async Task<RaftLogEntry?> GetEntryAsync(long index, CancellationToken cancellationToken = default)
    {
        if (index <= 0 || index > _entryPositions.Count) return null;

        await _lock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            long pos = _entryPositions[(int)index - 1];
            _file.Seek(pos, SeekOrigin.Begin);

            byte[] header = ArrayPool<byte>.Shared.Rent(20);
            try
            {
                await _file.ReadExactlyAsync(header.AsMemory(0, 20), cancellationToken).ConfigureAwait(false);

                long term = BinaryPrimitives.ReadInt64LittleEndian(header.AsSpan(0, 8));
                long idx = BinaryPrimitives.ReadInt64LittleEndian(header.AsSpan(8, 8));
                int len = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(16, 4));

                byte[] data = new byte[len];
                await _file.ReadExactlyAsync(data, 0, len, cancellationToken).ConfigureAwait(false);

                return new RaftLogEntry
                {
                    Term = term,
                    Index = idx,
                    Command = data
                };
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(header);
            }
        }
        finally
        {
            _lock.Release();
        }
    }

    public async IAsyncEnumerable<RaftLogEntry> GetEntriesAsync(long startIndex, int count,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        List<RaftLogEntry> result = [];
        await _lock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            byte[] header = ArrayPool<byte>.Shared.Rent(20);
            try
            {
                for (int i = 0; i < count; i++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    long index = startIndex + i;
                    if (index > _entryPositions.Count) break;

                    long pos = _entryPositions[(int)index - 1];
                    _file.Seek(pos, SeekOrigin.Begin);

                    await _file.ReadExactlyAsync(header.AsMemory(0, 20), cancellationToken).ConfigureAwait(false);

                    long term = BinaryPrimitives.ReadInt64LittleEndian(header.AsSpan(0, 8));
                    long idx = BinaryPrimitives.ReadInt64LittleEndian(header.AsSpan(8, 8));
                    int len = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(16, 4));

                    byte[] data = new byte[len];
                    await _file.ReadExactlyAsync(data, 0, len, cancellationToken).ConfigureAwait(false);

                    result.Add(new RaftLogEntry
                    {
                        Term = term,
                        Index = idx,
                        Command = data
                    });
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(header);
            }
        }
        finally
        {
            _lock.Release();
        }

        foreach (RaftLogEntry entry in result)
            yield return entry;
    }

    public async Task TruncateFromAsync(long index, CancellationToken cancellationToken = default)
    {
        await _lock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (index > _entryPositions.Count) return;

            // If index is 1, we clear everything
            if (index <= 1)
            {
                _file.SetLength(0);
                _file.Flush(true);
                _entryPositions.Clear();
                LastTerm = 0;
                return;
            }

            int keepCount = (int)index - 1;
            long newLength = _entryPositions[keepCount]; // The position of the entry we are about to delete

            _file.SetLength(newLength);
            _file.Flush(true);
            _entryPositions.RemoveRange(keepCount, _entryPositions.Count - keepCount);

            // Update LastTerm (keepCount >= 1 is guaranteed since index > 1)
            long lastPos = _entryPositions[^1];
            _file.Seek(lastPos, SeekOrigin.Begin);
            byte[] termBytes = ArrayPool<byte>.Shared.Rent(8);
            try
            {
                await _file.ReadExactlyAsync(termBytes.AsMemory(0, 8), cancellationToken).ConfigureAwait(false);
                LastTerm = BinaryPrimitives.ReadInt64LittleEndian(termBytes.AsSpan(0, 8));
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(termBytes);
            }
        }
        finally
        {
            _lock.Release();
        }
    }

    public async Task<long> GetTermAtIndexAsync(long index, CancellationToken cancellationToken = default)
    {
        if (index == 0) return 0L;
        if (index > _entryPositions.Count) return 0L;

        await _lock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            long pos = _entryPositions[(int)index - 1];
            _file.Seek(pos, SeekOrigin.Begin);
            byte[] termBytes = ArrayPool<byte>.Shared.Rent(8);
            try
            {
                await _file.ReadExactlyAsync(termBytes.AsMemory(0, 8), cancellationToken).ConfigureAwait(false);
                return BinaryPrimitives.ReadInt64LittleEndian(termBytes.AsSpan(0, 8));
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(termBytes);
            }
        }
        finally
        {
            _lock.Release();
        }
    }

    protected virtual void Dispose(bool disposing)
    {
        if (disposing)
        {
            _file.Dispose();
            _lock.Dispose();
        }
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }
}
