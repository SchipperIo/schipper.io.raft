// IonStream License 1.0
// Copyright (c) 2026 SCHIPPER.IO LLC - All Rights Reserved
// Licensor: SCHIPPER.IO LLC - Contact: contact@schipper.io

namespace Schipper.Io.Raft.Storage;

/// <summary>
/// In-memory descriptor for one on-disk log segment (a <c>.seg</c> data file plus its <c>.idx</c>
/// offset sidecar). Only <see cref="FirstIndex"/>/<see cref="LastIndex"/> and the file paths are
/// kept resident; sealed segments' offset arrays load lazily on demand.
/// </summary>
internal sealed class LogSegment
{
    /// <summary>Index of the first entry stored in this segment (from the file name).</summary>
    public required long FirstIndex { get; init; }

    /// <summary>Index of the last entry in this segment; <c>FirstIndex - 1</c> when empty.</summary>
    public long LastIndex { get; set; }

    /// <summary>Absolute path of the <c>.seg</c> data file.</summary>
    public required string SegPath { get; init; }

    /// <summary>Absolute path of the <c>.idx</c> offset sidecar.</summary>
    public required string IdxPath { get; init; }

    /// <summary>
    /// Cached <c>prevTerm</c> from the segment header (term of entry <c>FirstIndex - 1</c>;
    /// 0 for the first segment ever created). Read lazily for sealed segments.
    /// </summary>
    public long? PrevTermCache { get; set; }
}
