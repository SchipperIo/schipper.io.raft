// IonStream License 1.0
// Copyright (c) 2026 SCHIPPER.IO LLC - All Rights Reserved
// Licensor: SCHIPPER.IO LLC - Contact: contact@schipper.io

namespace Schipper.Io.Raft;

/// <summary>
/// Time source for election and heartbeat timers. Inject a fake in tests so a replay of the same
/// RPC trace elects the same way. Production uses wall-clock UTC and <see cref="Task.Delay(TimeSpan, CancellationToken)"/>.
/// </summary>
public interface IRaftClock
{
    DateTimeOffset UtcNow { get; }

    Task Delay(TimeSpan delay, CancellationToken cancellationToken = default);
}

internal sealed class SystemRaftClock : IRaftClock
{
    public DateTimeOffset UtcNow => DateTimeOffset.UtcNow;

    public Task Delay(TimeSpan delay, CancellationToken cancellationToken = default) =>
        Task.Delay(delay, cancellationToken);
}
