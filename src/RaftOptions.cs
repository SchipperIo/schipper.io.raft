// IonStream License 1.0
// Copyright (c) 2026 SCHIPPER.IO LLC - All Rights Reserved
// Licensor: SCHIPPER.IO LLC - Contact: contact@schipper.io

namespace Schipper.Io.Raft;

public class RaftOptions
{
    public int ElectionTimeoutMinMs { get; set; } = 3000;
    public int ElectionTimeoutMaxMs { get; set; } = 6000;
    public int HeartbeatIntervalMs { get; set; } = 300;

    /// <summary>
    /// Timeout for RequestVote RPC calls, in milliseconds.
    /// Keep this below the election timeout window to avoid long stalls.
    /// </summary>
    public int VoteRpcDeadlineMs { get; set; } = 1500;

    /// <summary>
    /// Timeout for AppendEntries RPC calls, in milliseconds.
    /// Keep this below the election timeout window to avoid long stalls.
    /// </summary>
    public int AppendEntriesRpcDeadlineMs { get; set; } = 1500;

    /// <summary>
    /// Shared key for authentication between nodes.
    /// </summary>
    public string? AuthToken { get; set; }

    /// <summary>
    /// Initial delay before validating peer connections, in milliseconds.
    /// Gives servers time to start listening. Use 0 for testing.
    /// </summary>
    public int InitialValidationDelayMs { get; set; } = 1000;

    /// <summary>
    /// Delay between connection retry attempts, in milliseconds.
    /// </summary>
    public int ConnectionRetryDelayMs { get; set; } = 2000;

    /// <summary>
    /// Delay after error recovery in the main loop, in milliseconds.
    /// </summary>
    public int ErrorRecoveryDelayMs { get; set; } = 1000;

    /// <summary>
    /// Timeout for waiting on quorum connections during startup, in milliseconds.
    /// </summary>
    public int QuorumConnectionTimeoutMs { get; set; } = 5000;

    /// <summary>
    /// When true, skips connection validation entirely during startup.
    /// </summary>
    public bool SkipConnectionValidation { get; set; }

    /// <summary>
    /// Timeout for waiting on the loop task during StopAsync, in milliseconds.
    /// </summary>
    public int StopTimeoutMs { get; set; } = 5000;

    /// <summary>
    /// True when fast mode has been applied via <see cref="SetFastMode"/> or the <c>init</c> accessor.
    /// </summary>
    public bool FastMode { get; private set; }

    /// <summary>
    /// Applies fast mode: sets all timeout values to very low constants for testing.
    /// Supported in object initializers via the <c>init</c> accessor.
    /// </summary>
    public void SetFastMode(bool value)
    {
        FastMode = value;
        if (value)
        {
            ElectionTimeoutMinMs = 10;
            ElectionTimeoutMaxMs = 20;
            HeartbeatIntervalMs = 5;
            VoteRpcDeadlineMs = 50;
            AppendEntriesRpcDeadlineMs = 50;
            InitialValidationDelayMs = 0;
            ConnectionRetryDelayMs = 10;
            ErrorRecoveryDelayMs = 10;
            QuorumConnectionTimeoutMs = 100;
            SkipConnectionValidation = true;
        }
    }
}
