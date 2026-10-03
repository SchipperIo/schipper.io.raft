// IonStream License 1.0
// Copyright (c) 2026 SCHIPPER.IO LLC - All Rights Reserved
// Licensor: SCHIPPER.IO LLC - Contact: contact@schipper.io

using Microsoft.Extensions.Logging;
using Schipper.Io.Raft.Protocol;
using System.Diagnostics;
using System.Collections.Concurrent;
using System.Security.Cryptography;

namespace Schipper.Io.Raft;

public class LeaderChangedEventArgs(string? leaderId) : EventArgs
{
    public string? LeaderId { get; } = leaderId;
}

public enum RaftRole
{
    Follower,
    Candidate,
    Leader
}

public class RaftNode : IRaftNode
{
    internal readonly string _nodeId;
    private readonly IRaftTransport _transport;
    private readonly ConcurrentDictionary<string, string> _peers;
    private readonly ILogger<RaftNode> _logger;
    private readonly IRaftMetrics? _metrics;
    private readonly RaftOptions _options;
    private readonly IRaftClock _clock;
    private readonly Func<int, int, int> _drawElectionTimeoutMs;
    private readonly SemaphoreSlim _lock = new(1, 1);
    private readonly SemaphoreSlim _applyLock = new(1, 1);
    private LeaderChangedEventArgs? _pendingLeaderChanged;

    // Persistent state
    internal long _currentTerm;
    internal string? _votedFor;
    private readonly IRaftLog _raftLog;
    private readonly IRaftHardStateStore? _hardStateStore;

    private int _disposed;

    // Volatile state
    internal long _commitIndex;
    internal long _lastApplied;
    internal RaftRole _role = RaftRole.Follower;
    internal DateTimeOffset _lastHeartbeat;
    internal TimeSpan _currentElectionTimeout; // Add this field
    private DateTimeOffset _lastHeartbeatSendTime = DateTimeOffset.MinValue;
    internal bool _electionStarted;

    public string? LeaderId { get; internal set; }

    public string NodeId => _nodeId;
    public RaftRole Role => _role;
    public long CommitIndex => _commitIndex;

    public bool IsReady => LeaderId != null;

    // Leader state
    internal readonly Dictionary<string, long> _nextIndex = [];
    internal readonly Dictionary<string, long> _matchIndex = [];
    private readonly ConcurrentDictionary<string, DateTimeOffset> _peerLastContact = new();
    private readonly ConcurrentDictionary<string, string> _readonlyPeers = new();
    private readonly ConcurrentDictionary<string, Task> _inFlightHeartbeats = new();
    private readonly ConcurrentDictionary<string, bool> _peerConnected = new();
    private readonly ConcurrentDictionary<string, bool> _peersRequiringSeed = new();
    private volatile KeyValuePair<string, string>[]? _cachedAllPeers;

    public IEnumerable<string> GetUnhealthyPeers(TimeSpan threshold)
    {
        DateTimeOffset now = _clock.UtcNow;
        return _peers.Keys.Where(peerId =>
            _peerLastContact.TryGetValue(peerId, out DateTimeOffset last) &&
            now - last > threshold);
    }

    private readonly CancellationTokenSource _stopCts = new();
    private Task? _loopTask;

    public RaftNode(
        string nodeId,
        IDictionary<string, string> peers,
        IRaftTransport transport,
        IRaftLog raftLog,
        ILogger<RaftNode> logger,
        IRaftMetrics? metrics = null,
        RaftOptions? options = null,
        IRaftClock? clock = null,
        Func<int, int, int>? drawElectionTimeoutMs = null)
    {
        _nodeId = nodeId;
        _peers = new ConcurrentDictionary<string, string>(peers, StringComparer.Ordinal);
        _transport = transport;
        _raftLog = raftLog;
        _logger = logger;
        _metrics = metrics;
        _options = options ?? new RaftOptions();
        _clock = clock ?? new SystemRaftClock();
        _drawElectionTimeoutMs = drawElectionTimeoutMs ?? RandomNumberGenerator.GetInt32;

        // Seed durable hard state (term/votedFor) when the log can persist it, so a restarted
        // node never votes twice in the same term.
        if (raftLog is IRaftHardStateStore hardStateStore)
        {
            _hardStateStore = hardStateStore;
            (long term, string? votedFor) = hardStateStore.LoadHardState();
            _currentTerm = term;
            _votedFor = votedFor;
        }

        ResetElectionTimeout(); // Initialize
    }

    /// <summary>
    /// Persists the current term and vote when the log doubles as a hard-state store; no-op otherwise.
    /// Must be awaited before a term/vote change becomes externally visible.
    /// </summary>
    private Task PersistHardStateAsync(CancellationToken token) =>
        _hardStateStore?.SaveHardStateAsync(_currentTerm, _votedFor, token) ?? Task.CompletedTask;

    internal void ResetElectionTimeout()
    {
        _lastHeartbeat = _clock.UtcNow;
        int delay = _drawElectionTimeoutMs(_options.ElectionTimeoutMinMs, _options.ElectionTimeoutMaxMs);
        _currentElectionTimeout = TimeSpan.FromMilliseconds(delay);
    }

    public async Task StartAsync() => _loopTask = RunAsync(_stopCts.Token);

    private async Task ValidateConnectionsAsync(CancellationToken token)
    {
        if (_options.SkipConnectionValidation)
        {
            _logger.LogInformation("Skipping connection validation (SkipConnectionValidation=true)");
            return;
        }

        if (_peers.IsEmpty)
        {
            if (_logger.IsEnabled(LogLevel.Information))
            {
                _logger.LogInformation("No peers configured, skipping connection validation");
            }
            return;
        }

        if (_logger.IsEnabled(LogLevel.Information))
        {
            _logger.LogInformation("Starting connection validation for {PeerCount} peers: {Peers}",
                _peers.Count, string.Join(", ", _peers.Keys));
        }

        // Give servers a moment to start listening before we try to connect
        if (_options.InitialValidationDelayMs > 0)
        {
            await _clock.Delay(TimeSpan.FromMilliseconds(_options.InitialValidationDelayMs), token);
        }

        List<Task> tasks = [.. _peers.Keys.Select(async peerId =>
        {
            int attempt = 0;
            while (!token.IsCancellationRequested)
            {
                attempt++;
                try
                {
                    await _transport.ValidateConnectionAsync(peerId, token);
                    if (_logger.IsEnabled(LogLevel.Information))
                    {
                        _logger.LogInformation("Connection to peer {PeerId} validated (attempt {Attempt})", peerId, attempt);
                    }
                    break; // Success
                }
                catch (Exception ex)
                {
                    if (_logger.IsEnabled(LogLevel.Debug))
                    {
                        _logger.LogDebug(ex, "Failed to validate connection to peer {PeerId} (attempt {Attempt}). Retrying in {DelayMs}ms...",
                            peerId, attempt, _options.ConnectionRetryDelayMs);
                    }
                    await _clock.Delay(TimeSpan.FromMilliseconds(_options.ConnectionRetryDelayMs), token);
                }
            }
        })];

        // Wait for a quorum of connections (including self)
        int totalNodes = _peers.Count + 1;
        int quorum = (totalNodes / 2) + 1;
        int neededPeers = quorum - 1;

        if (neededPeers > 0)
        {
            if (_logger.IsEnabled(LogLevel.Information))
            {
                _logger.LogInformation("Waiting for {NeededPeers} of {TotalPeers} peer connections before starting elections (quorum: {Quorum}/{TotalNodes})...",
                    neededPeers, _peers.Count, quorum, totalNodes);
            }

            List<Task> pendingTasks = [.. tasks];
            int connected = 0;
            Stopwatch sw = Stopwatch.StartNew();
            using CancellationTokenSource timeoutCts = new(_options.QuorumConnectionTimeoutMs);
            using CancellationTokenSource linkedCts = CancellationTokenSource.CreateLinkedTokenSource(token, timeoutCts.Token);

            while (connected < neededPeers && pendingTasks.Count > 0 && !linkedCts.Token.IsCancellationRequested)
            {
                Task timeoutTask = _clock.Delay(Timeout.InfiniteTimeSpan, linkedCts.Token);
                Task completedTask = await Task.WhenAny([.. pendingTasks, timeoutTask]);

                if (completedTask == timeoutTask)
                {
                    if (_logger.IsEnabled(LogLevel.Warning))
                    {
                        _logger.LogWarning("Quorum connection timeout ({TimeoutMs}ms) reached with {Connected}/{Needed} connections",
                            _options.QuorumConnectionTimeoutMs, connected, neededPeers);
                    }
                    break;
                }

                pendingTasks.Remove(completedTask);

                if (completedTask.IsCompletedSuccessfully)
                {
                    connected++;
                    if (_logger.IsEnabled(LogLevel.Information))
                    {
                        _logger.LogInformation("Peer connection {Connected}/{Needed} established after {ElapsedMs}ms",
                            connected, neededPeers, sw.ElapsedMilliseconds);
                    }
                }
            }

            sw.Stop();

            if (connected >= neededPeers)
            {
                if (_logger.IsEnabled(LogLevel.Information))
                {
                    _logger.LogInformation("Quorum connections established ({Connected}/{Needed}) in {ElapsedSec}s. Starting elections.",
                        connected, neededPeers, sw.Elapsed.TotalSeconds);
                }
            }
            else
            {
                if (_logger.IsEnabled(LogLevel.Warning))
                {
                    _logger.LogWarning("Failed to establish quorum ({Connected}/{Needed}) within timeout. Proceeding anyway.",
                        connected, neededPeers);
                }
            }
        }
    }

    public async Task RunAsync(CancellationToken token)
    {
        // Validate connections on startup
        await ValidateConnectionsAsync(token);

        while (!token.IsCancellationRequested)
        {
            try
            {
                RaftRole currentRole;
                await CheckElectionTimeoutAsync(token);
                await _lock.WaitAsync(token);
                try
                {
                    currentRole = _role;
                }
                finally
                {
                    _lock.Release();
                }

                FlushLeaderChanged();

                switch (currentRole)
                {
                    case RaftRole.Candidate:
                        await RunElectionAsync(token);
                        break;
                    case RaftRole.Leader:
                        // Send periodic heartbeats to prevent follower election timeouts
                        if (_clock.UtcNow - _lastHeartbeatSendTime > TimeSpan.FromMilliseconds(_options.HeartbeatIntervalMs))
                        {
                            DispatchHeartbeats(token);
                        }
                        break;
                }

                // Poll at the heartbeat interval, or immediately in fast mode
                int delayMs = _options.FastMode ? 1 : _options.HeartbeatIntervalMs / 5;
                if (delayMs < 1) delayMs = 1;
                await _clock.Delay(TimeSpan.FromMilliseconds(delayMs), token);
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                if (_logger.IsEnabled(LogLevel.Error))
                {
                    _logger.LogError(ex, "Node {NodeId}: Error in Raft loop", _nodeId);
                }
                try { await _clock.Delay(TimeSpan.FromMilliseconds(_options.ErrorRecoveryDelayMs), token); } catch (OperationCanceledException) { break; }
            }
        }
    }

    internal async Task CheckElectionTimeoutAsync(CancellationToken token)
    {
        if (token.IsCancellationRequested) return;
        await _lock.WaitAsync(token);
        try
        {
            if (_role == RaftRole.Leader) return;
            if (_clock.UtcNow - _lastHeartbeat <= _currentElectionTimeout) return;
            if (_logger.IsEnabled(LogLevel.Information))
            {
                _logger.LogInformation("Node {NodeId}: Election timeout after {Elapsed}ms (timeout={Timeout}ms, leader={LeaderId}, term={Term}, host={Host}). Becoming Candidate.",
                    _nodeId,
                    (_clock.UtcNow - _lastHeartbeat).TotalMilliseconds,
                    _currentElectionTimeout.TotalMilliseconds,
                    LeaderId ?? "none",
                    _currentTerm,
                    Environment.MachineName);
            }
            _role = RaftRole.Candidate;
            _currentTerm++;
            _metrics?.SetRaftTerm(_currentTerm);
            _votedFor = _nodeId;
            await PersistHardStateAsync(token);
            SetLeader(null);
            ResetElectionTimeout();
            _electionStarted = false;
        }
        finally
        {
            _lock.Release();
        }

        FlushLeaderChanged();
    }

    internal async Task RunElectionAsync(CancellationToken token)
    {
        VoteRequest requestTemplate;
        long electionTerm;
        Stopwatch sw = Stopwatch.StartNew();
        bool wonElection = false;

        await _lock.WaitAsync(token);
        try
        {
            if (_electionStarted || _role != RaftRole.Candidate) return;
            _electionStarted = true;

            electionTerm = _currentTerm;
            requestTemplate = new VoteRequest
            {
                Term = electionTerm,
                CandidateId = _nodeId,
                LastLogIndex = _raftLog.LastIndex,
                LastLogTerm = _raftLog.LastTerm,
                AuthToken = _options.AuthToken
            };
        }
        finally
        {
            _lock.Release();
        }

        // Send RequestVote to all peers
        int votes = 1; // Self
        _metrics?.RecordRaftVoteReceived(); // Count self vote

        IEnumerable<Task<VoteResponse?>> tasks = _peers.Select(async peer =>
        {
            try
            {
                VoteResponse response = await _transport.RequestVoteAsync(peer.Key, requestTemplate, token);
                return response;
            }
            catch (Exception ex)
            {
                if (_logger.IsEnabled(LogLevel.Warning))
                {
                    _logger.LogWarning("Failed to request vote from {Peer}: {ErrorType} - {Message}", peer.Key, ex.GetType().Name, ex.Message);
                }
                return null;
            }
        });

        VoteResponse?[] responses = await Task.WhenAll(tasks);

        await _lock.WaitAsync(token);
        try
        {
            if (_role != RaftRole.Candidate) return;
            if (_currentTerm > electionTerm) return;

            foreach (VoteResponse? response in responses)
            {
                token.ThrowIfCancellationRequested();
                if (response != null && response.Authenticated)
                {
                    if (response.Term > _currentTerm)
                    {
                        if (_logger.IsEnabled(LogLevel.Information))
                        {
                            _logger.LogInformation("Node {NodeId}: Received higher term {Term} (mine: {MyTerm}) in election response from a peer. Stepping down.",
                                _nodeId, response.Term, _currentTerm);
                        }
                        _currentTerm = response.Term;
                        _metrics?.SetRaftTerm(_currentTerm);
                        _role = RaftRole.Follower;
                        _votedFor = null;
                        await PersistHardStateAsync(token);
                        _electionStarted = false;
                        return;
                    }
                    if (response.VoteGranted)
                    {
                        votes++;
                        _metrics?.RecordRaftVoteReceived();
                    }
                }
            }

            int majority = (_peers.Count + 1) / 2;

            if (votes > majority)
            {
                wonElection = true;
                if (_logger.IsEnabled(LogLevel.Information))
                {
                    _logger.LogInformation("Node {NodeId}: Won election with {Votes} votes. Becoming Leader.", _nodeId, votes);
                }
                _role = RaftRole.Leader;
                SetLeader(_nodeId);
                long next = _raftLog.LastIndex + 1;
                foreach (KeyValuePair<string, string> peer in _peers)
                {
                    _nextIndex[peer.Key] = next;
                    _matchIndex[peer.Key] = 0;
                }
                foreach (KeyValuePair<string, string> peer in _readonlyPeers)
                {
                    _nextIndex[peer.Key] = next;
                    _matchIndex[peer.Key] = 0;
                }
            }
            else
            {
                if (_logger.IsEnabled(LogLevel.Information))
                {
                    _logger.LogInformation("Node {NodeId}: Lost election with {Votes} votes.", _nodeId, votes);
                }
                // Do not step down immediately, wait for next timeout or heartbeat
                // _role = RaftRole.Follower; 
            }
        }
        finally
        {
            _lock.Release();
            sw.Stop();
            _metrics?.RecordRaftElectionDuration(sw.Elapsed.TotalSeconds);
        }

        FlushLeaderChanged();

        if (wonElection)
        {
            await SendHeartbeatsAsync(token);
        }
    }

    private KeyValuePair<string, string>[] BuildAllPeers()
    {
        List<KeyValuePair<string, string>> list = new(_peers.Count + _readonlyPeers.Count);
        foreach (KeyValuePair<string, string> p in _peers)
            list.Add(p);
        foreach (KeyValuePair<string, string> rp in _readonlyPeers)
            if (!_peers.ContainsKey(rp.Key))
                list.Add(rp);
        return [.. list];
    }

    internal Task SendHeartbeatsAsync(CancellationToken token)
    {
        _lastHeartbeatSendTime = _clock.UtcNow;
        KeyValuePair<string, string>[] allPeers = _cachedAllPeers ??= BuildAllPeers();

        List<Task> newTasks = [];
        int skippedInFlight = 0;
        foreach (KeyValuePair<string, string> peer in allPeers)
        {
            // If a heartbeat is already in-flight for this peer, skip it -- don't let a slow peer
            // block the Raft main loop from sending heartbeats to other peers or the next round.
            if (_inFlightHeartbeats.TryGetValue(peer.Key, out Task? existing) && !existing.IsCompleted)
            {
                skippedInFlight++;
                continue;
            }

            Task task = SendAppendEntriesToPeerAsync(peer.Key, peer.Value, token);
            _inFlightHeartbeats[peer.Key] = task;
            newTasks.Add(task);
        }

        if (_logger.IsEnabled(LogLevel.Debug))
        {
            _logger.LogDebug("Node {NodeId}: Heartbeat dispatch round: peers={PeerCount}, dispatched={Dispatched}, skippedInFlight={Skipped}",
                _nodeId, allPeers.Length, newTasks.Count, skippedInFlight);
        }

        return newTasks.Count > 0 ? Task.WhenAll(newTasks) : Task.CompletedTask;
    }

    private void DispatchHeartbeats(CancellationToken token)
    {
        Task heartbeatTask = SendHeartbeatsAsync(token);
        _ = heartbeatTask.ContinueWith(t =>
        {
            if (t.IsFaulted && t.Exception != null && _logger.IsEnabled(LogLevel.Warning))
            {
                _logger.LogWarning(t.Exception, "Node {NodeId}: Heartbeat dispatch task faulted", _nodeId);
            }
        }, TaskScheduler.Default);
    }

    private async Task SendAppendEntriesToPeerAsync(string peerId, string peerAddress, CancellationToken token)
    {
        long oldCommitIndex = 0;
        long newCommitIndex = 0;
        bool shouldApply = false;
        try
        {
            long prevLogIndex;
            long prevLogTerm;
            RaftLogEntry[] entries;
            long currentTermSnapshot;

            try
            {
                await _lock.WaitAsync(token);
            }
            catch (ObjectDisposedException) { return; }

            try
            {
                if (_role != RaftRole.Leader) return;
                currentTermSnapshot = _currentTerm;

                if (!_nextIndex.ContainsKey(peerId)) _nextIndex[peerId] = _raftLog.LastIndex + 1;
                long nextIdx = _nextIndex[peerId];

                // With a compacted log the leader cannot serve entries below FirstIndex; the
                // peer needs a seed transfer (snapshot). Skip this round -- retries stay paced
                // by the heartbeat interval, so this backs off like an unhealthy peer instead
                // of tight-looping.
                if (_raftLog is ICompactableRaftLog compactableLog)
                {
                    long logFirstIndex = compactableLog.FirstIndex;
                    if (logFirstIndex > 1 && nextIdx < logFirstIndex)
                    {
                        if (_peersRequiringSeed.TryAdd(peerId, true) && _logger.IsEnabled(LogLevel.Warning))
                        {
                            _logger.LogWarning("Node {NodeId}: peer {PeerId} requires seed transfer; log compacted to {FirstIndex}",
                                _nodeId, peerId, logFirstIndex);
                        }
                        return;
                    }
                    _peersRequiringSeed.TryRemove(peerId, out _);
                }

                prevLogIndex = nextIdx - 1;
                prevLogTerm = await _raftLog.GetTermAtIndexAsync(prevLogIndex, token);

                List<RaftLogEntry> collected = [];
                await foreach (RaftLogEntry e in _raftLog.GetEntriesAsync(nextIdx, 100, token).WithCancellation(token))
                    collected.Add(e);
                entries = [.. collected];
            }
            finally
            {
                _lock.Release();
            }

            AppendEntriesRequest request = new()
            {
                Term = currentTermSnapshot,
                LeaderId = _nodeId,
                PrevLogIndex = prevLogIndex,
                PrevLogTerm = prevLogTerm,
                Entries = entries,
                LeaderCommit = _commitIndex,
                AuthToken = _options.AuthToken
            };

            AppendEntriesResponse response = await _transport.AppendEntriesAsync(peerId, request, token);
            _peerLastContact[peerId] = _clock.UtcNow;

            // Log recovery if this peer was previously unreachable
            bool wasDown = _peerConnected.TryGetValue(peerId, out bool connected) && !connected;
            _peerConnected[peerId] = true;
            if (wasDown)
            {
                _logger.LogInformation("Node {NodeId}: Connection to {Peer} restored.", _nodeId, peerId);
            }

            if (_logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogDebug("Node {NodeId}: Heartbeat to {PeerId} succeeded (entries={EntryCount}, term={Term})",
                    _nodeId, peerId, entries.Length, request.Term);
            }

            try
            {
                await _lock.WaitAsync(token);
            }
            catch (ObjectDisposedException) { return; }

            try
            {
                if (_role != RaftRole.Leader || _currentTerm != currentTermSnapshot) return;

                if (response.Authenticated && response.Term > _currentTerm)
                {
                    _currentTerm = response.Term;
                    _metrics?.SetRaftTerm(_currentTerm);
                    _role = RaftRole.Follower;
                    _votedFor = null;
                    await PersistHardStateAsync(token);
                    _electionStarted = false;
                    return;
                }

                if (!response.Authenticated) return;

                if (response.Success)
                {
                    long acknowledged = prevLogIndex + entries.Length;
                    long previousMatch = _matchIndex.TryGetValue(peerId, out long existingMatch) ? existingMatch : 0;
                    long match = Math.Max(previousMatch, acknowledged);
                    _matchIndex[peerId] = match;
                    _nextIndex[peerId] = match + 1;

                    oldCommitIndex = _commitIndex;
                    await TryAdvanceCommitIndexAsync(token);
                    if (_commitIndex > oldCommitIndex)
                    {
                        newCommitIndex = _commitIndex;
                        shouldApply = true;
                    }
                }
                else
                {
                    _nextIndex[peerId] = Math.Max(1, _nextIndex[peerId] - 1);
                }
            }
            finally
            {
                _lock.Release();
            }

            FlushLeaderChanged();
        }
        catch (ObjectDisposedException)
        {
        }
        catch (Exception ex)
        {
            if (token.IsCancellationRequested) return;

            bool wasConnected = !(_peerConnected.TryGetValue(peerId, out bool c) && !c);
            _peerConnected[peerId] = false;

            if (wasConnected && _logger.IsEnabled(LogLevel.Warning))
            {
                string detail = ex.Message.Split('\n', 2)[0].TrimEnd('.');
                if (detail.Length > 120) detail = detail[..120] + "...";
                _logger.LogWarning("Node {NodeId}: Heartbeat to {Peer} failed -- {Detail}", _nodeId, peerId, detail);
            }
        }

        if (shouldApply)
        {
            await ApplyEntriesAsync(oldCommitIndex + 1, newCommitIndex, token);
        }
    }

    /// <summary>
    /// Caller holds <see cref="_lock"/>. Advances <see cref="_commitIndex"/> from voting peers only.
    /// One voter commits from the local log. Two voters are primary/backup (the leader's last index
    /// is enough). Three or more use a voting majority. Readonly peers never enter the span.
    /// </summary>
    private async Task TryAdvanceCommitIndexAsync(CancellationToken token)
    {
        int voterCount = _peers.Count + 1;
        long majorityIndex;
        {
            Span<long> matchSpan = voterCount <= 64
                ? stackalloc long[voterCount]
                : new long[voterCount];
            int mi = 0;
            foreach (string peerId in _peers.Keys)
                matchSpan[mi++] = _matchIndex.TryGetValue(peerId, out long match) ? match : 0;
            matchSpan[mi] = _raftLog.LastIndex;
            matchSpan.Sort();
            int pick = voterCount >= 3 ? (voterCount - 1) / 2 : voterCount / 2;
            majorityIndex = matchSpan[pick];
        }

        if (majorityIndex > _commitIndex
            && await _raftLog.GetTermAtIndexAsync(majorityIndex, token) == _currentTerm)
        {
            _commitIndex = majorityIndex;
            _metrics?.SetRaftCommitIndex(_commitIndex);
            if (_logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogDebug("[{NodeId}] CommitIndex updated to {CommitIndex}", _nodeId, _commitIndex);
            }
        }
    }

    private bool ValidateAuthToken(string? token)
    {
        if (string.IsNullOrEmpty(_options.AuthToken)) return true;
        return _options.AuthToken == token;
    }

    // RPC Handlers
    public async Task<VoteResponse> HandleRequestVoteAsync(VoteRequest request, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        // Ensure we have a connection back to the candidate
        if (!string.IsNullOrEmpty(request.CandidateId))
        {
            _ = _transport.ValidateConnectionAsync(request.CandidateId, token).ContinueWith(t =>
            {
                if (t.Exception != null)
                {
                    if (_logger.IsEnabled(LogLevel.Warning))
                    {
                        _logger.LogWarning(t.Exception, "Failed to validate connection back to {CandidateId}", request.CandidateId);
                    }
                }
            }, TaskScheduler.Default);
        }

        if (!ValidateAuthToken(request.AuthToken))
        {
            return new VoteResponse { Term = _currentTerm, VoteGranted = false, Authenticated = false };
        }

        // Treat Term 0 as a connection probe/ping and return immediately without changing state
        if (request.Term == 0)
        {
            return new VoteResponse { Term = _currentTerm, VoteGranted = false };
        }

        try
        {
            await _lock.WaitAsync(token);
        }
        catch (ObjectDisposedException)
        {
            return new VoteResponse { Term = 0, VoteGranted = false };
        }

        VoteResponse response = new() { Term = 0, VoteGranted = false };
        try
        {
            bool hardStateChanged = false;
            if (request.Term > _currentTerm)
            {
                if (_logger.IsEnabled(LogLevel.Information))
                {
                    _logger.LogInformation("Node {NodeId}: Received higher term {Term} from {Candidate}. Stepping down.", _nodeId, request.Term, request.CandidateId);
                }
                _currentTerm = request.Term;
                _metrics?.SetRaftTerm(_currentTerm);
                _role = RaftRole.Follower;
                _votedFor = null;
                hardStateChanged = true;
                SetLeader(null);
                ResetElectionTimeout();
                _electionStarted = false;
            }

            // If we are a candidate and receive a request from a valid leader (or equal term candidate), we might step down?
            // Actually, if request.Term == _currentTerm, and we haven't voted yet (except for self?),
            // But if we are Candidate, we voted for self.
            // So if request.Term == _currentTerm, we reject unless it's us.

            // However, if we receive AppendEntries with same term, we step down.
            // RequestVote with same term? We reject because we voted for self.

            bool granted = false;
            if (request.Term >= _currentTerm && (_votedFor == null || _votedFor == request.CandidateId))
            {
                // Check log up-to-date
                long lastLogIndex = _raftLog.LastIndex;
                long lastLogTerm = _raftLog.LastTerm;

                if (request.LastLogTerm > lastLogTerm ||
                    (request.LastLogTerm == lastLogTerm && request.LastLogIndex >= lastLogIndex))
                {
                    granted = true;
                    _votedFor = request.CandidateId;
                    hardStateChanged = true;
                    ResetElectionTimeout();
                }
            }
            else
            {
                if (_logger.IsEnabled(LogLevel.Information))
                {
                    _logger.LogInformation("Node {NodeId}: Rejecting vote for {Candidate} (Term {Term}). MyTerm: {MyTerm}, VotedFor: {VotedFor}",
                       _nodeId, request.CandidateId, request.Term, _currentTerm, _votedFor);
                }
            }

            // Persist term/vote before the response leaves this node so a restart cannot
            // grant a conflicting vote in the same term.
            if (hardStateChanged)
            {
                await PersistHardStateAsync(token);
            }

            response = new VoteResponse { Term = _currentTerm, VoteGranted = granted };
        }
        finally
        {
            _lock.Release();
        }

        FlushLeaderChanged();
        return response;
    }

    public async Task<JoinResponse> HandleJoinAsync(JoinRequest request, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (!ValidateAuthToken(request.AuthToken))
        {
            return new JoinResponse { Success = false, Authenticated = false };
        }

        try
        {
            await _lock.WaitAsync(token);
        }
        catch (ObjectDisposedException)
        {
            return new JoinResponse { Success = false };
        }

        try
        {
            if (_role != RaftRole.Leader)
            {
                return new JoinResponse { Success = false, LeaderId = LeaderId };
            }

            _readonlyPeers[request.NodeId] = request.Address;
            _cachedAllPeers = null; // Invalidate heartbeat peer cache
            _peerLastContact[request.NodeId] = _clock.UtcNow;

            // Seeded join: a node restored from a seed/snapshot advertises the highest log index
            // it already holds, so replication starts at the tail instead of replaying from 1.
            // Default (LastLogIndex = 0) keeps the historical full-replay behavior, letting a
            // fresh node rebuild ClusterState (topics, partitions, leaders) from history.
            _nextIndex[request.NodeId] = request.LastLogIndex + 1;
            _matchIndex[request.NodeId] = 0;
            _peersRequiringSeed.TryRemove(request.NodeId, out _);

            return new JoinResponse
            {
                Success = true,
                LeaderId = _nodeId,
                Peers = _peers.Concat(_readonlyPeers.Where(rp => !_peers.ContainsKey(rp.Key))).ToDictionary(kvp => kvp.Key, kvp => kvp.Value)
            };
        }
        finally
        {
            _lock.Release();
        }
    }

    public async Task RemovePeerAsync(string nodeId, CancellationToken token = default)
    {
        await _lock.WaitAsync(token);
        try
        {
            _readonlyPeers.TryRemove(nodeId, out _);
            _peerLastContact.TryRemove(nodeId, out _);
            _inFlightHeartbeats.TryRemove(nodeId, out _);
            _peerConnected.TryRemove(nodeId, out _);
            _peersRequiringSeed.TryRemove(nodeId, out _);
            _nextIndex.Remove(nodeId);
            _matchIndex.Remove(nodeId);
            _cachedAllPeers = null;
        }
        finally
        {
            _lock.Release();
        }
    }

    public void PromoteReadonlyPeer(string nodeId)
    {
        if (_readonlyPeers.TryRemove(nodeId, out string? address))
        {
            _peers.TryAdd(nodeId, address);
            _cachedAllPeers = null;
            if (_logger.IsEnabled(LogLevel.Information))
                _logger.LogInformation("Node {NodeId}: promoted {PeerId} from readonly to voting peer", _nodeId, nodeId);
        }
    }

    public bool IsReadonlyPeer(string nodeId) => _readonlyPeers.ContainsKey(nodeId);

    public bool HasVotingPeers => !_peers.IsEmpty;

    /// <summary>
    /// Highest log index that is safe to compact away. As leader this is
    /// min(matchIndex over all voting and readonly peers, CommitIndex); as follower/candidate
    /// (or with no peers) it is CommitIndex. Hosts must not compact above this value.
    /// </summary>
    public long GetSafeCompactionIndex()
    {
        _lock.Wait();
        try
        {
            if (_role != RaftRole.Leader) return _commitIndex;

            long safeIndex = _commitIndex;
            foreach (string peerId in _peers.Keys)
            {
                safeIndex = Math.Min(safeIndex, _matchIndex.TryGetValue(peerId, out long match) ? match : 0);
            }
            foreach (string peerId in _readonlyPeers.Keys)
            {
                if (_peers.ContainsKey(peerId)) continue;
                safeIndex = Math.Min(safeIndex, _matchIndex.TryGetValue(peerId, out long match) ? match : 0);
            }
            return safeIndex;
        }
        finally
        {
            _lock.Release();
        }
    }

    public async Task<AppendEntriesResponse> HandleAppendEntriesAsync(AppendEntriesRequest request, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        // Ensure we have a connection back to the leader
        if (!string.IsNullOrEmpty(request.LeaderId))
        {
            _ = _transport.ValidateConnectionAsync(request.LeaderId, token).ContinueWith(t =>
            {
                if (t.Exception != null)
                {
                    if (_logger.IsEnabled(LogLevel.Warning))
                    {
                        _logger.LogWarning(t.Exception, "Failed to validate connection back to {LeaderId}", request.LeaderId);
                    }
                }
            }, TaskScheduler.Default);
        }

        if (!ValidateAuthToken(request.AuthToken))
        {
            return new AppendEntriesResponse { Term = _currentTerm, Success = false, Authenticated = false };
        }

        long oldCommitIndex = 0;
        long newCommitIndex = 0;
        bool shouldApply = false;
        bool success = false;

        try
        {
            await _lock.WaitAsync(token);
        }
        catch (ObjectDisposedException)
        {
            return new AppendEntriesResponse { Term = 0, Success = false };
        }

        try
        {
            if (request.Term > _currentTerm)
            {
                if (_logger.IsEnabled(LogLevel.Information))
                {
                    _logger.LogInformation("Node {NodeId}: Received AppendEntries with higher term {Term} (mine: {MyTerm}) from leader {LeaderId}. Stepping down.",
                        _nodeId, request.Term, _currentTerm, request.LeaderId);
                }
                _currentTerm = request.Term;
                _metrics?.SetRaftTerm(_currentTerm);
                _role = RaftRole.Follower;
                _votedFor = null;
                await PersistHardStateAsync(token);
                SetLeader(request.LeaderId);
            }
            else if (request.Term == _currentTerm && _role == RaftRole.Candidate)
            {
                _role = RaftRole.Follower;
                SetLeader(request.LeaderId);
            }

            if (request.Term == _currentTerm)
            {
                SetLeader(request.LeaderId);
                ResetElectionTimeout();
            }

            if (request.Term >= _currentTerm)
            {
                if (_role != RaftRole.Candidate)
                {
                    _role = RaftRole.Follower; // Recognize leader
                }
                _lastHeartbeat = _clock.UtcNow; // Reset timeout on valid heartbeat

                // 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
                if (request.PrevLogIndex > 0)
                {
                    if (request.PrevLogIndex > _raftLog.LastIndex)
                    {
                        success = false;
                    }
                    else
                    {
                        long prevTerm = await _raftLog.GetTermAtIndexAsync(request.PrevLogIndex, token);
                        if (prevTerm != request.PrevLogTerm)
                            success = false;
                        else
                            success = true;
                    }
                }
                else
                {
                    success = true;
                }

                if (success)
                {

                // 3. If an existing entry conflicts with a new one (same index but different terms),
                // delete the existing entry and all that follow.
                // 4. Append any new entries not already in the log.

                long newEntryIndex = 0;
                long logIndex = request.PrevLogIndex + 1;

                // Find conflict
                for (; newEntryIndex < request.Entries.Count; newEntryIndex++)
                {
                    token.ThrowIfCancellationRequested();
                    if (logIndex > _raftLog.LastIndex) break; // No conflict, just new entries

                    long existingTerm = await _raftLog.GetTermAtIndexAsync(logIndex, token);
                    if (existingTerm != request.Entries[(int)newEntryIndex].Term)
                    {
                        // Conflict
                        await _raftLog.TruncateFromAsync(logIndex, token);
                        break;
                    }
                    logIndex++;
                }

                // Append remaining entries
                if (newEntryIndex < request.Entries.Count)
                {
                    RaftLogEntry[] entriesToAppend = [.. request.Entries.Skip((int)newEntryIndex)];
                    await _raftLog.AppendAsync(entriesToAppend, token);
                }

                // 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
                if (request.LeaderCommit > _commitIndex)
                {
                    long lastNewEntryIndex = request.PrevLogIndex + request.Entries.Count;
                    newCommitIndex = Math.Min(request.LeaderCommit, lastNewEntryIndex);

                    if (newCommitIndex > _commitIndex)
                    {
                        oldCommitIndex = _commitIndex;
                        _commitIndex = newCommitIndex;
                        _metrics?.SetRaftCommitIndex(_commitIndex);
                        if (_logger.IsEnabled(LogLevel.Debug))
                        {
                            _logger.LogDebug("[{NodeId}] CommitIndex updated to {Index}", _nodeId, _commitIndex);
                        }
                        shouldApply = true;
                    }
                }
                }
            }
        }
        finally
        {
            _lock.Release();
        }

        FlushLeaderChanged();

        if (shouldApply)
        {
            await ApplyEntriesAsync(oldCommitIndex + 1, newCommitIndex, token);
        }

        return new AppendEntriesResponse { Term = _currentTerm, Success = success };
    }

    public async Task<ProposeResponse> HandleProposeAsync(ProposeRequest request, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        bool isLeader = false;
        string? leaderId = null;
        long appendedIndex = 0;
        long appendedTerm = 0;
        long oldCommitIndex = 0;
        long newCommitIndex = 0;
        bool shouldApply = false;

        try
        {
            await _lock.WaitAsync(token);
        }
        catch (ObjectDisposedException)
        {
            return new ProposeResponse { Success = false };
        }

        try
        {
            if (_role != RaftRole.Leader)
            {
                leaderId = LeaderId;
            }
            else
            {
                isLeader = true;
                leaderId = _nodeId;

                RaftLogEntry entry = new()
                {
                    Term = _currentTerm,
                    Index = _raftLog.LastIndex + 1,
                    Command = request.Command
                };

                await _raftLog.AppendAsync([entry], token);
                appendedIndex = entry.Index;
                appendedTerm = entry.Term;

                if (_peers.Count + 1 <= 2)
                {
                    oldCommitIndex = _commitIndex;
                    await TryAdvanceCommitIndexAsync(token);
                    if (_commitIndex > oldCommitIndex)
                    {
                        newCommitIndex = _commitIndex;
                        shouldApply = true;
                    }
                }
            }
        }
        finally
        {
            _lock.Release();
        }

        if (shouldApply)
        {
            await ApplyEntriesAsync(oldCommitIndex + 1, newCommitIndex, token);
        }

        if (!isLeader)
        {
            return new ProposeResponse { Success = false, LeaderId = leaderId };
        }

        if (!_peers.IsEmpty)
        {
            await SendHeartbeatsAsync(token);
        }

        try
        {
            await _lock.WaitAsync(token);
        }
        catch (ObjectDisposedException)
        {
            return new ProposeResponse { Success = false, LeaderId = leaderId };
        }

        try
        {
            bool committed = _role == RaftRole.Leader
                && _currentTerm == appendedTerm
                && _commitIndex >= appendedIndex;
            return new ProposeResponse { Success = committed, LeaderId = LeaderId };
        }
        finally
        {
            _lock.Release();
        }
    }

    public async Task<bool> ProposeAsync(ReadOnlyMemory<byte> command, CancellationToken token = default)
    {
        ProposeRequest request = new() { Command = command };

        // HandleProposeAsync re-checks _role under the lock, eliminating the TOCTOU race
        // between an outer role check and the lock acquisition in HandleProposeAsync.
        ProposeResponse localResponse = await HandleProposeAsync(request, token);
        if (localResponse.Success) return true;

        // Not leader -- forward to the known leader if available
        string? leaderId = LeaderId;
        if (leaderId != null)
        {
            if (!_peers.ContainsKey(leaderId))
            {
                if (_logger.IsEnabled(LogLevel.Warning))
                    _logger.LogWarning("Leader {LeaderId} not found in peers list.", leaderId);
                return false;
            }
            try
            {
                ProposeResponse response = await _transport.ProposeAsync(leaderId, request, token);
                return response.Success;
            }
            catch (Exception ex)
            {
                if (_logger.IsEnabled(LogLevel.Warning))
                    _logger.LogWarning(ex, "Failed to forward proposal to leader {LeaderId}", leaderId);
                return false;
            }
        }

        return false;
    }

    public event Func<RaftLogEntry, Task>? OnCommit;
    public event EventHandler<LeaderChangedEventArgs>? OnLeaderChanged;

    /// <summary>
    /// Updates <see cref="LeaderId"/> under the node lock. The <see cref="OnLeaderChanged"/> handler
    /// runs after that lock is released so it may call back into the node.
    /// </summary>
    private void SetLeader(string? leaderId)
    {
        if (LeaderId != leaderId)
        {
            if (_logger.IsEnabled(LogLevel.Information))
            {
                _logger.LogInformation("Node {NodeId}: Leader changed from {OldLeader} to {NewLeader}", _nodeId, LeaderId ?? "None", leaderId ?? "None");
            }
            LeaderId = leaderId;
            _metrics?.SetRaftLeader(leaderId ?? string.Empty);
            _pendingLeaderChanged = new LeaderChangedEventArgs(leaderId);
        }
    }

    private void FlushLeaderChanged()
    {
        LeaderChangedEventArgs? args = _pendingLeaderChanged;
        _pendingLeaderChanged = null;
        if (args is not null)
            OnLeaderChanged?.Invoke(this, args);
    }

    internal async Task ApplyEntriesAsync(long fromIndex, long toIndex, CancellationToken token)
    {
        try { await _applyLock.WaitAsync(token); }
        catch (ObjectDisposedException) { return; }

        if (_logger.IsEnabled(LogLevel.Debug))
            _logger.LogDebug("[{NodeId}] Applying entries from {From} to {To}", _nodeId, fromIndex, toIndex);

        long start = Math.Max(fromIndex, _lastApplied + 1);
        bool lockHeld = true;
        try
        {
            for (long i = start; i <= toIndex; i++)
            {
                token.ThrowIfCancellationRequested();
                RaftLogEntry? entry = await _raftLog.GetEntryAsync(i, token);
                if (entry is null)
                {
                    long firstIndex = _raftLog is ICompactableRaftLog compactable ? compactable.FirstIndex : 1;
                    if (i >= firstIndex)
                        return;
                    _lastApplied = i;
                    continue;
                }

                _applyLock.Release();
                lockHeld = false;

                try
                {
                    if (OnCommit != null)
                        await OnCommit(entry);
                }
                catch (Exception ex)
                {
                    if (_logger.IsEnabled(LogLevel.Error))
                        _logger.LogError(ex, "[{NodeId}] Error applying committed entry {Index}", _nodeId, i);
                    try { await _stopCts.CancelAsync(); } catch (ObjectDisposedException) { }
                    throw;
                }

                _lastApplied = i;

                if (i < toIndex)
                {
                    try { await _applyLock.WaitAsync(token); }
                    catch (ObjectDisposedException) { return; }
                    lockHeld = true;
                }
            }
        }
        finally
        {
            if (lockHeld) _applyLock.Release();
        }
    }

    public async Task StopAsync()
    {
        await _stopCts.CancelAsync();
        if (_loopTask != null)
        {
            try
            {
                // Wait for the loop to finish, but with a timeout to avoid hanging tests
                using CancellationTokenSource timeoutCts = new(TimeSpan.FromMilliseconds(_options.StopTimeoutMs));
                await _loopTask.WaitAsync(timeoutCts.Token);
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                if (_logger.IsEnabled(LogLevel.Warning))
                {
                    _logger.LogWarning(ex, "Node {NodeId}: Error while stopping Raft loop", _nodeId);
                }
            }
        }
    }

    protected virtual void Dispose(bool disposing)
    {
        if (!disposing || Interlocked.Exchange(ref _disposed, 1) != 0) return;
        _stopCts.Cancel();
        _stopCts.Dispose();
        _lock.Dispose();
        _applyLock.Dispose();
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }
}
