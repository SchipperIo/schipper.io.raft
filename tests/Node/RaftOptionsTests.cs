// IonStream License 1.0
// Copyright (c) 2026 SCHIPPER.IO LLC - All Rights Reserved
// Licensor: SCHIPPER.IO LLC - Contact: contact@schipper.io

namespace Schipper.Io.Raft.Tests.Node;

[TestClass]
public class RaftOptionsTests
{
    [TestMethod]
    public void Properties_RoundTripAssignedValues()
    {
        RaftOptions options = new()
        {
            ElectionTimeoutMinMs = 100,
            ElectionTimeoutMaxMs = 200,
            HeartbeatIntervalMs = 50,
            VoteRpcDeadlineMs = 75,
            AppendEntriesRpcDeadlineMs = 80,
            AuthToken = "auth-token",
            InitialValidationDelayMs = 10,
            ConnectionRetryDelayMs = 20,
            ErrorRecoveryDelayMs = 30,
            QuorumConnectionTimeoutMs = 40,
            SkipConnectionValidation = true
        };

        Assert.AreEqual(100, options.ElectionTimeoutMinMs);
        Assert.AreEqual(200, options.ElectionTimeoutMaxMs);
        Assert.AreEqual(50, options.HeartbeatIntervalMs);
        Assert.AreEqual(75, options.VoteRpcDeadlineMs);
        Assert.AreEqual(80, options.AppendEntriesRpcDeadlineMs);
        Assert.AreEqual("auth-token", options.AuthToken);
        Assert.AreEqual(10, options.InitialValidationDelayMs);
        Assert.AreEqual(20, options.ConnectionRetryDelayMs);
        Assert.AreEqual(30, options.ErrorRecoveryDelayMs);
        Assert.AreEqual(40, options.QuorumConnectionTimeoutMs);
        Assert.IsTrue(options.SkipConnectionValidation);
    }

    [TestMethod]
    public void FastMode_WhenEnabled_OverridesTimeoutDefaults()
    {
        RaftOptions options = new();

        options.SetFastMode(true);

        Assert.IsTrue(options.FastMode);
        Assert.AreEqual(10, options.ElectionTimeoutMinMs);
        Assert.AreEqual(20, options.ElectionTimeoutMaxMs);
        Assert.AreEqual(5, options.HeartbeatIntervalMs);
        Assert.AreEqual(50, options.VoteRpcDeadlineMs);
        Assert.AreEqual(50, options.AppendEntriesRpcDeadlineMs);
        Assert.AreEqual(0, options.InitialValidationDelayMs);
        Assert.AreEqual(10, options.ConnectionRetryDelayMs);
        Assert.AreEqual(10, options.ErrorRecoveryDelayMs);
        Assert.AreEqual(100, options.QuorumConnectionTimeoutMs);
        Assert.IsTrue(options.SkipConnectionValidation);
    }
}
