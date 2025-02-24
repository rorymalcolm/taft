import { spawn, ChildProcess } from "child_process";
import axios from "axios";
import { setTimeout as sleep } from "timers/promises";
import * as fs from "fs";
import * as path from "path";

// Node configuration
type NodeConfig = {
  nodeId: number;
  port: number;
};

// Node status returned by the status endpoint
type NodeStatus = {
  nodeId: number;
  state: "follower" | "candidate" | "leader";
  currentTerm: number;
  votedFor: number | null;
  logLength: number;
  commitIndex: number;
  lastApplied: number;
  leader: number | null;
};

class RaftNode {
  public readonly nodeId: number;
  public readonly port: number;
  public process: ChildProcess | null = null;
  private isRunning: boolean = false;

  constructor(nodeId: number, port: number) {
    this.nodeId = nodeId;
    this.port = port;
  }

  async start(clusterConfig: NodeConfig[]): Promise<void> {
    if (this.isRunning) {
      console.log(`Node ${this.nodeId} is already running`);
      return;
    }

    const clusterConfigJson = JSON.stringify(clusterConfig);

    // Start the node process
    this.process = spawn(
      "node",
      [
        "dist/index.js",
        "start",
        "--node",
        this.nodeId.toString(),
        "--port",
        this.port.toString(),
        "--cluster",
        clusterConfigJson,
      ],
      {
        stdio: "pipe",
      }
    );

    // Handle process output
    this.process.stdout?.on("data", (data) => {
      console.log(`[Node ${this.nodeId}] ${data.toString().trim()}`);
    });

    this.process.stderr?.on("data", (data) => {
      console.error(`[Node ${this.nodeId} ERROR] ${data.toString().trim()}`);
    });

    this.process.on("exit", (code) => {
      console.log(`Node ${this.nodeId} exited with code ${code}`);
      this.isRunning = false;
      this.process = null;
    });

    this.isRunning = true;

    // Wait a bit for the server to start
    await sleep(500);

    // Verify the node is responding
    try {
      await this.getStatus();
      console.log(`✅ Node ${this.nodeId} is running on port ${this.port}`);
    } catch (error) {
      console.error(
        `❌ Failed to verify Node ${this.nodeId} is running:`,
        error
      );
      throw new Error(`Failed to start node ${this.nodeId}`);
    }
  }

  async stop(): Promise<void> {
    if (!this.isRunning || !this.process) {
      console.log(`Node ${this.nodeId} is not running`);
      return;
    }

    console.log(`Stopping node ${this.nodeId}...`);

    // First try graceful termination
    this.process.kill("SIGTERM");

    // Wait for process to exit (with timeout)
    try {
      await Promise.race([
        new Promise<void>((resolve) => {
          const checkInterval = setInterval(() => {
            if (!this.isRunning) {
              clearInterval(checkInterval);
              resolve();
            }
          }, 100);
        }),
        // Add a timeout of 2 seconds
        new Promise<void>((_, reject) =>
          setTimeout(
            () => reject(new Error("Timeout waiting for process to exit")),
            2000
          )
        ),
      ]);
    } catch (err) {
      // If timeout occurred, force kill the process
      console.log(`Forcefully terminating node ${this.nodeId}...`);
      if (this.process && this.process.pid) {
        try {
          process.kill(this.process.pid, "SIGKILL");
        } catch (e) {
          // Process may already be gone
          console.log(`Error killing process ${this.process.pid}: ${e}`);
        }
      }
    }

    // Ensure we mark the node as not running
    this.isRunning = false;
    this.process = null;

    console.log(`🛑 Node ${this.nodeId} stopped`);
  }

  async getStatus(): Promise<NodeStatus> {
    try {
      const response = await axios.get(`http://localhost:${this.port}/status`);
      return response.data;
    } catch (error) {
      throw new Error(`Failed to get status for node ${this.nodeId}: ${error}`);
    }
  }

  async executeCommand(command: string): Promise<any> {
    try {
      const response = await axios.post(
        `http://localhost:${this.port}/execute`,
        {
          command,
        }
      );
      return response.data;
    } catch (error: any) {
      if (error.response && error.response.data) {
        // This might be a valid response (e.g., not the leader)
        return error.response.data;
      }
      throw new Error(
        `Failed to execute command on node ${this.nodeId}: ${error}`
      );
    }
  }

  isAlive(): boolean {
    return this.isRunning;
  }
}

class RaftCluster {
  private nodes: RaftNode[] = [];
  private clusterConfig: NodeConfig[] = [];
  private basePort: number;

  constructor(nodeCount: number, basePort: number = 3000) {
    this.basePort = basePort;

    // Create node configurations
    for (let i = 0; i < nodeCount; i++) {
      const nodeId = i + 1; // Start node IDs from 1
      const port = basePort + i;
      this.clusterConfig.push({ nodeId, port });
    }

    // Create node instances
    this.nodes = this.clusterConfig.map(
      (config) => new RaftNode(config.nodeId, config.port)
    );
  }

  async start(): Promise<void> {
    console.log(`🚀 Starting Raft cluster with ${this.nodes.length} nodes...`);

    // Start all nodes
    const startPromises = this.nodes.map((node) =>
      node.start(this.clusterConfig)
    );

    await Promise.all(startPromises);
    console.log(`✅ All nodes started successfully`);

    // Wait for leader election
    await this.waitForLeaderElection();
  }

  async stop(): Promise<void> {
    console.log("🛑 Stopping all nodes...");

    // Stop each node with a short delay between them
    for (const node of this.nodes) {
      if (node.isAlive()) {
        try {
          await node.stop();
        } catch (error) {
          console.error(`Error stopping node ${node.nodeId}:`, error);
          // Continue stopping other nodes even if one fails
        }
        // Short delay to avoid overwhelming the system
        await sleep(100);
      }
    }

    // Final check - are there any nodes still alive?
    const remainingLiveNodes = this.nodes.filter((node) => node.isAlive());
    if (remainingLiveNodes.length > 0) {
      console.warn(
        `Warning: ${remainingLiveNodes.length} nodes still appear to be running after stop attempt`
      );
      // Try one more time with force
      for (const node of remainingLiveNodes) {
        try {
          if (node.process && node.process.pid) {
            console.log(
              `Force killing node ${node.nodeId} (PID: ${node.process.pid})...`
            );
            process.kill(node.process.pid, "SIGKILL");
          }
        } catch (e) {
          // Already gone, ignore
        }
      }
    }

    console.log("✅ All nodes stopped");
  }

  async getNodeStatuses(): Promise<NodeStatus[]> {
    const statusPromises = this.nodes
      .filter((node) => node.isAlive())
      .map((node) => node.getStatus().catch(() => null));

    const statuses = await Promise.all(statusPromises);
    return statuses.filter((status): status is NodeStatus => status !== null);
  }

  async waitForLeaderElection(
    timeoutMs: number = 5000
  ): Promise<NodeStatus | null> {
    console.log("⏳ Waiting for leader election...");

    const startTime = Date.now();

    while (Date.now() - startTime < timeoutMs) {
      const statuses = await this.getNodeStatuses();

      // Find the leader
      const leader = statuses.find((status) => status.state === "leader");

      if (leader) {
        console.log(
          `✅ Leader elected: Node ${leader.nodeId} (Term: ${leader.currentTerm})`
        );
        return leader;
      }

      await sleep(200);
    }

    console.error("❌ Leader election timed out");
    return null;
  }

  async findLeader(): Promise<NodeStatus | null> {
    const statuses = await this.getNodeStatuses();
    return statuses.find((status) => status.state === "leader") || null;
  }

  async executeCommandOnLeader(command: string): Promise<any> {
    const leader = await this.findLeader();

    if (!leader) {
      throw new Error("No leader found to execute command");
    }

    const leaderNode = this.nodes.find((node) => node.nodeId === leader.nodeId);

    if (!leaderNode) {
      throw new Error(`Leader node ${leader.nodeId} not found in cluster`);
    }

    return leaderNode.executeCommand(command);
  }

  async executeCommandOnNode(nodeId: number, command: string): Promise<any> {
    const node = this.getNode(nodeId);

    if (!node) {
      throw new Error(`Node ${nodeId} not found`);
    }

    return node.executeCommand(command);
  }

  async stopNode(nodeId: number): Promise<void> {
    const node = this.getNode(nodeId);

    if (!node) {
      throw new Error(`Node ${nodeId} not found`);
    }

    await node.stop();
    console.log(`Node ${nodeId} stopped`);
  }

  async startNode(nodeId: number): Promise<void> {
    const node = this.getNode(nodeId);

    if (!node) {
      throw new Error(`Node ${nodeId} not found`);
    }

    await node.start(this.clusterConfig);
    console.log(`Node ${nodeId} started`);
  }

  getNode(nodeId: number): RaftNode | undefined {
    return this.nodes.find((node) => node.nodeId === nodeId);
  }

  getNodeCount(): number {
    return this.nodes.length;
  }

  getLiveNodeCount(): number {
    return this.nodes.filter((node) => node.isAlive()).length;
  }
}

// Test scenarios
const testScenarios = {
  /**
   * Basic leader election test
   */
  async testLeaderElection(cluster: RaftCluster): Promise<boolean> {
    console.log("\n🧪 RUNNING TEST: Leader Election");

    // Get current leader
    const initialLeader = await cluster.findLeader();

    if (!initialLeader) {
      console.error("❌ No initial leader found");
      return false;
    }

    console.log(
      `Initial leader: Node ${initialLeader.nodeId} (Term: ${initialLeader.currentTerm})`
    );

    // Stop the leader
    await cluster.stopNode(initialLeader.nodeId);
    console.log(`Stopped leader node ${initialLeader.nodeId}`);

    // Wait for new leader election
    const newLeader = await cluster.waitForLeaderElection(10000);

    if (!newLeader) {
      console.error(
        "❌ Failed to elect new leader after stopping the initial leader"
      );
      return false;
    }

    if (newLeader.nodeId === initialLeader.nodeId) {
      console.error("❌ New leader has the same ID as the stopped leader");
      return false;
    }

    console.log(
      `New leader elected: Node ${newLeader.nodeId} (Term: ${newLeader.currentTerm})`
    );
    console.log("✅ Leader election test passed");

    // Restart the old leader
    await cluster.startNode(initialLeader.nodeId);

    return true;
  },

  /**
   * Test log replication
   */
  async testLogReplication(cluster: RaftCluster): Promise<boolean> {
    console.log("\n🧪 RUNNING TEST: Log Replication");

    // Make sure there's a leader
    const leader = await cluster.findLeader();
    if (!leader) {
      console.error("❌ No leader found for log replication test");
      return false;
    }

    // Send commands to the leader
    const commands = ["set x 1", "set y 2", "set z 3"];

    console.log(
      `Sending ${commands.length} commands to leader (Node ${leader.nodeId})...`
    );

    for (const command of commands) {
      const result = await cluster.executeCommandOnLeader(command);
      console.log(`Command "${command}" result:`, result);

      if (!result.success) {
        console.error(`❌ Command "${command}" failed:`, result);
        return false;
      }
    }

    // Wait for replication
    await sleep(1000);

    // Check all nodes have the same log length
    const statuses = await cluster.getNodeStatuses();
    const leaderStatus = statuses.find((s) => s.nodeId === leader.nodeId);

    if (!leaderStatus) {
      console.error("❌ Cannot find leader status");
      return false;
    }

    const expectedLogLength = leaderStatus.logLength;

    const allInSync = statuses.every((status) => {
      const inSync = status.logLength === expectedLogLength;
      console.log(
        `Node ${status.nodeId}: log length ${status.logLength} (${
          inSync ? "✓" : "✗"
        })`
      );
      return inSync;
    });

    if (!allInSync) {
      console.error("❌ Not all nodes have the same log length");
      return false;
    }

    console.log(`✅ All nodes have the same log length (${expectedLogLength})`);
    console.log("✅ Log replication test passed");

    return true;
  },

  /**
   * Test log consistency after leader changes
   */
  async testLogConsistencyAfterLeaderChange(
    cluster: RaftCluster
  ): Promise<boolean> {
    console.log("\n🧪 RUNNING TEST: Log Consistency After Leader Change");

    // First leader
    const leader1 = await cluster.findLeader();
    if (!leader1) {
      console.error("❌ No initial leader found");
      return false;
    }

    // Send some commands to the first leader
    const commands1 = ["set a 10", "set b 20"];

    console.log(
      `Sending ${commands1.length} commands to first leader (Node ${leader1.nodeId})...`
    );

    for (const command of commands1) {
      await cluster.executeCommandOnLeader(command);
    }

    // Wait for replication
    await sleep(1000);

    // Get the log state before leader change
    const statuses1 = await cluster.getNodeStatuses();
    const leaderStatus1 = statuses1.find((s) => s.nodeId === leader1.nodeId);

    if (!leaderStatus1) {
      console.error("❌ Cannot find first leader status");
      return false;
    }

    const initialLogLength = leaderStatus1.logLength;
    console.log(`Log length after first set of commands: ${initialLogLength}`);

    // Stop the leader to force an election
    await cluster.stopNode(leader1.nodeId);
    console.log(`Stopped first leader (Node ${leader1.nodeId})`);

    // Wait for new leader
    const leader2 = await cluster.waitForLeaderElection();
    if (!leader2) {
      console.error("❌ Failed to elect a new leader");
      return false;
    }

    console.log(`New leader elected: Node ${leader2.nodeId}`);

    // Send more commands to the new leader
    const commands2 = ["set c 30", "set d 40"];

    console.log(
      `Sending ${commands2.length} commands to new leader (Node ${leader2.nodeId})...`
    );

    for (const command of commands2) {
      await cluster.executeCommandOnLeader(command);
    }

    // Wait for replication
    await sleep(1000);

    // Get the log state after leader change
    const statuses2 = await cluster.getNodeStatuses();
    const leaderStatus2 = statuses2.find((s) => s.nodeId === leader2.nodeId);

    if (!leaderStatus2) {
      console.error("❌ Cannot find second leader status");
      return false;
    }

    const finalLogLength = leaderStatus2.logLength;
    console.log(`Log length after second set of commands: ${finalLogLength}`);

    // Verify log length increased
    if (finalLogLength <= initialLogLength) {
      console.error("❌ Log did not grow after leader change");
      return false;
    }

    // Verify all live nodes have the same log length
    const allSameLength = statuses2.every((status) => {
      const isSame = status.logLength === finalLogLength;
      console.log(
        `Node ${status.nodeId}: log length ${status.logLength} (${
          isSame ? "✓" : "✗"
        })`
      );
      return isSame;
    });

    if (!allSameLength) {
      console.error(
        "❌ Not all nodes have the same log length after leader change"
      );
      return false;
    }

    console.log("✅ All nodes have consistent logs after leader change");

    // Restart the first leader
    await cluster.startNode(leader1.nodeId);
    console.log(`Restarted former leader (Node ${leader1.nodeId})`);

    // Wait for sync
    await sleep(2000);

    // Verify the restarted node catches up
    const finalStatuses = await cluster.getNodeStatuses();
    const restartedNodeStatus = finalStatuses.find(
      (s) => s.nodeId === leader1.nodeId
    );

    if (!restartedNodeStatus) {
      console.error("❌ Cannot find restarted node status");
      return false;
    }

    if (restartedNodeStatus.logLength !== finalLogLength) {
      console.error(
        `❌ Restarted node has incorrect log length: ${restartedNodeStatus.logLength} (expected ${finalLogLength})`
      );
      return false;
    }

    console.log(
      `✅ Restarted node caught up with log length ${restartedNodeStatus.logLength}`
    );
    console.log("✅ Log consistency test passed");

    return true;
  },

  /**
   * Test handling of network partitions
   */
  async testNetworkPartition(cluster: RaftCluster): Promise<boolean> {
    console.log("\n🧪 RUNNING TEST: Network Partition");

    // We need at least 5 nodes for this test
    if (cluster.getNodeCount() < 5) {
      console.error("❌ Network partition test requires at least 5 nodes");
      return false;
    }

    // Find current leader
    const leader = await cluster.findLeader();
    if (!leader) {
      console.error("❌ No leader found");
      return false;
    }

    console.log(`Current leader is Node ${leader.nodeId}`);

    // Create a partition by stopping a minority of nodes (but not the leader)
    const nodesToPartition = 2;
    const allStatuses = await cluster.getNodeStatuses();
    const nonLeaderNodes = allStatuses
      .filter((status) => status.nodeId !== leader.nodeId)
      .map((status) => status.nodeId);

    // Select nodes to partition
    const partitionedNodeIds = nonLeaderNodes.slice(0, nodesToPartition);

    console.log(
      `Creating partition: stopping nodes ${partitionedNodeIds.join(", ")}...`
    );

    // Stop the partitioned nodes
    for (const nodeId of partitionedNodeIds) {
      await cluster.stopNode(nodeId);
    }

    // Verify leader is still the same
    await sleep(2000);

    const leaderAfterPartition = await cluster.findLeader();
    if (!leaderAfterPartition) {
      console.error("❌ Lost leader after minority partition");
      return false;
    }

    if (leaderAfterPartition.nodeId !== leader.nodeId) {
      console.error(
        `❌ Leader changed from Node ${leader.nodeId} to Node ${leaderAfterPartition.nodeId} after minority partition`
      );
      return false;
    }

    console.log(
      `✅ Leader is still Node ${leader.nodeId} after minority partition`
    );

    // Send a command to verify cluster is still operational
    const commandResult = await cluster.executeCommandOnLeader(
      "set partition_test 1"
    );

    if (!commandResult.success) {
      console.error("❌ Failed to execute command after partition");
      return false;
    }

    console.log("✅ Successfully executed command after partition");

    // Bring back the partitioned nodes
    for (const nodeId of partitionedNodeIds) {
      await cluster.startNode(nodeId);
    }

    // Wait for nodes to catch up
    await sleep(2000);

    // Verify all nodes have the same log length
    const finalStatuses = await cluster.getNodeStatuses();
    const leaderStatus = finalStatuses.find((s) => s.state === "leader");

    if (!leaderStatus) {
      console.error("❌ No leader found after healing partition");
      return false;
    }

    const expectedLogLength = leaderStatus.logLength;

    const allInSync = finalStatuses.every((status) => {
      const inSync = status.logLength === expectedLogLength;
      console.log(
        `Node ${status.nodeId}: log length ${status.logLength} (${
          inSync ? "✓" : "✗"
        })`
      );
      return inSync;
    });

    if (!allInSync) {
      console.error(
        "❌ Not all nodes have the same log length after healing partition"
      );
      return false;
    }

    console.log(
      `✅ All nodes have the same log length (${expectedLogLength}) after healing partition`
    );
    console.log("✅ Network partition test passed");

    return true;
  },
};

// Create a global reference to active clusters for cleanup on exit
let activeCluster: RaftCluster | null = null;

// Setup signal handlers for graceful shutdown
function setupExitHandlers() {
  // Handle ctrl+c and other termination signals
  const exitHandler = async (
    options: { cleanup?: boolean; exit?: boolean },
    exitCode: number
  ) => {
    if (options.cleanup && activeCluster) {
      console.log("\n🧹 Cleaning up before exit...");
      try {
        await activeCluster.stop();
        console.log("✅ Cleanup complete");
      } catch (error) {
        console.error("❌ Error during cleanup:", error);
      }
      activeCluster = null;
    }

    if (options.exit) {
      process.exit(exitCode || 0);
    }
  };

  // Do cleanup on exit
  process.on("exit", exitHandler.bind(null, { cleanup: true }));

  // Handle ctrl+c
  process.on("SIGINT", exitHandler.bind(null, { cleanup: true, exit: true }));

  // Handle kill command
  process.on("SIGTERM", exitHandler.bind(null, { cleanup: true, exit: true }));

  // Handle uncaught exceptions
  process.on("uncaughtException", (err) => {
    console.error("Uncaught Exception:", err);
    exitHandler({ cleanup: true, exit: true }, 1);
  });
}

// Main test runner
async function runTests() {
  console.log("🧪 Starting Raft test harness...");
  setupExitHandlers();

  // Setup a cluster of 5 nodes
  const cluster = new RaftCluster(5, 3000);
  activeCluster = cluster;

  try {
    // Start the cluster with timeout
    try {
      await Promise.race([
        cluster.start(),
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error("Timeout starting cluster")), 10000)
        ),
      ]);
    } catch (error) {
      console.error("❌ Failed to start cluster within timeout:", error);
      // Make sure we stop any nodes that did start
      await cluster.stop();
      process.exit(1);
    }

    // Run tests
    const results = [];

    // Run each test in sequence, with error handling for each
    try {
      console.log("\n🧪 RUNNING TEST: Leader Election");
      results.push(await testScenarios.testLeaderElection(cluster));
    } catch (error) {
      console.error("❌ Leader election test failed with error:", error);
      results.push(false);
    }

    try {
      console.log("\n🧪 RUNNING TEST: Log Replication");
      results.push(await testScenarios.testLogReplication(cluster));
    } catch (error) {
      console.error("❌ Log replication test failed with error:", error);
      results.push(false);
    }

    try {
      console.log("\n🧪 RUNNING TEST: Log Consistency After Leader Change");
      results.push(
        await testScenarios.testLogConsistencyAfterLeaderChange(cluster)
      );
    } catch (error) {
      console.error("❌ Log consistency test failed with error:", error);
      results.push(false);
    }

    try {
      console.log("\n🧪 RUNNING TEST: Network Partition");
      results.push(await testScenarios.testNetworkPartition(cluster));
    } catch (error) {
      console.error("❌ Network partition test failed with error:", error);
      results.push(false);
    }

    // Print summary
    console.log("\n📊 TEST SUMMARY:");
    const passedCount = results.filter(Boolean).length;
    console.log(`✅ Passed: ${passedCount}/${results.length}`);

    if (passedCount !== results.length) {
      console.log("❌ Some tests failed");
      await cluster.stop();
      activeCluster = null;
      process.exit(1);
    } else {
      console.log("🎉 All tests passed!");
    }
  } catch (error) {
    console.error("❌ Error running tests:", error);
    process.exit(1);
  } finally {
    // Clean up
    try {
      if (activeCluster) {
        await activeCluster.stop();
        activeCluster = null;
      }
    } catch (error) {
      console.error("❌ Error stopping cluster during cleanup:", error);
      process.exit(1);
    }
  }
}

// Run the tests if this file is executed directly
if (require.main === module) {
  runTests().catch((error) => {
    console.error("Unhandled error in test harness:", error);
    process.exit(1);
  });
}

export { RaftCluster, testScenarios };
