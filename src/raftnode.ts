import http from "http";

import {
  RequestVoteRequest,
  AppendEntriesRequest,
  RaftNodeState,
  LogEntry,
} from "./types";
import pino from "pino";
import {
  readBody,
  sendAppendEntriesRequest,
  sendRequestVoteRequest,
} from "./utils";

const ELECTION_TIMEOUT_MIN = 150;
const ELECTION_TIMEOUT_MAX = 300; // Added randomization range
const HEARTBEAT_TIMEOUT = 100;
const HEARTBEAT_INTERVAL = 25;

const logger = pino();

function randomElectionTimeOut() {
  return (
    Math.floor(Math.random() * (ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN)) +
    ELECTION_TIMEOUT_MIN
  );
}

export default class RaftNode {
  private nodeId = 0;
  private port = 0;
  private clusterTopology: { nodeId: number; port: number }[] = [];

  // raft node state and timeout
  private state: RaftNodeState = "follower";
  private electionTimeout: number = this.setElectionTimeout();
  private lastHeartbeat: number = Date.now();
  private heartbeatTimer: NodeJS.Timeout | null = null;

  // persistent state on all servers
  // update on stable storage before responding to RPCs
  private currentTerm: number = 0;
  private votedFor: number | null = null;
  private log: LogEntry[] = [];

  // volatile state on all servers
  private commitIndex: number = 0;
  private lastApplied: number = 0;

  // volatile state on leaders
  private nextIndex: Record<number, number> = {}; // Changed to Record/Map
  private matchIndex: Record<number, number> = {}; // Changed to Record/Map

  constructor(
    nodeId: number,
    port: number,
    cluster: { nodeId: number; port: number }[]
  ) {
    logger.info({
      msg: `starting node`,
      nodeId,
      port,
      cluster,
    });
    this.nodeId = nodeId;
    this.port = port;
    this.clusterTopology = cluster;

    // Initialize with a no-op entry at index 0
    this.log.push({ term: 0, command: "" });
  }

  private setElectionTimeout() {
    const timeout = randomElectionTimeOut();
    this.electionTimeout = timeout;
    setTimeout(() => {
      // Check if we should start an election
      if (
        this.lastHeartbeat + HEARTBEAT_TIMEOUT < Date.now() &&
        this.state !== "leader"
      ) {
        logger.info({
          msg: `starting election`,
          nodeId: this.nodeId,
          state: this.state,
          lastHeartbeatAt: new Date(this.lastHeartbeat).toISOString(),
          timeFromWhichWeNeedToStartAnElection: new Date(
            this.lastHeartbeat + HEARTBEAT_TIMEOUT
          ).toISOString(),
          currentTimeFormatted: new Date(Date.now()).toISOString(),
          secsSinceLastHeartbeat: (Date.now() - this.lastHeartbeat) / 1000,
        });
        this.processCandidateTransition();
      } else {
        // Reset the election timeout if we're not starting an election
        this.setElectionTimeout();
      }
    }, timeout);
    return timeout;
  }

  private async processCandidateTransition() {
    this.state = "candidate";
    this.currentTerm++; // Increment term
    this.votedFor = this.nodeId; // Vote for self - fixed this!

    logger.info({
      msg: `node is now a candidate`,
      nodeId: this.nodeId,
      term: this.currentTerm,
    });

    // Start new election timer in case this election fails
    const electionTimeout = randomElectionTimeOut();
    const startOfElection = Date.now();

    // Calculate votes needed for majority
    const quorum = Math.floor(this.clusterTopology.length / 2) + 1;
    let voteCount = 1; // Count self vote

    // Request votes from all other servers
    const voteRequests = this.clusterTopology
      .filter((node) => node.nodeId !== this.nodeId)
      .map(async (node) => {
        if (this.state !== "candidate") return; // Stop if no longer a candidate

        logger.info({
          msg: `sending vote request`,
          nodeId: this.nodeId,
          targetNodeId: node.nodeId,
          port: node.port,
          term: this.currentTerm,
        });

        // Prepare vote request with correct log information
        const lastLogIndex = this.log.length - 1;
        const lastLogTerm = this.log[lastLogIndex]?.term || 0;

        const voteResponse = await sendRequestVoteRequest(node.port, {
          term: this.currentTerm,
          candidateId: this.nodeId,
          lastLogIndex: lastLogIndex,
          lastLogTerm: lastLogTerm,
        });

        // Handle the response
        logger.info({
          msg: `got vote response`,
          nodeId: this.nodeId,
          targetNodeId: node.nodeId,
          port: node.port,
          voteResponse,
        });

        // If we discover a higher term, revert to follower
        if (voteResponse.term > this.currentTerm) {
          this.currentTerm = voteResponse.term;
          this.state = "follower";
          this.votedFor = null;
          this.lastHeartbeat = Date.now(); // Reset heartbeat timer
          return;
        }

        // Count the vote if granted
        if (voteResponse.voteGranted && this.state === "candidate") {
          voteCount++;

          // Check if we have a majority
          if (voteCount >= quorum) {
            // We won the election!
            this.becomeLeader();
          }
        }
      });

    // Wait for all vote requests to complete or timeout
    await Promise.all(voteRequests).catch((err) => {
      logger.error({ msg: "Error in vote requests", error: err });
    });

    // If election timeout elapses and we're still a candidate, start a new election
    if (this.state === "candidate") {
      setTimeout(() => {
        if (this.state === "candidate") {
          logger.info({
            msg: `election timeout elapsed, starting new election`,
            nodeId: this.nodeId,
            term: this.currentTerm,
          });
          this.processCandidateTransition();
        }
      }, electionTimeout);
    }
  }

  private becomeLeader() {
    if (this.state !== "candidate") return;

    this.state = "leader";
    logger.info({
      msg: `node is now the leader`,
      nodeId: this.nodeId,
      term: this.currentTerm,
    });

    // Initialize leader state
    this.nextIndex = {};
    this.matchIndex = {};

    // Initialize nextIndex to the length of our log, and matchIndex to 0
    for (const node of this.clusterTopology) {
      if (node.nodeId !== this.nodeId) {
        this.nextIndex[node.nodeId] = this.log.length;
        this.matchIndex[node.nodeId] = 0;
      }
    }

    // Send initial empty append entries (heartbeats)
    this.sendHeartbeats();

    // Start periodic heartbeats
    this.startHeartbeatTimer();
  }

  private startHeartbeatTimer() {
    // Clear any existing timer
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
    }

    // Start a new timer
    this.heartbeatTimer = setInterval(() => {
      if (this.state === "leader") {
        this.sendHeartbeats();
      } else {
        // If we're no longer the leader, stop sending heartbeats
        this.stopHeartbeatTimer();
      }
    }, HEARTBEAT_INTERVAL);
  }

  private stopHeartbeatTimer() {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
  }

  private sendHeartbeats() {
    if (this.state !== "leader") return;

    for (const node of this.clusterTopology) {
      if (node.nodeId !== this.nodeId) {
        this.replicateLogsToFollower(node.nodeId);
      }
    }
  }

  private async replicateLogsToFollower(nodeId: number) {
    if (this.state !== "leader") return;

    const node = this.clusterTopology.find((n) => n.nodeId === nodeId);
    if (!node) return;

    const prevLogIndex = this.nextIndex[nodeId] - 1;
    const prevLogTerm = this.log[prevLogIndex]?.term || 0;
    const entries = this.log.slice(this.nextIndex[nodeId]);

    logger.info({
      msg: `sending append entries`,
      nodeId: this.nodeId,
      targetNodeId: nodeId,
      prevLogIndex,
      prevLogTerm,
      entriesCount: entries.length,
      nextIndex: this.nextIndex[nodeId],
    });

    try {
      const response = await sendAppendEntriesRequest(node.port, {
        term: this.currentTerm,
        leaderId: this.nodeId,
        prevLogIndex,
        prevLogTerm,
        entries,
        leaderCommit: this.commitIndex,
      });

      // If we discover a higher term, revert to follower
      if (response.term > this.currentTerm) {
        this.currentTerm = response.term;
        this.state = "follower";
        this.votedFor = null;
        this.stopHeartbeatTimer();
        return;
      }

      if (response.success) {
        // Update our tracking of the follower's log state
        if (entries.length > 0) {
          this.nextIndex[nodeId] = prevLogIndex + entries.length + 1;
          this.matchIndex[nodeId] = prevLogIndex + entries.length;

          logger.info({
            msg: `append entries successful`,
            nodeId: this.nodeId,
            targetNodeId: nodeId,
            newNextIndex: this.nextIndex[nodeId],
            newMatchIndex: this.matchIndex[nodeId],
          });

          // Try to advance the commit index
          this.updateCommitIndex();
        }
      } else {
        // Follower rejected the append - decrement nextIndex and retry
        this.nextIndex[nodeId] = Math.max(1, this.nextIndex[nodeId] - 1);

        logger.info({
          msg: `append entries failed, retrying with earlier entry`,
          nodeId: this.nodeId,
          targetNodeId: nodeId,
          newNextIndex: this.nextIndex[nodeId],
        });

        // Schedule immediate retry
        setImmediate(() => this.replicateLogsToFollower(nodeId));
      }
    } catch (error) {
      logger.error({
        msg: `error sending append entries`,
        nodeId: this.nodeId,
        targetNodeId: nodeId,
        error,
      });
    }
  }

  private updateCommitIndex() {
    // Implement the leader commit rule (§5.3, §5.4):
    // If there exists an N such that N > commitIndex, a majority of
    // matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N

    if (this.state !== "leader") return;

    // Look for highest N that meets criteria
    for (let n = this.log.length - 1; n > this.commitIndex; n--) {
      // Only consider entries from current term
      if (this.log[n].term !== this.currentTerm) continue;

      // Count servers that have this entry
      let matchCount = 1; // Count self

      for (const node of this.clusterTopology) {
        if (node.nodeId !== this.nodeId && this.matchIndex[node.nodeId] >= n) {
          matchCount++;
        }
      }

      // If we have a majority, commit up to this index
      if (matchCount > this.clusterTopology.length / 2) {
        logger.info({
          msg: `advancing commit index`,
          nodeId: this.nodeId,
          oldCommitIndex: this.commitIndex,
          newCommitIndex: n,
        });

        this.commitIndex = n;
        this.applyLogEntries();
        break;
      }
    }
  }

  private applyLogEntries() {
    // Apply committed entries to state machine
    while (this.lastApplied < this.commitIndex) {
      this.lastApplied++;

      const entry = this.log[this.lastApplied];
      logger.info({
        msg: `applying log entry to state machine`,
        nodeId: this.nodeId,
        index: this.lastApplied,
        term: entry.term,
        command: entry.command,
      });

      // In a real implementation, you would apply the command to your state machine here
      // For this example, we simply log it
    }
  }

  private appendEntries(
    term: number,
    leaderId: number,
    prevLogIndex: number,
    prevLogTerm: number,
    entries: LogEntry[],
    leaderCommit: number
  ): {
    term: number;
    success: boolean;
  } {
    logger.info({
      msg: `received append entries request`,
      nodeId: this.nodeId,
      leaderId,
      term,
      prevLogIndex,
      prevLogTerm,
      entriesCount: entries.length,
    });

    // Update last heartbeat time
    this.lastHeartbeat = Date.now();

    // Handle term comparison (Rule 1)
    if (term > this.currentTerm) {
      logger.info({
        msg: `discovered higher term`,
        nodeId: this.nodeId,
        oldTerm: this.currentTerm,
        newTerm: term,
      });

      this.currentTerm = term;
      this.state = "follower";
      this.votedFor = null;
    }

    // Reject if term is less than current term
    if (term < this.currentTerm) {
      logger.info({
        msg: `rejecting append entries - term too old`,
        nodeId: this.nodeId,
        messageTerm: term,
        currentTerm: this.currentTerm,
      });

      return {
        term: this.currentTerm,
        success: false,
      };
    }

    // Revert to follower if we're a candidate
    if (this.state === "candidate") {
      this.state = "follower";
    }

    // Check for log consistency (Rule 2)
    if (
      prevLogIndex >= this.log.length ||
      (prevLogIndex > 0 && this.log[prevLogIndex].term !== prevLogTerm)
    ) {
      logger.info({
        msg: `rejecting append entries - log inconsistency`,
        nodeId: this.nodeId,
        logLength: this.log.length,
        prevLogIndex,
        prevLogTerm,
        actualTermAtPrevIndex:
          prevLogIndex < this.log.length ? this.log[prevLogIndex].term : "none",
      });

      return {
        term: this.currentTerm,
        success: false,
      };
    }

    // Handle log conflicts (Rule 3)
    if (entries.length > 0) {
      let newEntryIndex = prevLogIndex + 1;

      for (let i = 0; i < entries.length; i++) {
        if (newEntryIndex < this.log.length) {
          // Check for conflicts
          if (this.log[newEntryIndex].term !== entries[i].term) {
            // Delete this and all following entries
            logger.info({
              msg: `deleting conflicting entries`,
              nodeId: this.nodeId,
              fromIndex: newEntryIndex,
              existingTerm: this.log[newEntryIndex].term,
              newTerm: entries[i].term,
            });

            this.log = this.log.slice(0, newEntryIndex);
            // Then we'll append the new entry below
            break;
          }
        } else {
          // We've reached the end of our log, break to append entries
          break;
        }

        newEntryIndex++;
      }

      // Append any new entries not already in the log (Rule 4)
      for (let i = 0; i < entries.length; i++) {
        const entryIndex = prevLogIndex + 1 + i;

        if (entryIndex >= this.log.length) {
          this.log.push(entries[i]);

          logger.info({
            msg: `appended new entry`,
            nodeId: this.nodeId,
            index: entryIndex,
            term: entries[i].term,
          });
        }
      }
    }

    // Update commit index if needed (Rule 5)
    if (leaderCommit > this.commitIndex) {
      const lastNewEntryIndex = prevLogIndex + entries.length;
      this.commitIndex = Math.min(leaderCommit, lastNewEntryIndex);

      logger.info({
        msg: `updating commit index`,
        nodeId: this.nodeId,
        newCommitIndex: this.commitIndex,
      });

      // Apply newly committed entries
      this.applyLogEntries();
    }

    return {
      term: this.currentTerm,
      success: true,
    };
  }

  private requestVote(
    term: number,
    candidateId: number,
    lastLogIndex: number,
    lastLogTerm: number
  ): {
    term: number;
    voteGranted: boolean;
  } {
    logger.info({
      msg: `received vote request`,
      nodeId: this.nodeId,
      candidateId,
      term,
      lastLogIndex,
      lastLogTerm,
    });

    // Update last heartbeat to avoid starting an election
    this.lastHeartbeat = Date.now();

    // Handle term comparison
    if (term > this.currentTerm) {
      logger.info({
        msg: `discovered higher term in vote request`,
        nodeId: this.nodeId,
        oldTerm: this.currentTerm,
        newTerm: term,
      });

      this.currentTerm = term;
      this.state = "follower";
      this.votedFor = null;
    }

    // Reject if term is less than current term
    if (term < this.currentTerm) {
      logger.info({
        msg: `rejecting vote - term too old`,
        nodeId: this.nodeId,
        messageTerm: term,
        currentTerm: this.currentTerm,
      });

      return {
        term: this.currentTerm,
        voteGranted: false,
      };
    }

    // Check if we can vote for this candidate (Rule 2)
    // - We haven't voted for another candidate in this term
    // - The candidate's log is at least as up-to-date as ours
    const ourLastLogIndex = this.log.length - 1;
    const ourLastLogTerm = this.log[ourLastLogIndex]?.term || 0;

    const canVote =
      (this.votedFor === null || this.votedFor === candidateId) &&
      (lastLogTerm > ourLastLogTerm ||
        (lastLogTerm === ourLastLogTerm && lastLogIndex >= ourLastLogIndex));

    if (canVote) {
      logger.info({
        msg: `granting vote`,
        nodeId: this.nodeId,
        candidateId,
        term,
      });

      this.votedFor = candidateId;
      return {
        term: this.currentTerm,
        voteGranted: true,
      };
    } else {
      logger.info({
        msg: `rejecting vote`,
        nodeId: this.nodeId,
        candidateId,
        term,
        votedFor: this.votedFor,
        ourLastLogTerm,
        ourLastLogIndex,
      });

      return {
        term: this.currentTerm,
        voteGranted: false,
      };
    }
  }

  public processCommand(command: string) {
    logger.info({
      msg: `processing command`,
      nodeId: this.nodeId,
      command,
      state: this.state,
    });

    // Only leaders can process commands
    if (this.state !== "leader") {
      return {
        success: false,
        error: "not the leader",
        leader:
          this.clusterTopology.find(
            (node) => node.nodeId === this.getLeaderId()
          )?.port || null,
      };
    }

    // Add the new command to our log
    const newEntry: LogEntry = {
      term: this.currentTerm,
      command,
    };

    this.log.push(newEntry);

    const entryIndex = this.log.length - 1;

    logger.info({
      msg: `added command to log`,
      nodeId: this.nodeId,
      index: entryIndex,
      term: this.currentTerm,
    });

    // Trigger immediate replication to followers
    this.sendHeartbeats();

    return {
      success: true,
      index: entryIndex,
    };
  }

  private getLeaderId(): number | null {
    // In a real implementation, we might track the current leader
    // For now, just return null if we don't know
    if (this.state === "leader") {
      return this.nodeId;
    }

    return null; // We don't know the leader
  }

  public requestListener: http.RequestListener = async (req, res) => {
    const { method, url } = req;

    console.log(`Received ${method} request for ${url}`);

    // Status endpoint - explicitly allow GET
    if (url === "/status") {
      if (method === "GET") {
        try {
          // Return node status
          const status = {
            nodeId: this.nodeId,
            state: this.state,
            currentTerm: this.currentTerm,
            votedFor: this.votedFor,
            logLength: this.log.length,
            commitIndex: this.commitIndex,
            lastApplied: this.lastApplied,
            leader: this.getLeaderId(),
          };

          res.statusCode = 200;
          res.setHeader("Content-Type", "application/json");
          res.write(JSON.stringify(status));
          res.end();
          return;
        } catch (error) {
          logger.error({
            msg: `error handling status request`,
            error,
          });

          res.statusCode = 500;
          res.end("Internal Server Error");
          return;
        }
      }
    }

    // For all other endpoints, require POST
    if (method !== "POST") {
      res.statusCode = 405;
      res.setHeader("Allow", "POST");
      res.end("Method Not Allowed");
      return;
    }

    try {
      if (url === "/raft/appendEntries") {
        const body: AppendEntriesRequest = JSON.parse(await readBody(req));
        const {
          term,
          leaderId,
          prevLogIndex,
          prevLogTerm,
          entries,
          leaderCommit,
        } = body;

        const response = this.appendEntries(
          term,
          leaderId,
          prevLogIndex,
          prevLogTerm,
          entries,
          leaderCommit
        );

        res.statusCode = 200;
        res.setHeader("Content-Type", "application/json");
        res.write(JSON.stringify(response));
        res.end();
      } else if (url === "/raft/requestVote") {
        const body: RequestVoteRequest = JSON.parse(await readBody(req));
        const { term, candidateId, lastLogIndex, lastLogTerm } = body;

        const response = this.requestVote(
          term,
          candidateId,
          lastLogIndex,
          lastLogTerm
        );

        res.statusCode = 200;
        res.setHeader("Content-Type", "application/json");
        res.write(JSON.stringify(response));
        res.end();
      } else if (url === "/execute") {
        logger.info({
          msg: `received command request`,
          nodeId: this.nodeId,
        });

        const body = await readBody(req);
        const { command } = JSON.parse(body);

        const response = this.processCommand(command);

        res.statusCode = response.success ? 200 : 500;
        res.setHeader("Content-Type", "application/json");
        res.write(JSON.stringify(response));
        res.end();
      } else {
        res.statusCode = 404;
        res.end("Not Found");
      }
    } catch (error) {
      logger.error({
        msg: `error handling request`,
        url,
        error,
      });

      res.statusCode = 500;
      res.end("Internal Server Error");
    }
  };
}
