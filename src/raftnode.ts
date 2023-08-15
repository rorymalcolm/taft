import http from "http";
import axios from "axios";
import {
  RequestVoteRequest,
  AppendEntriesRequest,
  RaftNodeState,
  LogEntry,
} from "./types";
import pino from "pino";

const ELECTION_TIMEOUT_MIN = 150;
const HEARTBEAT_TIMEOUT = 100;
const HEARTBEAT_CHECK_INTERVAL = 49;

const logger = pino();

function randomElectionTimeOut() {
  return (
    Math.floor(Math.random() * ELECTION_TIMEOUT_MIN) + ELECTION_TIMEOUT_MIN
  );
}

async function sendRequestVoteRequest(
  port: number,
  request: RequestVoteRequest
): Promise<{ term: number; voteGranted: boolean }> {
  const req = await axios.post(
    `http://localhost:${port}/raft/requestVote`,
    request
  );
  return await req.data;
}

async function sendAppendEntriesRequest(
  port: number,
  request: AppendEntriesRequest
): Promise<{ term: number; success: boolean }> {
  const req = await axios.post(
    `http://localhost:${port}/raft/appendEntries`,
    request
  );
  return await req.data;
}

async function readBody(request: http.IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    let body = "";
    request.on("data", (chunk) => {
      body += chunk;
    });
    request.on("end", () => {
      resolve(body);
    });
    request.on("error", (error) => {
      reject(error);
    });
  });
}

export default class RaftNode {
  private nodeId = 0;
  private port = 0;
  private clusterTopology: { node: number; port: number }[] = [];

  // raft node state and timeout
  private state: RaftNodeState = "follower";
  private electionTimeout: number = this.setElectionTimeout();
  private lastHeartbeat: number = Date.now();

  // persistent state on all servers
  // update on stable storage before responding to RPCs
  private currentTerm: number = 0;
  private votedFor: number | null = null;
  private log: LogEntry[] = [];

  // volatile state on all servers
  private commitIndex: number = 0;
  private lastApplied: number = 0;

  // volatile state on leaders
  private nextIndex: number[] = [];
  private matchIndex: number[] = [];

  constructor(
    nodeId: number,
    port: number,
    cluster: { node: number; port: number }[]
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
    this.heartbeat();
  }

  getNodeId() {
    return this.nodeId;
  }

  getPort() {
    return this.port;
  }

  getState() {
    return this.state;
  }

  private setElectionTimeout() {
    const timeout = randomElectionTimeOut();
    this.electionTimeout = timeout;
    setTimeout(() => {
      this.setElectionTimeout();
      if (this.lastHeartbeat + HEARTBEAT_TIMEOUT < Date.now()) {
        this.processCandidateTransition(timeout);
      }
    }, timeout);
    return timeout;
  }

  private async processCandidateTransition(timeout: number) {
    this.state = "candidate";
    this.currentTerm++;
    this.votedFor = null;
    this.lastHeartbeat = Date.now();
    const startOfElection = Date.now();
    const quorum = Math.floor(this.clusterTopology.length / 2) + 1;
    logger.debug({ msg: `node is now a candidate`, nodeId: this.nodeId });
    let voteCount = 1; // we always vote for ourselves
    for (const node of this.clusterTopology) {
      if (node.node !== this.nodeId && this.state === "candidate") {
        logger.debug({
          msg: `sending vote request`,
          nodeId: this.nodeId,
          targetNodeId: node.node,
          port: node.port,
        });
        const voteRequestResponse = await sendRequestVoteRequest(node.port, {
          term: this.currentTerm,
          candidateId: this.nodeId,
          lastLogIndex: this.log.length - 1,
          lastLogTerm: this.log[this.log.length - 1]?.term || 0,
        });
        logger.debug({
          msg: `got vote response`,
          nodeId: this.nodeId,
          targetNodeId: node.node,
          port: node.port,
          voteRequestResponse,
        });
        if (voteRequestResponse.term > this.currentTerm) {
          this.currentTerm = voteRequestResponse.term;
          this.state = "follower";
          this.votedFor = null;
          return;
        }
        if (voteRequestResponse.voteGranted) {
          voteCount++;
          if (startOfElection + timeout! < Date.now()) {
            this.processCandidateTransition(randomElectionTimeOut());
          } else if (voteCount >= quorum && this.state === "candidate") {
            this.state = "leader";
            logger.info({
              msg: `node is now the leader`,
              nodeId: this.nodeId,
              term: this.currentTerm,
            });
            this.nextIndex = [];
            this.matchIndex = [];
            for (let i = 0; i < this.clusterTopology.length; i++) {
              this.nextIndex.push(this.log.length);
              this.matchIndex.push(0);
            }
            this.appendEntriesToAll();
          }
        }
      }
    }
    this.setElectionTimeout();
  }

  private appendEntriesToAll() {
    for (const node of this.clusterTopology) {
      if (node.node !== this.nodeId) {
        this.appendEntriesToNode(node.node);
      }
    }
  }

  private async appendEntriesToNode(nodeId: number) {
    const node = this.clusterTopology.find((n) => n.node === nodeId);
    if (!node) {
      return;
    }
    await sendAppendEntriesRequest(node.port, {
      term: this.currentTerm,
      leaderId: this.nodeId,
      prevLogIndex: this.nextIndex[nodeId],
      prevLogTerm: this.log[this.nextIndex[nodeId] - 1]?.term || 0,
      entries: this.log.slice(this.nextIndex[nodeId]),
      leaderCommit: this.commitIndex,
    });
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
    // 1. Reply false if term < currentTerm - this occurs when the leader is out of date and
    // is trying to append entries to a follower that has already moved on to a new term
    logger.info({
      msg: `received append entries request`,
      nodeId: this.nodeId,
      leaderId,
      term,
      prevLogIndex,
      prevLogTerm,
      entries,
    });
    this.lastHeartbeat = Date.now();
    if (this.state === "candidate") {
      this.state = "follower";
    }
    if (term > this.currentTerm) {
      this.currentTerm = term;
      this.votedFor = null;
      this.state = "follower";
      return {
        term: this.currentTerm,
        success: false,
      };
    }
    if (term < this.currentTerm) {
      logger.info({
        msg: `append entries request rejected as term is out of date`,
        nodeId: this.nodeId,
        leaderId,
        term,
        currentTerm: this.currentTerm,
      });
      return {
        term: this.currentTerm,
        success: false,
      };
    }

    // 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
    // - this means the leader is trying to append entries to a follower that doesn't have the same log
    // as the leader
    if (this.log[prevLogIndex]?.term !== prevLogTerm) {
      logger.info({
        msg: `append entries request rejected as log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm`,
        nodeId: this.nodeId,
        leaderId,
        term,
        prevLogIndex,
        prevLogTerm,
        entries,
      });
      return {
        term: this.currentTerm,
        success: false,
      };
    }

    // 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
    // - this means the leader is trying to append entries to a follower that has a different log than the leader
    // - this is a bit more complicated, because we need to delete the entry at prevLogIndex and all that follow it
    // - we can do this by slicing the log at prevLogIndex + 1
    // - we can then append the new entries to the log
    // - we can then set commitIndex to the minimum of leaderCommit and the index of the last new entry
    for (const entry of entries) {
      logger.info({
        msg: `appending entry`,
        nodeId: this.nodeId,
        entry,
      });
      if (this.log[prevLogIndex + 1]?.term !== entry.term) {
        logger.info({
          msg: `deleting existing entry and all that follow it`,
          nodeId: this.nodeId,
          entry,
        });
        this.log = this.log.slice(0, prevLogIndex + 1);
        break;
      }
      prevLogIndex++;
    }
    prevLogTerm = this.log[prevLogIndex]?.term || 0;
    // 4. Append any new entries not already in the log
    // - this is a happy path, where the leader is trying to append entries to a follower that has the same log as the leader
    // - we can simply append the new entries to the log
    this.log.push(...entries);

    logger.info({
      msg: `appended entries`,
      nodeId: this.nodeId,
      entryCount: entries.length,
      logSize: this.log.length,
    });

    // 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
    // - we can then set commitIndex to the minimum of leaderCommit and the index of the last new entry
    if (leaderCommit > this.commitIndex) {
      this.commitIndex = Math.min(leaderCommit, this.log.length - 1);
    }
    logger.info({
      msg: `current log`,
      nodeId: this.nodeId,
      log: this.log,
    });
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
    // 1. Reply false if term < currentTerm
    // - this occurs when the candidate is out of date and is trying to become leader
    this.lastHeartbeat = Date.now();
    if (term > this.currentTerm) {
      this.currentTerm = term;
      this.votedFor = null;
      this.state = "follower";
    }
    if (term < this.currentTerm) {
      return {
        term: this.currentTerm,
        voteGranted: false,
      };
    }
    // 2. If votedFor is null or candidateId, and candidate’s log is at
    // least as up-to-date as receiver’s log, grant vote
    // - this is a happy path, where the candidate is up to date and can become leader
    // - we can simply set votedFor to candidateId and return true
    if (
      this.votedFor === null ||
      this.votedFor === candidateId ||
      lastLogTerm > this.log[this.log.length - 1]?.term ||
      (lastLogTerm === this.log[this.log.length - 1]?.term &&
        lastLogIndex >= this.log.length - 1)
    ) {
      this.votedFor = candidateId;
      return {
        term: this.currentTerm,
        voteGranted: true,
      };
    }
    return {
      term: this.currentTerm,
      voteGranted: false,
    };
  }

  private heartbeat() {
    setTimeout(() => this.heartbeat(), HEARTBEAT_CHECK_INTERVAL);
    if (
      this.state !== "leader" ||
      Date.now() - this.lastHeartbeat < HEARTBEAT_CHECK_INTERVAL
    ) {
      return;
    }
    for (const node of this.clusterTopology) {
      if (node.node !== this.nodeId) {
        this.lastHeartbeat = Date.now();
        this.appendEntriesToNode(node.node);
      }
    }
  }

  private processCommand(command: string) {
    if (this.state !== "leader") {
      return {
        success: false,
        error: "not the leader",
      };
    }
    this.log.push({
      term: this.currentTerm,
      command,
    });
    this.appendEntries(
      this.currentTerm,
      this.nodeId,
      this.log.length - 2 || 0,
      this.log[this.log.length - 2]?.term || 0,
      [
        {
          term: this.currentTerm,
          command,
        },
        ...(this.log.slice(this.log.length - 1) || []),
      ],
      this.commitIndex
    );
    this.appendEntriesToAll();
    return {
      success: true,
    };
  }

  requestListener: http.RequestListener = async (req, res) => {
    const { method, url } = req;
    if (method !== "POST") {
      res.statusCode = 405;
    }
    if (url === "/raft/appendEntries") {
      this.state = "follower";
      const body: AppendEntriesRequest = JSON.parse(await readBody(req));
      const {
        term,
        leaderId,
        prevLogIndex,
        prevLogTerm,
        entries,
        leaderCommit,
      } = body;
      const { term: responseTerm, success } = this.appendEntries(
        term,
        leaderId,
        prevLogIndex,
        prevLogTerm,
        entries,
        leaderCommit
      );
      res.statusCode = 200;
      res.setHeader("Content-Type", "application/json");
      res.write(
        JSON.stringify({
          term: responseTerm,
          success,
        })
      );
    } else if (url === "/raft/requestVote") {
      const body: RequestVoteRequest = JSON.parse(await readBody(req));
      const { term, candidateId, lastLogIndex, lastLogTerm } = body;
      const { term: responseTerm, voteGranted } = this.requestVote(
        term,
        candidateId,
        lastLogIndex,
        lastLogTerm
      );
      res.statusCode = 200;
      res.setHeader("Content-Type", "application/json");
      res.write(
        JSON.stringify({
          term: responseTerm,
          voteGranted,
        })
      );
    } else if (url === "/execute") {
      const body = await readBody(req);
      const { command } = JSON.parse(body);
      const cmdOutput = this.processCommand(command);
      if (cmdOutput.success) {
        res.statusCode = 200;
      } else {
        res.statusCode = 500;
        res.setHeader("Content-Type", "application/json");
        res.write(JSON.stringify({ cmdOutput }));
      }
    } else {
      res.statusCode = 404;
    }
    res.end();
  };
}
