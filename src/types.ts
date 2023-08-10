export type LogEntry = {
  term: number;
  command: string;
};

export type AppendEntriesRequest = {
  term: number;
  leaderId: number;
  prevLogIndex: number;
  prevLogTerm: number;
  entries: LogEntry[];
  leaderCommit: number;
};

export type RequestVoteRequest = {
  term: number;
  candidateId: number;
  lastLogIndex: number;
  lastLogTerm: number;
};

export type RaftNodeState = "follower" | "candidate" | "leader";
