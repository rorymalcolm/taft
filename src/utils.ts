import axios from "axios";
import { AppendEntriesRequest, RequestVoteRequest } from "./types";
import http from "http";

export async function sendRequestVoteRequest(
  port: number,
  request: RequestVoteRequest
): Promise<{ term: number; voteGranted: boolean }> {
  try {
    const req = await axios.post(
      `http://localhost:${port}/raft/requestVote`,
      request
    );
    return await req.data;
  } catch (e) {
    return {
      term: -1,
      voteGranted: false,
    };
  }
}

export async function sendAppendEntriesRequest(
  port: number,
  request: AppendEntriesRequest
): Promise<{ term: number; success: boolean }> {
  try {
    const req = await axios.post(
      `http://localhost:${port}/raft/appendEntries`,
      request
    );
    return await req.data;
  } catch (e) {
    return {
      term: -1,
      success: false,
    };
  }
}

export async function readBody(request: http.IncomingMessage): Promise<string> {
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
