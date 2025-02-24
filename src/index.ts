import yargs from "yargs";
import RaftNode from "./raftnode";
import http from "http";

const args = yargs(process.argv.slice(2))
  .command("start", "Starts the server", (yargs) => {
    return yargs
      .option("node", {
        alias: "n",
        type: "number",
        description: "Node ID to start the server with",
        requiresArg: true,
      })
      .option("port", {
        alias: "p",
        type: "number",
        description: "Port to start the server listening on",
        requiresArg: true,
      })
      .option("cluster", {
        alias: "c",
        type: "string",
        description:
          "A topology of the cluster, in the format of a JSON array of objects with a port and node property",
        requiresArg: true,
      })
      .demandOption(["node", "port", "cluster"]);
  })
  .help()
  .parseSync();

if (args._.includes("start")) {
  console.log(
    `Starting node ${args.node} on port ${args.port} with cluster ${args.cluster}`
  );
  const cluster = JSON.parse(args.cluster as string) as {
    nodeId: number;
    port: number;
  }[];
  const raftClient = new RaftNode(
    args.node as number,
    args.port as number,
    cluster
  );
  console.log(`listening on port ${args.port}`);
  http.createServer(raftClient.requestListener).listen(args.port as number);
} else {
  console.log("Unknown command");
}
