# taft - toy raft in typescript

## what is this?

this is a toy implementation of the raft consensus algorithm in typescript. it is not intended for production use, but rather as a learning tool. it is based on the [raft paper](https://raft.github.io/raft.pdf) and the [raft dissertation](https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf).

## setup

to get started with taft:

```bash
# install dependencies
yarn install

# build the typescript code
yarn build
```

## running the tests

we've built a test harness that spins up a cluster of nodes and verifies raft properties:

```bash
# run all tests
yarn test
```

the test harness will automatically:

- start 5 raft nodes
- wait for leader election
- run various test scenarios
- clean up all processes when done

## what do the tests verify?

our test suite verifies four key properties of raft:

### 1. leader election

this test checks if the cluster can properly elect a leader, and that when a leader fails, a new leader is elected.

```
🧪 RUNNING TEST: Leader Election
```

the test:

- identifies the current leader
- stops the leader
- verifies a new leader is elected from the remaining nodes
- restarts the old leader
- checks that the cluster is stable

### 2. log replication

this test verifies that commands sent to the leader are replicated to all followers.

```
🧪 RUNNING TEST: Log Replication
```

the test:

- sends several commands to the leader ("set x 1", "set y 2", "set z 3")
- verifies all nodes eventually have the same log length
- confirms replication is working properly

### 3. log consistency after leader change

this test checks that logs remain consistent even when leadership changes.

```
🧪 RUNNING TEST: Log Consistency After Leader Change
```

the test:

- sends commands to the current leader
- stops the leader to force an election
- sends more commands to the new leader
- verifies all nodes eventually converge to the same log state
- restarts the old leader and confirms it catches up

### 4. network partition resilience

this test verifies that the cluster continues to operate during network partitions.

```
🧪 RUNNING TEST: Network Partition
```

the test:

- creates a "partition" by stopping a minority of nodes
- verifies the leader remains stable (as it's in the majority partition)
- confirms commands can still be processed
- heals the partition by restarting nodes
- verifies all nodes eventually converge

## how the cluster works

each node in our raft implementation:

- starts in follower state
- transitions to candidate if it doesn't hear from a leader
- becomes leader if it gets votes from a majority
- as leader, replicates commands to followers
- steps down if it discovers a higher term

nodes communicate via http endpoints:

- `/raft/requestVote` - used during elections
- `/raft/appendEntries` - used for log replication and heartbeats
- `/execute` - submit commands to the cluster
- `/status` - get the current state of a node

## troubleshooting

if the tests fail with "EADDRINUSE" errors, you likely have orphaned raft processes:

```bash
# find and kill processes using ports 3000-3004
lsof -i :3000-3004 | grep LISTEN
kill -9 [PID]
```

the test harness has built-in cleanup that should prevent this, but if you ctrl+c during a test, some processes might remain.

## what is raft?

raft is a protocol for distributed consensus. it is a way for a cluster of nodes to agree on a value. it is used in many distributed systems to ensure that all nodes in a cluster agree on the same value. it is used in many distributed databases, such as [etcd](https://etcd.io/) and [cockroachdb](https://www.cockroachlabs.com/).

the key insight of raft is to decompose consensus into more understandable sub-problems:

- leader election (using randomized timeouts)
- log replication (leader-driven)
- safety (voting restrictions & commitment rules)
