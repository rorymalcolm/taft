# taft - toy raft in typescript

## what is this?

this is a toy implementation of the raft consensus algorithm in typescript. it is not intended for production use, but rather as a learning tool. It is based on the [raft paper](https://raft.github.io/raft.pdf) and the [raft dissertation](https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf).

## how do i use it?

currently you run the script at `./scripts/test.sh` and see the output, we currently do not reset the election timeouts so we constantly have elections. this is a work in progress. we will eventually have a cli so you can run a cluster of nodes and send messages to them.

## what is raft?

raft is a protocol for distributed consensus. it is a way for a cluster of nodes to agree on a value. it is used in many distributed systems to ensure that all nodes in a cluster agree on the same value. it is used in many distributed databases, such as [etcd](https://etcd.io/) and [cockroachdb](https://www.cockroachlabs.com/).
