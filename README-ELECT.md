# Election Library

Election libary is in the [raft library](https://github.com/etcd-io/raft) and includes a fork of raft.go(election.go), rawnode.go(rawnode_election.go), node.go(node_election).
Compared with corresponding part in raft library, election refactors the log structure and remove some unnecessary codes, which includes the Propose/ProposeConfChange/TransferLeader/ReadIndex, and maintains other main features.
