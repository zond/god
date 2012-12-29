dhash
===

The distributed hash bits and pieces in Go Database. Uses radix for tree structure, discord for routing and timenet for clock synchronization.

# Synchronization

To ensure that all Nodes in the network have the data they should have, each node regularly synchronizes with those nodes that should have
redundant copies of its data.

This is done by comparing the merkle trees of their respective databases [radix.Sync](../radix/sync.go).
