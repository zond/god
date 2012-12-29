dhash
===

The distributed hash bits and pieces in Go Database. Uses radix for tree structure, discord for routing and timenet for clock synchronization.

# Inspiration

Go Database is mainly inspired by the scalability of Chord/DHash, and the performance and feature set of Redis.

It tries to couple a performant in memory database with simple yet powerful features with a scalable and operationally simple cluster concept.

# Synchronization

To ensure that all Nodes in the network have the data they should have, each node regularly synchronizes with those nodes that should have
redundant copies of its data.

This is done by comparing their respective databases, and copying any entries with newer timestamps within the relevant range, using [radix.Sync](../../blob/master/radix/sync.go).

# Cleaning

To ensure that all Nodes in the network get rid of the data they should not have, each node regularly cleans its database.

This is done by looking at the first entry it should not own (the first one after its own position), 
checking what other Node should own it, and then doing a destructive sync (again using [radix.Sync](../../blob/master/radix/sync.go)) 
between the misplaced entry and the position of the proper owner.

# Migration

Using non hashed values as keys in the cluster would normally cause severe imbalances between the Nodes, since it would be very unlikely that the spread out position they take by default would represent the actual keys used.

To avoid this, it is normally recommended to use a hashing function, for example [murmur.Hash](../../blob/master/murmur/murmur.go), to create keys for the data.

But to allow those user who so wish to use other data (perhaps ordered, or segmented in some way) as keys, the Nodes will migrate to cover roughly the same amount of data each.

This is done by comparing the owned entries (both tombstones and sub trees and regular data) each node owns to the data its successor owns, and if the predecessor owns too much it will decrease its position to achieve balance.

This is not a perfect mechanism, but it seems to even out the load quite a bit in situations where non hashed keys are used a lot.
