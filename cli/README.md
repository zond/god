cli
===

A simple command line interface to http://github.com/zond/god/client to try out the Go Database from a shell.

# Usage

    cli [-ip 127.0.0.1] [-port 9191] [-enc string] COMMAND

The -ip and -port options are, not surprisingly, the address and port of a node in the database cluster.

The command is one of:

* `` to display address and position of all nodes in the cluster.
* `describe KEY` to show details for the node at position KEY.
* `describeAll` to show details for all nodes in the cluster.
* `get KEY` to fetch the value under KEY.
* `put KEY VALUE` to put VALUE under KEY.
* `del KEY` to remove the value under KEY.
* `subGet KEY SUBKEY` to fetch the value under SUBKEY in the sub tree under KEY.
* `subPut KEY SUBKEY VALUE` to put VALUE under SUBKEY in the sub tree under KEY.
* `subDel KEY SUBKEY` to remove the value under SUBKEY in the sub tree under KEY.
* `kill` to remove everything in the database. This will not keep tombstones, so make sure not to wake up old nodes with old data after this, or the data will be reanimated and haunt you.
* `clear` to remove all byte values (not sub trees) in the database.
* `subClear KEY` to remove the sub tree under KEY.

