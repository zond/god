cli
===

A simple command line interface to http://github.com/zond/god/client to try out the Go Database from a shell.

# Usage

    cli [-ip 127.0.0.1] [-port 9191] [-enc string] COMMAND

The -ip and -port options are, non surprisingly, the address and port of a node in the database cluster.

The command is one of

* get KEY
 * Fetch the value under KEY.
* put KEY VALUE
 * Put VALUE under KEY.
* del KEY
 * Remove the value under KEY.
* subGet KEY SUBKEY
 * Fetch the value under SUBKEY in the sub tree under KEY.
* subPut KEY SUBKEY VALUE
 * Put VALUE under SUBKEY in the sub tree under KEY.
* subDel KEY SUBKEY
 * Remove the value under SUBKEY in the sub tree under KEY.


