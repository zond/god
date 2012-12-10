god
===

A Go database

# TODO

* Client API
 * SubSize
 * SubClear
 * Inter/Union/Diff
 * StoreInter/StoreUnion/StoreDiff
* Persistence
 * Put logging inside radix.Tree lock
 * Functionality
  * Snapshot
	 * Make Persistence handle the entire snapshotting
	  * Create a map[string]struct{Val:[]byte,Ver:int64} where the keys are "[Key]/[SubKey]"
		* Spool all source logs into the map
		* Spool the map into the snapshot
 * Manual tests
 * Automatic tests
* Web interface
 * Functionality
  * Diagnose and understand cluster from web interface
	* See and interact with content from web interface
* jsonrpc API
 * Functionality
  * Export the DHash.[METHODNAME] over jsonrpc as well as gob-rpc
* Quality
 * Code review
 * Code comments
 * Architectural documentation
* Example app
 * Implementation
 * Documentation

