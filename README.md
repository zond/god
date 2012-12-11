god
===

A Go database

# TODO

* Consider the Redis case
 * Set intersection with scores instead of keys
  * Provides lots of mathematically interesting options
	* Can possibly be implemented by the client just as well?
	 * Fetch one set
	 * Iterate pagewise over the other set and union, inter or diff on your own
 * Scores are not even possible here (same score = same key conflict)
* Client API
 * Inter/Union/Diff
  * Automated tests
 * StoreInter/StoreUnion/StoreDiff
* Persistence
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

