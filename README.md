god
===

A Go database

# TODO

* Xor setop?
* Symdiff setop?
* Add a mirror tree for all trees
 * Let all Puts and Deletes affect the mirror if the proper argument is set
 * Put value|key => key in the mirror tree
 * Add functions using the mirror tree to find ranges or matches to the value
 * Add DHash API endpoints for all this
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

