god
===

A Go database

# TODO

* Byte and Tree on the same key
 * Test it
* Deletes
 * Build tentative delete into Tree
  * See that tentatively deleted entries are still there, even if never returned
	* See that they get deleted for REAL on REAL deletes
 * Make them not included in slices and index calculations
* Persistence
 * Move the logging inside radix.Tree lock
 * Functionality
  * Snapshot
	* Clean deletions
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

