# kusari

## objective

kusari is (to be) a decentralized file-sync application for use on Linux. The aim is to keep linux configurations in sync via a p2p mechanism - without the use of a centralized node. It is currently a big **WIP** and comprises only those components that track and persist filesystem events for configured directories. While my hope is that this evolves into a usable tool, it is - as of right now - a learning project. I wanted to learn Go. I wanted to build something somewhat useful. This is what we got.

My primary goal of learning is why I haven't yet depended on too many 3rd party modules. If it was reasonable for me to implement it myself, without learning something other than Go, I did.  I how have a decent grasp of the language fundamentals and quirks. I might, therefore, branch out, as it were.

"kusari" (pronounced: /kuˈsaɹi/) is Japanese for "chain(s)" reflecting my image of linking hosts together and also tracking chains of filesystem events.

## big TODOs

 - documentation & code comments (mostly code comments)
 - add (very) basic sync functionality
	 + decide on communication mechanism (RPC/REST/?)
	 + add logic to create expected state from fs event store
	 + sync state to most recent server (assuming no conflicts have occurred)
	 + do a debounced sync on any filesystem change
 - more robustly test `scry` filesystem watcher
	 + some fuzzing might be nice
	 + more edge cases
 - restructure
	 + do some research on Go project structuring standards and mimic