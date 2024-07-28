
## Requirements

Clients can send 3 diff RPCs to the k/v server 
- Put(key, value)
    - Installs or replaces values for a particular key in the map
- Append(key, arg)
    - Appends arg to key's value and returns the old value
- Get(key)
    - Non-existing return empty string

Details
- Server maintains an in-memory map of kv pairs.
- Kv are strings. 
- Each client talks to the server through a Clerk. The Clerk manages the RPC interactions.
- The Clerk must make the interactions linearizable. Same as if they executed one at a time. 


## Task 1

Your first task is to implement a solution that works when there are no dropped messages.

You'll need to add RPC-sending code to the Clerk Put/Append/Get methods in client.go, and implement Put, Append() and Get() RPC handlers in server.go.

You have completed this task when you pass the first two tests in the test suite: "one client" and "many clients".

