
Files
- mr-out-x
- 1 line per func as "%v-%v" (k,v)
- stored in cwd
- int files as mr-x-y

Map
- keys into nReduce buckets 

Reduce


## Task Process

**The coordinator should notice if a worker hasn't completed its task in a reasonable amount of time (for this lab, use ten seconds), and give the same task to a different worker.

Coordinator
- Initialize tasks
- Check if a task has taken a long time; re-assign it to someone

Worker
- Get map task
    - Check if we have tasks pending
    - If so return the task
        - Process the task
        - Report completion
    - If no more tasks (all done), report all done, go to reduce

- Get reduce task
    - Process the task
    - Report completion