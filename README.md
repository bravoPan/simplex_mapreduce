# simplex_mapreduce
A golang implemented map reduce

### Map-Reduce: Split, Map, Reduce, Merge

- First, Split the file into nMap(100) pieces, so that we have `1, 2, 3, ... 100` pieces files.
- Second, Count the frequency(1) for every file and save each piece into nReduce(10) files
```
1-1, 1-2, ...1-10
2-1, 2-2, ...2-10
100-1........100-10
```
- Then Reduce (count the frequency for the same key) every column and Sort into one file, so we have 10 files finally.
- Finally, we Merge all Reduced files into one file from sorting nReduced  files.


### Distributed Model

Implemented by Golang RPC. First we need a MapReduce Struct to contain some information about Workers, the first is `map(int)string WorkerInfo` to record the workerId to IpAddress. And buffered channel `RegisterWorkers string chan` to save all workers available. It also needs to setup a server to be called from the workers to register.

The initialization code could be like this:

```
'''
In Worker.go, use go to register workers Concurrently, but need to avoid accessing the resources
 in Master with conflict. And also every worker needs to setup a server to be called from the
  master to DoJob(Map/Reduce) later on.
'''

func RegisterAtMaster(workerId) {
    call(Master, "Master.Register", workerId)
}

// In Master.go
func RegisterForWorker(workerId) {
    mr.RegisterWorkers <- i //buffer it in the channel
    mutex.Lock() // Lock here to avoid the mutual exclusion
    WorkerInfo[i] = IpAdd(i)
    mutex.Unlock()
}
```

Then when we run `RunMaster` to make the Master to process Map and Reduce work Concurrently. The code we achieve the synchronization is like this:

```
func RunMaster() {
    taskCompleted = make(int channel, nMap) //used to buffer before the Map/Reduce to exit
    for i:=0; i < nMap; i++{
        i := i
        go func(){
            wk := <- mr.WorkerChannel //get one from buffer
            mutex.Lock() // Lock to avoid mutex exclusion
            ok := call(wk, "Worker.DoMap") // call workers to execute DoMap Job
            if !ok {
                error here
            } else{
                taskCompleted <- i //stuff current goroutine num to buffered chan
                mr.WorkerChannel <- wk // stuff back the channel for next goroutine use
                return
            }
        }()
    }

    for i:=0; i <nMap; i++{
        <- taskCompleted
    }

    // Same Logic as the Reduce Func
    KillWorkers()
}
```

Also here is a note, the transportation layer for the server we used is UDS(unix domain socket) since we execute all the program in the same system, there's no need to exchange id via external network, but this is also easy to be tuned.

### Worker Failure and Unit Test: One and Many

To make a Single Worker Failure after 10 Jobs execution fail, in the unit test file `_test.go` we create a function `TestSingleFail`. Then in the `Worker` struct, we assign a `nRPC=10` by initialization. Here is the Code for `RunWorker`:

```
for wk.nRPC != 0 {
    conn, err := wk.l.Accept()
    if err == nil {
        wk.nRPC -= 1
        go rpcs.ServeConn(conn) // This ServeConn method runs on a single connection
        wk.nJobs += 1
    } else {
        break
    }
}
```

Then we can this test on the failure `Worker` case. If the Worker fails, then there's a var in the Worker's struct to record the status of itself, after a period of the time, checks it for status, if the worker is dead, then reregister it at the Master thread.

```
//this funciton will run every period of time
func CheckAlive() {
    for i in all Workers{
        if i.alive == false {
            call(Master, "Master.Register") // to register itself
        }
    }
}
```
