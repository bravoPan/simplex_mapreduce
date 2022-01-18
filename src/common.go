package mapreduce

import "fmt"
import "net/rpc"

const (
  Map = "Map"
  Reduce = "Reduce"
)

type JobType string

type DoJobArgs struct {
  File string
  Operation JobType
  JobNumber int       // this job's number
  NumOtherPhase int   // total number of jobs in other phase (map or reduce)
}

type ShutdownArgs struct {
}

type ShutdownReply struct {
  Njobs int
  OK bool
}

type DoJobReply struct {
  OK bool
}

type RegisterArgs struct {
  Worker string
}

type RegisterReply struct {
  OK bool
}

func call(srv string, rpcname string,
          args interface{}, reply interface{}) bool {
  c, errx := rpc.Dial("unix", srv)
  if errx != nil {
    return false
  }
  defer c.Close()

  err := c.Call(rpcname, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}
