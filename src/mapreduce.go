package mapreduce

import "fmt"
import "os"
import "log"
import "strconv"
import "encoding/json"
import "sort"
import "container/list"
import "net/rpc"
import "net"
import "bufio"
import "hash/fnv"
import "sync"

type MapReduce struct {
  nMap int // Number of Map jobs
  nReduce int  // Number of Reduce jobs
  file string  // Name of input file
  MasterAddress string
  registerChannel chan string
  DoneChannel chan bool
  alive bool
  l net.Listener
  stats *list.List

  // Map of registered workers that you need to keep up to date
  Workers map[string]*WorkerInfo

  // mutual exclusion
  mux *sync.Mutex // @see https://tour.golang.org/concurrency/9
}

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    n, err = fmt.Printf(format, a...)
  }
  return
}

// Map and Reduce deal with <key, value> pairs:
type KeyValue struct {
  Key string
  Value string
}

// rpc from worker to call mr to set registerChannel to current workers
func (mr *MapReduce) Register(args *RegisterArgs, res *RegisterReply) error {
  DPrintf("Register: worker %s\n", args.Worker)
  mr.registerChannel <- args.Worker
  res.OK = true
  return nil
}

// shudown master server
func (mr *MapReduce) Shutdown(args *ShutdownArgs, res *ShutdownReply) error {
  DPrintf("Shutdown: registration server\n")
  mr.alive = false
  mr.l.Close()    // causes the Accept to fail
  return nil
}


// Name of the file that is the input for map job <MapJob>
func MapName(fileName string, MapJob int) string {
  return "mrtmp." +  fileName + "-" + strconv.Itoa(MapJob)
}

func (mr *MapReduce) Split(fileName string) {
  fmt.Printf("Split %s\n", fileName)
  infile, err := os.Open(fileName);
  if err != nil {
    log.Fatal("Split: ", err);
  }
  defer infile.Close()
  fi, err := infile.Stat();
  if err != nil {
    log.Fatal("Split: ", err);
  }
  size := fi.Size()
  nchunk := size / int64(mr.nMap);
  nchunk += 1

  outfile, err := os.Create(MapName(fileName, 0))
  if err != nil {
    log.Fatal("Split: ", err);
  }
  writer := bufio.NewWriter(outfile)
  m := 1
  i := 0

  scanner := bufio.NewScanner(infile)
  for scanner.Scan() {
    if (int64(i) > nchunk * int64(m)) {
      writer.Flush()
      outfile.Close()
      outfile, err = os.Create(MapName(fileName, m))
      writer = bufio.NewWriter(outfile)
      m += 1
    }
    line := scanner.Text() + "\n"
    writer.WriteString(line)
    i += len(line)
  }
  writer.Flush()
  outfile.Close()
}

// Remove single file
func RemoveFile(n string) {
  err := os.Remove(n)
  if err != nil {
    log.Fatal("CleanupFiles ", err);
  }
}

//cleanup all map and reduced files
func (mr *MapReduce) CleanupFiles() {
  for i := 0; i < mr.nMap; i++ {
    RemoveFile(MapName(mr.file, i))
    for j := 0; j < mr.nReduce; j++ {
      RemoveFile(ReduceName(mr.file, i, j))
    }
  }
  for i := 0; i < mr.nReduce; i++ {
    RemoveFile(MergeName(mr.file, i))
  }
  RemoveFile("mrtmp." + mr.file)
}

func (mr *MapReduce) StartRegistrationServer() {
  rpcs := rpc.NewServer()
  rpcs.Register(mr)
  os.Remove(mr.MasterAddress)   // only needed for "unix"
  l, e := net.Listen("unix", mr.MasterAddress)
  if e != nil {
    log.Fatal("RegstrationServer", mr.MasterAddress, " error: ", e)
  }
  mr.l = l

  // now that we are listening on the master address, can fork off
  // accepting connections to another thread.
  go func() {
    for mr.alive {
      conn, err := mr.l.Accept()
      if err == nil {
        go func() {
          rpcs.ServeConn(conn)
          conn.Close()
        }()
      } else {
        DPrintf("RegistrationServer: accept error")
        break
      }
    }
    DPrintf("RegistrationServer: done\n")
  }()
}

func ReduceName(fileName string, MapJob int, ReduceJob int) string {
  return MapName(fileName, MapJob) + "-" + strconv.Itoa(ReduceJob)
}

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}

func DoMap(JobNumber int, fileName string,
           nreduce int, Map func(string) *list.List) {
  name := MapName(fileName, JobNumber)
  file, err := os.Open(name)
  if err != nil {
    log.Fatal("DoMap: ", err);
  }
  fi, err := file.Stat();
  if err != nil {
    log.Fatal("DoMap: ", err);
  }
  size := fi.Size()
  fmt.Printf("DoMap: read split %s %d\n", name, size)
  b := make([]byte, size);
  _, err = file.Read(b);
  if err != nil {
    log.Fatal("DoMap: ", err);
  }
  file.Close()
  res := Map(string(b))
  // XXX a bit inefficient. could open r files and run over list once
  for r := 0; r < nreduce; r++ {
    file, err = os.Create(ReduceName(fileName, JobNumber, r))
    if err != nil {
      log.Fatal("DoMap: create ", err);
    }
    enc := json.NewEncoder(file) //returns a new encoder that writers to file
    for e := res.Front(); e != nil; e = e.Next() {
      kv := e.Value.(KeyValue)
      if hash(kv.Key) % uint32(nreduce) == uint32(r) {
        err := enc.Encode(&kv); //write v into the stream
        // err := enc.Encode("1")
        if err != nil {
          log.Fatal("DoMap: marshall ", err);
        }
      }
    }
    file.Close()
  }
}

func MergeName(fileName string,  ReduceJob int) string {
  return "mrtmp." +  fileName + "-res-" + strconv.Itoa(ReduceJob)
}

func DoReduce(job int, fileName string, nmap int,
              Reduce func(string,*list.List) string) {
  kvs := make(map[string]*list.List)
  for i := 0; i < nmap; i++ {
    name := ReduceName(fileName, i, job)
    fmt.Printf("DoReduce: read %s\n", name)
    file, err := os.Open(name)
    if err != nil {
      log.Fatal("DoReduce: ", err);
    }
    dec := json.NewDecoder(file)
    for {
      var kv KeyValue
      err = dec.Decode(&kv); //
      if err != nil {
        break;
      }
      _, ok := kvs[kv.Key]
      if !ok {
        kvs[kv.Key] = list.New()
      }
      kvs[kv.Key].PushBack(kv.Value)
    }
    file.Close()
  }
  var keys []string
  for k := range kvs {
    keys = append(keys, k)
  }
  sort.Strings(keys)
  p := MergeName(fileName, job)
  file, err := os.Create(p)
  if err != nil {
    log.Fatal("DoReduce: create ", err);
  }
  enc := json.NewEncoder(file)
  for _, k := range keys {
    res := Reduce(k, kvs[k])
    enc.Encode(KeyValue{k, res})
  }
  file.Close()
}


func InitMapReduce(nmap int, nreduce int,
                   file string, master string) *MapReduce {
  mr := new(MapReduce)
  mr.nMap = nmap
  mr.nReduce = nreduce
  mr.file = file
  mr.MasterAddress = master
  mr.alive = true
  mr.registerChannel = make(chan string)
  mr.DoneChannel = make(chan bool)

  // initialize any additional state here
  mr.Workers = make(map[string]*WorkerInfo)
  mr.mux = &sync.Mutex{}

  return mr
}

func MakeMapReduce(nmap int, nreduce int,
                   file string, master string) *MapReduce {
  // setup Master Server to listen on the port
  mr := InitMapReduce(nmap, nreduce, file, master)
  // fmt.Printf(master)
  mr.StartRegistrationServer()
  go mr.Run()
  return mr
}

// merge the reduced files
func (mr *MapReduce) Merge() {
  DPrintf("Merge phase")
  kvs := make(map[string]string)
  for i := 0; i < mr.nReduce; i++ {
    p := MergeName(mr.file, i)
    fmt.Printf("Merge: read %s\n", p)
    file, err := os.Open(p)
    if err != nil {
      log.Fatal("Merge: ", err);
    }
    dec := json.NewDecoder(file)
    for {
      var kv KeyValue
      err = dec.Decode(&kv);
      if err != nil {
        break;
      }
      kvs[kv.Key] = kv.Value
    }
    file.Close()
  }
  var keys []string
  for k := range kvs {
    keys = append(keys, k)
  }
  sort.Strings(keys)

  // a normal file..
  file, err := os.Create("mrtmp." + mr.file)
  if err != nil {
    log.Fatal("Merge: create ", err);
  }
  w := bufio.NewWriter(file)
  for _, k := range keys {
    fmt.Fprintf(w, "%s: %s\n", k, kvs[k])
  }
  w.Flush()
  file.Close()
}

// shutdown Master to shutdown
func (mr *MapReduce) CleanupRegistration()  {
    args := &ShutdownArgs{}
    var reply ShutdownReply
    ok := call(mr.MasterAddress, "MapReduce.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("Cleanup: RPC %s error\n", mr.MasterAddress)
    }
    DPrintf("CleanupRegistration: done\n");
}


// Run jobs in parallel, assuming a shared file system
func (mr *MapReduce) Run() {
  fmt.Printf("Run mapreduce job %s %s\n", mr.MasterAddress, mr.file)

  mr.Split(mr.file) // split the input file to 100(nMap) files
  mr.stats = mr.RunMaster() // run master to assign workers to finish map/reduce
  mr.Merge() // then merge all files
  mr.CleanupRegistration()

  fmt.Printf("%s: MapReduce done\n", mr.MasterAddress)

  mr.DoneChannel <- true
}
