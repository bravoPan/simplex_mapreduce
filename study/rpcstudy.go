package main

import "fmt"
import "os"
import "log"
import "net/rpc"
import "net"
import "strconv"

type RpcIns struct{
    ipAdd string
    rpcNum int
    alive bool

    // registeredChannel chan string
}

type RegisterArgs struct {
    Worker string
}

type ReplyArgs struct {
    OK bool
}



func (rs *RpcIns) SayHello(args *RegisterArgs, res *ReplyArgs) error{
    // rs.registeredChannel <- args.Worker
    // rs.alive = false
    res.OK = true
    fmt.Printf("hello there")
    return nil
}

func (rs *RpcIns)RSRegister() {
    rpcs := rpc.NewServer()
    rpcs.Register(rs)
    os.Remove(rs.ipAdd)
    l, e := net.Listen("unix", rs.ipAdd) // Listen is a non-block func. Unix Domain Sockets (UDS)
    if e != nil {
        log.Fatal("Reg Error ", rs.ipAdd)
    }

    fmt.Println("start listen ",  rs.ipAdd)

    go func() {
        for rs.alive {
            conn, err := l.Accept()
            if err == nil {
                go func() {
                    rpcs.ServeConn(conn)
                    // conn.Close()
                }()
            }
        }
    }()

    // fmt.Printf("hei")

    // go func() {
    //     for rs.alive {
    //         conn, err := l.Accept()
    //         if err == nil {
    //             go func() {
    //                 rpcs.ServeConn(conn)
    //                 conn.Close() //defer method?
    //             }()
    //         } else {
    //             fmt.Printf("Reg: Accep Error")
    //             break
    //         }
    //     }
    // }()
        // fmt.Printf("Serve Done!")
    // }()
}


func setup(port string) *RpcIns{
    // port:= "/var/tmp/824-" + strconv.Itoa(os.Getuid()) + "/" + "rpc-study"
    rs := new(RpcIns)
    rs.ipAdd = port
    rs.rpcNum = 1
    rs.alive = true
    // rs.registeredChannel = make(chan string)
    rs.RSRegister()
    return rs
}


func main(){
    port := "/var/tmp/824-" + strconv.Itoa(os.Getuid()) + "/" + "rpc-study"
    _ = setup(port)
    // rs.RSRegister()

    // go func(){
    c, _ := rpc.Dial("unix", port)
    defer c.Close()

    // var args RegisterArgs
    // args.worker = "1"
    args := &RegisterArgs{"HI, THERE"}

    var reps ReplyArgs
    reps.OK = false
    // reps := &ReplyArgs{}
    err := c.Call("RpcIns.SayHello", args, &reps)
    // fmt.Printf("%v", reps.ok)
    if err != nil {
        fmt.Printf("RPC fron client fails ", err)
    }

    // }()
    // go func() {

    // }()
}
