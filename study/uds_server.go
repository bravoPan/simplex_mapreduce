package main

import "os"
import "net"
import "strconv"
import "net/rpc"

func main() {
    rpcs := rpc.NewServer()
    // rpcs.Register
    path := "/var/tmp/824-" + strconv.Itoa(os.Getuid()) + "/"
    os.Mkdir(path, 0777)
    path += "mr"
    l, _ := net.Listen("unix", path)
    // JobNum := 10
    conn, _ := l.Accept()
    go rpcs.ServeConn(conn) //ServeConn runs the DefaultServer on a single connection.

    // for JobNum != 0 {
    //     conn, err := l.Accept()
    //     if err == nil {
    //         JobNum -= 1
    //         go rpcs.ServeConn(conn)
    //     } else {
    //         break
    //     }
    // }

    l.Close()
}
