package main

import "fmt"
import "sync"

func main () {
    mk := make(chan bool)
    // mk <- true
    task_comp := make(chan bool, 100)
    var mux *sync.Mutex
    mux = &sync.Mutex{}

    for i:=0; i < 100; i++{
        go func (){
            // for {
            mux.Lock()
            mk <- true
            func_in_mk := <- mk
            mk <- func_in_mk
            mux.Unlock()

            task_comp <- true
            // }
            // return
        }()
    }

    for i := 0; i < 100; i++{
        <- task_comp
    }
    <- mk
    fmt.Println("over")
}
