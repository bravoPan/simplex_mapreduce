package main

import "fmt"
// import "sync"

func main () {
    mk := make(chan bool, 2)
    mk <- true
    mk <- true
    task_comp := make(chan bool, 100)

    // Execute 100 tasks by 2 workers
    for i:=0; i < 100; i++{
        go func (){
            func_in_mk := <- mk
            mk <- func_in_mk
            task_comp <- true
        }()
    }

    for i := 0; i < 100; i++{
        <- task_comp
    }
    <- mk
    fmt.Println("over")
}
