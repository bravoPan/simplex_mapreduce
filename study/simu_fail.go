package main

import "fmt"
import "strconv"
// import "sync"

type testStructChan struct {
    workerId chan int
}

func main () {
    newtestChan := new(testStructChan)
    newtestChan.workerId = make(chan int, 2)

    for i:=0; i < 2; i++ {
        newtestChan.workerId <- i
    }

    // var int nRPC

    task_comp := make(chan bool, 100)
    for i:=0; i < 100; i++{
        i := i
        go func(){
            func_in_mk := <- newtestChan.workerId
            fmt.Println("Job ", strconv.Itoa(i), " is processed by Worker: ", func_in_mk)
            newtestChan.workerId <- func_in_mk
            task_comp <- true
        }()
    }

    for i := 0; i < 100; i++{
        <- task_comp
    }
    fmt.Println("over")
}
