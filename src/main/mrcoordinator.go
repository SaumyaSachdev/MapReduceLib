package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import "6.5840/mr"
import "time"
import "os"
import "fmt"
import "strconv"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}
	startTime := time.Now()
	red, _ := strconv.Atoi(os.Args[1])
	m := mr.MakeCoordinator(os.Args[2:], red)
	for m.Done() == false {
		time.Sleep(time.Second)
	}
	endTime := time.Now()

    	// Calculate the time difference
     	timeDiff := endTime.Sub(startTime)

    	// Print the time took
        fmt.Println("Time took:", timeDiff)
	time.Sleep(time.Second)
}
