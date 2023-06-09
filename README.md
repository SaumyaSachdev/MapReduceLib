# MapReduceLib

## Instructions to run this project :

Requirements : 

- One machine for coordinator and one machine for each of the workers
- GoLang installed on both the coordinator and worker machines
- The IP address of coordinator 

All commands should be run in the src/main directory 
Also, the source code along with the input, intermediate and output files should be placed in a networked File System accessible by all machines.
The IP address of the coordinator should be configured in the server() function in coordinator.go and the call() function in the worker.go.

Build the MapReduce application for word count :
```
go build -buildmode=plugin ../mrapps/wc.go
```
On the coordinator machine :

```
go run mrcoordinator.go 10 pg-*.txt
```
The 10 denotes the number of reduce task to be created.

On each worker machine :
```
go run mrworker.go wc.so
```
For verifying output, run the test script in the src/main directory :
```
bash test-mr.sh
```
### Note:
To run the MapReduce library in your local machine :
- Uncomment lines 219 and 220 in worker.go and comment line 221
- Uncomment line 66 and comment line 65 in coordinator.go
- Run the coordinator.go and worker.go as above in two terminals

