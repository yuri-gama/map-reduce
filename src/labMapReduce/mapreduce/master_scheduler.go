package mapreduce

import (
	"fmt"
	"log"
	"sync"
)

// Schedules map operations on remote workers. This will run until InputFilePathChan
// is closed. If there is no worker available, it'll block.
func (master *Master) schedule(task *Task, proc string, filePathChan chan string) int {
	//////////////////////////////////
	// YOU WANT TO MODIFY THIS CODE //
	//////////////////////////////////

	var (
		wg        sync.WaitGroup
		filePath  string
		worker    *RemoteWorker
		operation *Operation
		counter   int
	)

	log.Printf("Scheduling %v operations\n", proc)

	counter = 0
	fmt.Println(counter, proc)
	for filePath = range filePathChan {
		operation = &Operation{proc, counter, filePath}
		counter++

		worker = <-master.idleWorkerChan
		wg.Add(1)
		go master.runOperation(worker, operation, &wg)
	}

	wg.Wait()
	var valid, ok bool
	for master.totalFailedOp != 0 {
		select {
		case operation, valid = <-master.failedOperationChan:
			ok = true
		default:
			ok = false
		}
		if ok && valid {
			master.totalFailedOp -= 1
			operation = &Operation{operation.proc, operation.id, operation.filePath}

			worker = <-master.idleWorkerChan
			wg.Add(1)
			go master.runOperation(worker, operation, &wg)
		}

		wg.Wait()
	}

	log.Printf("%vx %v operations completed\n", counter, proc)
	return counter
}

// runOperation start a single operation on a RemoteWorker and wait for it to return or fail.
func (master *Master) runOperation(remoteWorker *RemoteWorker, operation *Operation, wg *sync.WaitGroup) {
	//////////////////////////////////
	// YOU WANT TO MODIFY THIS CODE //
	//////////////////////////////////

	var (
		err  error
		args *RunArgs
	)

	log.Printf("Running %v (ID: '%v' File: '%v' Worker: '%v')\n", operation.proc, operation.id, operation.filePath, remoteWorker.id)

	args = &RunArgs{operation.id, operation.filePath}
	err = remoteWorker.callRemoteWorker(operation.proc, args, new(struct{}))

	if err != nil {
		log.Printf("Operation %v '%v' Failed. Error: %v\n", operation.proc, operation.id, err)
		wg.Done()
		master.failedOperationChan <- operation
		master.failedWorkerChan <- remoteWorker
		master.totalFailedOp += 1
	} else {
		fmt.Println("predone: ", operation.filePath)
		wg.Done()
		master.idleWorkerChan <- remoteWorker
	}
}
