package main

import (
	"bytes"
	"cs675-spring20-labs/lab2_best_sol/serverless"
	"encoding/gob"
	"fmt"
)

// To compile the map plugin: run:
// go build --buildmode=plugin -o wcr_service.so wcr_service.go

// Define Word Count's reduce service
type wcrService string

// MapReduceArgs defines this plugin's argument format
type MapReduceArgs struct {
	JobName string
	InFile  string
	TaskNum int
	NReduce int
	NOthers int
}

type KeyValue struct {
	Key   string
	Value string
}

// doReduce does the job of a reduce worker: it reads the
// intermediate key/value pairs (produced by the map phase) for this
// task, sorts the intermediate key/value pairs by key, calls the
// user-defined reduce function (reduceF) for each key, and writes
// the output to disk. Each reduce generates an output file named
// using serverless.MergeName(jobName, reduceTask).
func doReduce(
	jobName string,
	reduceTaskNum int,
	nMap int,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from
	// map task number m using serverless.ReduceName(jobName,
	// mapTask, reduceTask).  Remember that you've encoded the values
	// in the intermediate files, so you will need to decode them. If
	// you chose to use JSON, you can read out multiple decoded
	// values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded
	// KeyValue objects to a file named mergeName(jobName,
	// reduceTaskNumber). We require you to use JSON here because
	// that is what the merger (the function driver.merge()) than
	// combines the output from all the reduce tasks expects. There
	// is nothing "special" about JSON -- it is just the marshalling
	// format we chose to use. It will look something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Use checkError to handle errors.
}

// DON'T MODIFY THIS FUNCTION
func (s wcrService) DoService(raw []byte) error {
	var args MapReduceArgs
	buf := bytes.NewBuffer(raw)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&args)
	if err != nil {
		fmt.Printf("Word Count Service: Failed to decode!\n")
		return err
	}
	fmt.Printf("Hello from wordCountService plugin: %s\n", args.InFile)

	doReduce(args.JobName, args.TaskNum, args.NOthers)

	return nil
}

var Interface wcrService
