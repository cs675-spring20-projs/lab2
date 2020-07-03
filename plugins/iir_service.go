package main

import (
	"bytes"
	"cs675-spring20-labs/lab2/serverless"
	"encoding/gob"
	"fmt"
)

// To compile the map plugin: run:
// go build --buildmode=plugin -o iir_service.so iir_service.go

// Define Inverted Indexing's reduce service
type iirService string

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

// The reduce function is called once for each key generated by Map,
// with a list of that key's string value (merged across all inputs).
// The return value should be a single output value for that key.
func reduceF(key string, values []string) string {
	// TODO: implement me
	// you have to write this function
	// TODO TODO TODO
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
func (s iirService) DoService(raw []byte) error {
	var args MapReduceArgs
	buf := bytes.NewBuffer(raw)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&args)
	if err != nil {
		fmt.Printf("Inverted Indexing Service: Failed to decode!\n")
		return err
	}
	fmt.Printf("Hello from inverted indexing service plugin: %s\n", args.InFile)

	doReduce(args.JobName, args.TaskNum, args.NOthers)

	return nil
}

var Interface iirService
