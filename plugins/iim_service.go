package main

import (
	"bytes"
	"cs675-spring20-labs/lab2/serverless"
	"encoding/gob"
	"fmt"
	"hash/fnv"
)

// To compile the map plugin: run:
// go build --buildmode=plugin -o iim_service.so iim_service.go

// Define Inverted Indexing's map service
type iimService string

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

// The mapping function is called once for each piece of the input.
// In this framework, the key is the name of the file that is being
// processed, and the value is the file's contents. The return value
// should be a slice of key/value pairs, each represented by a
// mapreduce.KeyValue.
func mapF(document string, value string) (res []KeyValue) {
	// TODO: implement me
	// you have to write this function
	// TODO TODO TODO
}

// doMap does the job of a map worker: it reads one of the input
// files (inFile), calls the user-defined function (mapF) for that
// file's contents, and partitions the output into nReduce
// intermediate files.
func doMap(
	jobName string,
	inFile string,
	taskNum int,
	nReduce int,
) {
	// TODO:
	// You will need to write this function.
	// You can find the filename for this map task's input to reduce
	// task number r using serverless.ReduceName(jobName, mapTask,
	// reduceTask). The ihash function (given below doMap) should be
	// used to decide which file a given key belongs into.
	//
	// The intermediate output of a map task is stored in the file
	// system as multiple files whose name indicates which map task
	// produced them, as well as which reduce task they are for.
	// Coming up with a scheme for how to store the key/value pairs
	// on disk can be tricky, especially when taking into account
	// that both keys and values could contain newlines, quotes, and
	// any other character you can think of.
	//
	// One format often used for serializing data to a byte stream
	// that the other end can correctly reconstruct is JSON. You are
	// not required to use JSON, but as the output of the reduce
	// tasks *must* be JSON, familiarizing yourself with it here may
	// prove useful. You can write out a data structure as a JSON
	// string to a file using the commented code below. The
	// corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the
	// values!  Use checkError to handle errors.
}

// We supply you an ihash function to help with mapping of a given
// key to an intermediate file.
func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}

// DON'T MODIFY THIS FUNCTION
func (s iimService) DoService(raw []byte) error {
	var args MapReduceArgs
	buf := bytes.NewBuffer(raw)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&args)
	if err != nil {
		fmt.Printf("Inverted Indexing Service: Failed to decode!\n")
		return err
	}
	fmt.Printf("Hello from inverted indexing service plugin: %s\n", args.InFile)

	doMap(args.JobName, args.InFile, args.TaskNum, args.NReduce)

	return nil
}

var Interface iimService
