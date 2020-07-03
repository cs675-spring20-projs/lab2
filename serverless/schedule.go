package serverless

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (drv *Driver) schedule(
	phase jobPhase,
	serviceName string,
	registerChan chan string,
) {
	// MapReduceArgs defines the format of your MapReduce service plugins.
	type MapReduceArgs struct {
		JobName string
		inFile  string
		TaskNum int
		NReduce int
		NOthers int
	}

	//
	// TODO: implement me
	// Fill out this piece if necessary
	// TODO TODO TODO
	//

	// invokeService is a goroutine that is used to call the RPC
	// method of Worker.InvokeService at the worker side.
	invokeService := func(worker string, args *MapReduceArgs) {
		//
		// TODO: implement me
		// Fill out the code to invoke the corresponding Map and
		// Reduce services.
		// TODO TODO TODO
		//
	}

	// TODO: implement me
	// All tasks have to be scheduled on workers, and only once all of them
	// have been completed successfully should the function return.
	// Hint 1: Use for loop to loop over all the tasks, and use select
	// inside of for loop between registerChan and readyChan.
	// Hint 2: You may want to use goroutine to invoke function invokeService
	// like the following:
	// go invokeService(worker, args)
	// TODO TODO TODO
	//

	// Work done: finish the task scheduling
	Debug("Driver: %v phase done\n", phase)
}
