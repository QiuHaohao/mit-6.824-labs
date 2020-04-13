package mr

func prepWorkerReadyReply(untypedTask interface{}, reply *WorkerReadyReply) {
	switch task := untypedTask.(type) {
	case *MapTask:
		reply.MapTask = task
	case *ReduceTask:
		reply.ReduceTask = task
	default:
		panic("unknown task type")
	}
}
