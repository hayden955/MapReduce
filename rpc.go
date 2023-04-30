package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type EmptyArs struct {
}

type EmptyReply struct {
	MapCompleted    bool
	ReduceCompleted bool
}

type Buckets struct {
	Bucket int
}

type AllBuckets struct {
	ReduceFileList []string
}

type ReduceTasks struct {
	Filename        string
	ReduceBucketNum int
}

type NewArs struct {
	BucketNumRequest int
	NoBucketsLeft    bool
}

type InterFiles struct {
	Filename        string
	ReduceBucketNum int
}

type MapTask struct {
	Filename         string
	NumReducer       int
	MapNumAssignment int
	TaskType         string
	ReduceFileList   []string
}

type IntermediateFile struct {
	MessageType   int
	MessageCount  string
	NumReduceType int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
