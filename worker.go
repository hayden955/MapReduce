package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"regexp"
	"sort"
	"strconv"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// Here we implement a few key functions to sort by the key
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, x int)      { a[i], a[x] = a[x], a[i] }
func (a ByKey) Less(i, x int) bool { return a[i].Key < a[x].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.

type WorkerSt struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
	bucket  int
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	w := WorkerSt{
		mapf:    mapf,
		reducef: reducef,
	}

	w.RequestMapTask()
	w.RequestBA(reducef)

}

// Requests map task, tries to do it, and repeats
func (w *WorkerSt) RequestMapTask() {

	for {

		args := EmptyArs{}
		reply := MapTask{}

		call("Coordinator.RequestMapTask", &args, &reply)
		fmt.Println("Received MapTask: ", reply.Filename)

		file, err := os.Open(reply.Filename)

		if err != nil {
			log.Fatalf("cannot open %v", reply.Filename)
		}

		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", reply.Filename)
		}
		file.Close()

		kva := w.mapf(reply.Filename, string(content))
		// store kva in multiple files according to rules described in the README
		// ...

		kvas := make([][]KeyValue, reply.NumReducer)
		for _, kv := range kva {
			v := (ihash(kv.Key) % reply.NumReducer)
			kvas[v] = append(kvas[v], kv)
		}

		for i := 0; i < reply.NumReducer; i++ {
			filename := WriteIMFiles(kvas[i], reply.MapNumAssignment, i)
			//fmt.Print(filename)
			SendIntermediateFile(filename, i)
		}

		emptyReply := EmptyReply{}
		call("Coordinator.MTaskCompleted", &reply, &emptyReply)

		if emptyReply.MapCompleted {
			fmt.Print("FINSIHED MAP TASKKKSKSKSK")
			break
		}
	}
}

func (w *WorkerSt) RequestReduceTask(reducef func(string, []string) string, bucket int, files2reduce []string) {
	IM := []KeyValue{}
	for i := 0; i < len(files2reduce); i++ {
		//fmt.Print(i)

		file, _ := os.Open(files2reduce[i])
		defer file.Close()

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				//fmt.Print(next.Decode(&kv))
				break
			}

			IM = append(IM, kv)

		}

		file.Close()
	}

	sort.Sort(ByKey(IM))

	Bname := "mr-out-" + strconv.Itoa(bucket)
	Bfile, _ := os.Create(Bname)

	i := 0
	for i < len(IM) {

		j := i + 1
		for j < len(IM) && IM[j].Key == IM[i].Key {
			j++
		}

		val := []string{}
		for k := i; k < j; k++ {
			val = append(val, IM[k].Value)
		}

		out := reducef(IM[i].Key, val)

		fmt.Fprintf(Bfile, "%v %v\n", IM[i].Key, out)

		i = j

	}

	Bfile.Close()

	args := AllBuckets{}
	reply := EmptyReply{}

	call("Coordinator.RTaskCompleted", &args, &reply)

}

func (w *WorkerSt) RequestBA(reducef func(string, []string) string) {
	args := EmptyArs{}
	reply := Buckets{}

	//fmt.Print(fmt.Sprintf("mr-.*-%d", reply.Bucket))

	for {

		call("Coordinator.RequestBA", &args, &reply)
		pattern := regexp.MustCompile(fmt.Sprintf("mr-.*-%d", reply.Bucket))
		assignfiles := []string{}
		directory, _ := os.Getwd()
		d, _ := os.Open(directory)

		defer d.Close()

		files, _ := d.Readdir(-1)
		//fmt.Print(files)
		for _, file := range files {

			if pattern.FindStringSubmatch(file.Name()) != nil {
				assignfiles = append(assignfiles, file.Name())

				//fmt.Print(pattern.FindStringSubmatch(file.Name()))
			}

		}

		fmt.Printf("bucketid = %d. assignfile = %v\n", reply.Bucket, assignfiles)
		w.RequestReduceTask(reducef, reply.Bucket, assignfiles)

		//fmt.Println("filePattern =", filePattern)

	}
}

func SendIntermediateFile(filename string, bucketnum int) ReduceTasks {
	// This function will get each intermediate filename
	// and send it to the Coordinator for it to add to
	// the reducer task channel
	args := InterFiles{}
	args.Filename = filename
	args.ReduceBucketNum = bucketnum

	reducereply := ReduceTasks{}
	res := call("Coordinator.IntermediateFileHandler", &args, &reducereply)
	_ = res

	return reducereply
}

func WriteIMFiles(intermediate []KeyValue, MapTaskNum, ReduceID int) string {

	filename := "mr-" + strconv.Itoa(MapTaskNum) + "-" + strconv.Itoa(ReduceID)
	// mr-(task number)-(reduce number)
	file, _ := os.Create(filename)
	// Creates the file with naming convention
	enc := json.NewEncoder(file)
	// encodes each keyvalue pair to the json file
	for _, kv := range intermediate {
		err := enc.Encode(&kv)
		_ = err
	}

	return filename
}

//func IMFiles (intermediate)

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	//fmt.Println(rpcname, err)
	return false
}
