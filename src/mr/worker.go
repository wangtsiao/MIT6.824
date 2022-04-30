package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func storageJson(filename string, kva []KeyValue) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot create %v", filename)
	}
	enc := json.NewEncoder(file)
	for _, kv := range kva {
		err = enc.Encode(kv)
	}
	_ = file.Close()
}

func loadKvaFromJson(filename string, kva *[]KeyValue) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot create %v", filename)
	}
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			log.Fatalf("decode from %v error", filename)
		}
		*kva = append(*kva, kv)
	}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := GetTaskArgs{}
		reply := GetTaskReply{}
		ok := call("Coordinator.GetTask", &args, &reply)
		if !ok {
			break
		}

		if reply.TaskType == mapTaskType && reply.TaskNum != taskNumNone {
			// map task
			filename := reply.FileName
			log.Printf("Receive map task file %s, number %v\n", filename, reply.TaskNum)
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %s", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %s", filename)
			}
			_ = file.Close()
			kva := mapf(filename, string(content))

			intermediate := []KeyValue{}
			intermediate = append(intermediate, kva...)

			buckets := make([][]KeyValue, reply.NReduce)
			for i := 0; i < reply.NReduce; i++ {
				buckets[i] = []KeyValue{}
			}
			for _, kv := range intermediate {
				idx := ihash(kv.Key) % reply.NReduce
				buckets[idx] = append(buckets[idx], kv)
			}

			// save buckets, name out-X-Y, X is map task number, Y is reduce task number.
			for i := 0; i < reply.NReduce; i++ {
				outname := "mr" + fmt.Sprintf("-%v", reply.TaskNum) + fmt.Sprintf("-%v", i)
				outfile, err := ioutil.TempFile("", outname+"*")
				if err != nil {
					log.Fatalf("cannot open %v", outfile.Name())
				}
				enc := json.NewEncoder(outfile)
				for _, kv := range buckets[i] {
					err = enc.Encode(kv)
				}
				_ = os.Rename(outfile.Name(), outname)
				_ = outfile.Close()
			}

			// rpc tell coordinator that the map task has been finished.
			argsFinish := TaskFinishedArgs{TaskNum: reply.TaskNum}
			replyFinish := TaskFinishedReply{}
			ok = call("Coordinator.TaskFinished", &argsFinish, &replyFinish)

		} else if reply.TaskType == reduceTaskType && reply.TaskNum != taskNumNone {
			// reduce task
			log.Printf("Receive reduce task number %v\n", reply.TaskNum)
			intermediate := []KeyValue{}
			for i := 0; i < reply.NMap; i++ {
				inname := "mr" + fmt.Sprintf("-%v", i) + fmt.Sprintf("-%v", reply.TaskNum)
				infile, err := os.Open(inname)
				if err != nil {
					log.Fatalf("cannot open %s", inname)
				}
				dec := json.NewDecoder(infile)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				_ = infile.Close()

				// P1: 错误示范，因为reduce任务可能会失败，之后要重新执行，因此不能在这里早早删除。
				// _ = os.Remove(inname)
			}

			sort.Sort(ByKey(intermediate))

			// output file
			outname := "mr-out-" + strconv.Itoa(reply.TaskNum)
			outfile, _ := ioutil.TempFile("", outname+"*")

			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				_, _ = fmt.Fprintf(outfile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			_ = os.Rename(outfile.Name(), outname)
			_ = outfile.Close()

			// rpc tell coordinator that the map task has been finished.
			argsFinish := TaskFinishedArgs{TaskNum: reply.TaskNum + reply.NMap}
			replyFinish := TaskFinishedReply{}
			ok = call("Coordinator.TaskFinished", &argsFinish, &replyFinish)

			// P1: 清理文件
			for i := 0; i < reply.NMap; i++ {
				inname := "mr" + fmt.Sprintf("-%v", i) + fmt.Sprintf("-%v", reply.TaskNum)
				_ = os.Remove(inname)
			}
		}
		//time.Sleep(time.Second)
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

	fmt.Println(err)
	return false
}
