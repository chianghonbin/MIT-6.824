package mapreduce

import (
	"encoding/json"
	"io"
	"log"
	"os"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	inFiles := make([]string, nMap)
	kvMap := make(map[string][]string)
	for index := range inFiles {
		debug("reduce task %v proceeds map task[%v]'s  intermediate file", reduceTaskNumber, index)
		inFiles[index] = reduceName(jobName, index, reduceTaskNumber)
		inFile, err := os.Open(inFiles[index])
		if err != nil {
			log.Fatal("open reduce input file :", err)
		}
		defer inFile.Close()

		dec := json.NewDecoder(inFile)
		for {
			var tmpKv KeyValue
			if err := dec.Decode(&tmpKv); err == io.EOF {
				break
			} else if err != nil {
				log.Fatal("Decode: ", err)
			}
			kvMap[tmpKv.Key] = append(kvMap[tmpKv.Key], tmpKv.Value)
		}

	}
	outFileName := mergeName(jobName, reduceTaskNumber)
	outFile, err := os.Create(outFileName)
	if err != nil {
		log.Fatal("create reduce task out file: ", err)
	}
	defer outFile.Close()
	enc := json.NewEncoder(outFile)
	for key, values := range kvMap {
		ret := reduceF(key, values)
		if err := enc.Encode(KeyValue{key, ret}); err != nil {
			log.Fatal("output encode:", err)
		}
	}
}
