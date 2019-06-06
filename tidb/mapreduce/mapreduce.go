package main

import (
	"bufio"
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

// ByKey order KeyValue set ASC
type ByKey []KeyValue

func (kv ByKey) Len() int           { return len(kv) }
func (kv ByKey) Swap(i, j int)      { kv[i], kv[j] = kv[j], kv[i] }
func (kv ByKey) Less(i, j int) bool { return strings.Compare(kv[i].Key, kv[j].Key) == -1 }

// KeyValue is a type used to hold the key/value pairs passed to the map and reduce functions.
type KeyValue struct {
	Key   string
	Value string
}

// ReduceF function from MIT 6.824 LAB1
type ReduceF func(key string, values []string) string

// MapF function from MIT 6.824 LAB1
type MapF func(filename string, contents string) []KeyValue

// jobPhase indicates whether a task is scheduled as a map or reduce task.
type jobPhase string

const (
	mapPhase    jobPhase = "mapPhase"
	reducePhase          = "reducePhase"
)

type task struct {
	dataDir    string
	jobName    string
	mapFile    string   // only for map, the input file
	phase      jobPhase // are we in mapPhase or reducePhase?
	taskNumber int      // this task's index in the current phase
	nMap       int      // number of map tasks
	nReduce    int      // number of reduce tasks
	mapF       MapF     // map function used in this job
	reduceF    ReduceF  // reduce function used in this job
	wg         sync.WaitGroup
}

// MRCluster represents a map-reduce cluster.
type MRCluster struct {
	nWorkers int
	wg       sync.WaitGroup
	taskCh   chan *task
	exit     chan struct{}
}

var singleton = &MRCluster{
	nWorkers: runtime.NumCPU(),
	taskCh:   make(chan *task),
	exit:     make(chan struct{}),
}

func init() {
	singleton.Start()
}

// GetMRCluster returns a reference to a MRCluster.
func GetMRCluster() *MRCluster {
	return singleton
}

// NWorkers returns how many workers there are in this cluster.
func (c *MRCluster) NWorkers() int { return c.nWorkers }

// Start starts this cluster.
func (c *MRCluster) Start() {
	for i := 0; i < c.nWorkers; i++ {
		c.wg.Add(1)
		go c.worker()
	}
}

func (c *MRCluster) worker() {
	defer c.wg.Done()
	concurrentMax := 2
	concurrent := 0
	for {
		select {
		case t := <-c.taskCh:
			if t.phase == mapPhase {
				content, err := ioutil.ReadFile(t.mapFile)
				if err != nil {
					panic(err)
				}
				fs := make([]*os.File, t.nReduce)
				bs := make([]*bufio.Writer, t.nReduce)
				for i := range fs {
					rpath := reduceName(t.dataDir, t.jobName, t.taskNumber, i)
					fs[i], bs[i] = CreateFileAndBuf(rpath)
				}
				results := t.mapF(t.mapFile, string(content))
				for _, kv := range results {
					enc := json.NewEncoder(bs[ihash(kv.Key)%t.nReduce])
					if err := enc.Encode(&kv); err != nil {
						log.Fatalln(err)
					}
				}
				for i := range fs {
					SafeClose(fs[i], bs[i])
				}
				t.wg.Done()
			} else {
				if concurrent > concurrentMax {
					doReduce(t)
				} else {
					concurrent++
					go func() {
						doReduce(t)
						concurrent--

					}()
				}
			}
		case <-c.exit:
			return
		}
	}
}

func doReduce(t *task) {
	groupByKey := make(map[string][]string)
	fs := make([]*os.File, t.nMap)
	bs := make([]*bufio.Reader, t.nMap)

	for i := range fs {
		fs[i], bs[i] = OpenFileAndBuf(reduceName(t.dataDir, t.jobName, i, t.taskNumber))
		defer fs[i].Close()
	}

	for i := 0; i < t.nMap; i++ {
		reader := bs[i]
		dec := json.NewDecoder(reader)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if _, ok := groupByKey[kv.Key]; ok != true {
				groupByKey[kv.Key] = make([]string, 0)
			}
			groupByKey[kv.Key] = append(groupByKey[kv.Key], kv.Value)
		}
	}

	mf, mfb := CreateFileAndBuf(mergeName(t.dataDir, t.jobName, t.taskNumber))

	for key, vals := range groupByKey {
		WriteToBuf(mfb, t.reduceF(key, vals))
	}

	SafeClose(mf, mfb)
	t.wg.Done()
}

// Shutdown shutdowns this cluster.
func (c *MRCluster) Shutdown() {
	close(c.exit)
	c.wg.Wait()
}

// Submit submits a job to this cluster.
func (c *MRCluster) Submit(jobName, dataDir string, mapF MapF, reduceF ReduceF, mapFiles []string, nReduce int) <-chan []string {
	notify := make(chan []string)
	go c.run(jobName, dataDir, mapF, reduceF, mapFiles, nReduce, notify)
	return notify
}

func (c *MRCluster) run(jobName, dataDir string, mapF MapF, reduceF ReduceF, mapFiles []string, nReduce int, notify chan<- []string) {
	// map phase
	nMap := len(mapFiles)
	tasks := make([]*task, 0, nMap)

	for i := 0; i < nMap; i++ {
		t := &task{
			dataDir:    dataDir,
			jobName:    jobName,
			mapFile:    mapFiles[i],
			phase:      mapPhase,
			taskNumber: i,
			nReduce:    nReduce,
			nMap:       nMap,
			mapF:       mapF,
		}
		t.wg.Add(1)
		tasks = append(tasks, t)
		go func() {
			c.taskCh <- t
		}()
	}

	for _, t := range tasks {
		t.wg.Wait()
	}
	// reduce phase
	rtasks := make([]*task, 0, nReduce)
	mfs := make([]string, 0, nReduce)

	for i := 0; i < nReduce; i++ {
		t := &task{
			dataDir:    dataDir,
			jobName:    jobName,
			phase:      reducePhase,
			taskNumber: i,
			nReduce:    nReduce,
			nMap:       nMap,
			reduceF:    reduceF,
		}
		t.wg.Add(1)
		rtasks = append(rtasks, t)
		go func() {
			c.taskCh <- t
		}()
	}

	for _, t := range rtasks {
		mf := mergeName(t.dataDir, t.jobName, t.taskNumber)
		mfs = append(mfs, mf)
		t.wg.Wait()
	}

	notify <- mfs
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}

func reduceName(dataDir, jobName string, mapTask int, reduceTask int) string {
	return path.Join(dataDir, "mrtmp."+jobName+"-"+strconv.Itoa(mapTask)+"-"+strconv.Itoa(reduceTask))
}

func mergeName(dataDir, jobName string, reduceTask int) string {
	return path.Join(dataDir, "mrtmp."+jobName+"-res-"+strconv.Itoa(reduceTask))
}
