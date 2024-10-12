package node

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"orchestrator/utils"
	"orchestrator/worker"
)

type Node struct {
	Name string

	Ip    string
	Cores int

	Memory          int
	MemoryAllocated int

	Disk          int64
	DiskAllocated int64

	Role      string
	TaskCount int

	Stats worker.Stats
}

func NewNode(workerName string, API string, role string) *Node {
	return &Node{
		Name: workerName,
		Ip:   API,
		Role: role,
	}
}

func (n *Node) GetStats() (*worker.Stats, error) {
	var resp *http.Response
	var err error
	url := fmt.Sprintf("%s/stats", n.Ip)
	resp, err = utils.HTTPWithRetry(http.Get, url)
	if err != nil {
		msg := fmt.Sprintf("Unable to connect to %v. Permanent failure.\n", n.Ip)
		log.Println(msg)
		return nil, errors.New(msg)
	}
	if resp.StatusCode != 200 {
		msg := fmt.Sprintf("Error retrieving stats from %v: %v", n.Ip, err)
		log.Println(msg)
		return nil, errors.New(msg)
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	var stats worker.Stats
	err = json.Unmarshal(body, &stats)
	if err != nil {
		msg := fmt.Sprintf("error decoding message while getting stats for node %s", n.Name)
		log.Println(msg)
		return nil, errors.New(msg)
	}
	n.Memory = int(stats.GetTotalMemoryKB())
	n.Disk = int64(stats.GetDiskTotal())
	n.Stats = stats

	return &n.Stats, nil
}
