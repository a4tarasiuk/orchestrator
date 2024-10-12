package scheduler

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"time"

	"orchestrator/node"
	"orchestrator/task"
	"orchestrator/worker"
)

const (
	// LIEB square ice constant
	// https://en.wikipedia.org/wiki/Lieb%27s_square_ice_constant
	LIEB = 1.53960071783900203869
)

type Epvm struct {
	Name string
}

func (e *Epvm) SelectCandidateNodes(t task.Task, nodes []*node.Node) []*node.Node {
	var candidates []*node.Node

	for _node := range nodes {
		if checkDisk(t, nodes[_node].Disk-nodes[_node].DiskAllocated) {
			candidates = append(candidates, nodes[_node])
		}
	}

	return candidates
}

func (e *Epvm) Score(t task.Task, nodes []*node.Node) map[string]float64 {
	nodeScores := make(map[string]float64)
	maxJobs := 4.0
	for _, node := range nodes {
		cpuUsage, _ := calculateCpuUsage(node)

		cpuLoad := calculateLoad(*cpuUsage, math.Pow(2, 0.8))
		memoryAllocated := float64(node.Stats.GetUsedMemoryKB()) +
			float64(node.MemoryAllocated)
		memoryPercentAllocated := memoryAllocated / float64(node.Memory)
		newMemPercent := calculateLoad(
			memoryAllocated+
				float64(t.Memory/1000), float64(node.Memory),
		)
		memCost := math.Pow(LIEB, newMemPercent) + math.Pow(
			LIEB,
			(float64(node.TaskCount+1))/maxJobs,
		) -
			math.Pow(LIEB, memoryPercentAllocated) -
			math.Pow(LIEB, float64(node.TaskCount)/float64(maxJobs))
		cpuCost := math.Pow(LIEB, cpuLoad) +
			math.Pow(LIEB, (float64(node.TaskCount+1))/maxJobs) -
			math.Pow(LIEB, cpuLoad) -
			math.Pow(LIEB, float64(node.TaskCount)/float64(maxJobs))
		nodeScores[node.Name] = memCost + cpuCost

	}

	return nodeScores
}

func (e *Epvm) Pick(scores map[string]float64, candidates []*node.Node) *node.Node {
	minCost := 0.00
	var bestNode *node.Node
	for idx, node := range candidates {
		if idx == 0 {
			minCost = scores[node.Name]
			bestNode = node
			continue
		}
		if scores[node.Name] < minCost {
			minCost = scores[node.Name]
			bestNode = node
		}
	}
	return bestNode
}

func calculateCpuUsage(node *node.Node) (*float64, error) {
	stat1, err := node.GetStats()
	if err != nil {
		return nil, err
	}
	time.Sleep(3 * time.Second)
	stat2, err := node.GetStats()
	if err != nil {
		return nil, err
	}
	stat1Idle := stat1.CPU.Idle + stat1.CPU.IOWait
	stat2Idle := stat2.CPU.Idle + stat2.CPU.IOWait
	stat1NonIdle := stat1.CPU.User + stat1.CPU.Nice +
		stat1.CPU.System + stat1.CPU.IRQ +
		stat1.CPU.SoftIRQ + stat1.CPU.Steal
	stat2NonIdle := stat2.CPU.User + stat2.CPU.Nice + stat2.CPU.System + stat2.CPU.IRQ +
		stat2.CPU.SoftIRQ + stat2.CPU.Steal
	stat1Total := stat1Idle + stat1NonIdle
	stat2Total := stat2Idle + stat2NonIdle
	total := stat2Total - stat1Total
	idle := stat2Idle - stat1Idle
	var cpuPercentUsage float64
	if total == 0 && idle == 0 {
		cpuPercentUsage = 0.00
	} else {
		cpuPercentUsage = (float64(total) - float64(idle)) / float64(total)
	}
	return &cpuPercentUsage, nil
}

func getNodeStats(node *node.Node) *worker.Stats {
	url := fmt.Sprintf("%s/stats", node.Ip)
	resp, err := http.Get(url)
	if err != nil {
		log.Printf("Error connecting to %v: %v", node.Ip, err)
	}
	if resp.StatusCode != 200 {
		log.Printf("Error retrieving stats from %v: %v", node.Ip, err)
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	var stats worker.Stats
	json.Unmarshal(body, &stats)
	return &stats
}

func checkDisk(t task.Task, diskAvailable int64) bool {
	return t.Disk <= diskAvailable
}

func calculateLoad(usage float64, capacity float64) float64 {
	return usage / capacity
}
