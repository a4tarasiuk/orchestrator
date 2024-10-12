package scheduler

import (
	"orchestrator/node"
	"orchestrator/task"
)

type RoundRobin struct {
	Name string

	lastWorkerIdx int
}

func (r *RoundRobin) SelectCandidateNodes(t task.Task, nodes []*node.Node) []*node.Node {
	return nodes
}

func (r *RoundRobin) Score(t task.Task, nodes []*node.Node) map[string]float64 {
	nodeScores := make(map[string]float64)

	var workerIdx int

	if r.lastWorkerIdx+1 < len(nodes) {
		workerIdx = r.lastWorkerIdx + 1
		r.lastWorkerIdx++
	} else {
		workerIdx = 0
		r.lastWorkerIdx = 0
	}

	for idx, node := range nodes {
		if idx == workerIdx {
			nodeScores[node.Name] = 0.1
		} else {
			nodeScores[node.Name] = 1.0
		}
	}

	return nodeScores
}

func (r *RoundRobin) Pick(scores map[string]float64, candidates []*node.Node) *node.Node {
	var bestNode *node.Node

	var lowestScore float64

	for idx, candidateNode := range candidates {
		if idx == 0 {
			bestNode = candidateNode
			lowestScore = scores[candidateNode.Name]

			continue
		}

		if scores[candidateNode.Name] < lowestScore {
			bestNode = candidateNode
			lowestScore = scores[candidateNode.Name]
		}
	}

	return bestNode
}
