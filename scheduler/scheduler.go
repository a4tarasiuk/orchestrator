package scheduler

import (
	"orchestrator/node"
	"orchestrator/task"
)

type Scheduler interface {
	SelectCandidateNodes(task.Task, []*node.Node) []*node.Node

	Score(task.Task, []*node.Node) map[string]float64

	Pick(scores map[string]float64, candidates []*node.Node) *node.Node
}
