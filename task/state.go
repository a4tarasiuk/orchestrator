package task

type State int

const (
	PENDING State = iota
	SCHEDULED
	RUNNING
	COMPLETED
	FAILED
)

var stateTransitionMap = map[State][]State{
	PENDING:   {SCHEDULED},
	SCHEDULED: {SCHEDULED, RUNNING, FAILED},
	RUNNING:   {RUNNING, COMPLETED, FAILED},
	COMPLETED: {},
	FAILED:    {},
}

func Contains(states []State, state State) bool {
	for _, s := range states {
		if s == state {
			return true
		}
	}

	return false
}

func ValidStateTransition(src State, dst State) bool {
	return Contains(stateTransitionMap[src], dst)
}
