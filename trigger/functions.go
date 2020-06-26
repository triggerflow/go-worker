package trigger

import "encoding/json"

// Actions contains the mapping from string to the trigger actions callables
var Actions = map[string]Action{
	"PASS":                     PassAction,
	"TERMINATE":                TerminateAction,
	"DAG_DUMMY_TASK":           DAGDummyTaskAction,
	"DAG_TASK_FAILURE_HANDLER": DAGTaskFailureHandlerAction,
	"DAG_TASK_RETRY_HANDLER":   DAGTaskRetryHandlerAction,
	"IBM_CF_INVOKE":            IBMCloudFunctionsInvoke,
}

// Conditions contains the mapping from string to the trigger conditions callables
var Conditions = map[string]Condition{
	"TRUE":              TrueCondition,
	"SIMPLE_JOIN":     SimpleJoinCondition,
	"DAG_TASK_JOIN":     DAGTaskJoinCondition,
	"COUNTER_THRESHOLD": CounterThresholdCondition,
}

var ContextParsers = map[string]func(message json.RawMessage)interface{}{
	"DAGS": DAGTaskDataParser,
	"JOIN": SimpleJoinDataParser,
}
