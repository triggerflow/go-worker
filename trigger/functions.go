package trigger

// Actions contains the mapping from string to the trigger actions functions
var Actions = map[string]Action{
	"PASS":                     PassAction,
	"TERMINATE":                TerminateAction,
	"DAG_DUMMY_TASK":           DAGDummyTaskAction,
	"DAG_TASK_FAILURE_HANDLER": DAGTaskFailureHandlerAction,
	"DAG_TASK_RETRY_HANDLER":   DAGTaskRetryHandlerAction,
	"IBM_CF_INVOKE":            IBMCloudFunctionsInvoke,
	"AWS_ASF_PASS":             AWSStepFunctionsPass,
	"AWS_ASF_TASK":             AWSStepFunctionsTask,
	"AWS_ASF_MAP":              AWSStepFunctionsMap,
	"AWS_ASF_END_STATEMACHINE": AWSStepFunctionsEndStateMachine,
}

// Conditions contains the mapping from string to the trigger conditions functions
var Conditions = map[string]Condition{
	"TRUE":                      TrueCondition,
	"JOIN":                      JoinCondition,
	"DAG_TASK_JOIN":             DAGTaskJoinCondition,
	"COUNTER_THRESHOLD":         CounterThresholdCondition,
	"AWS_ASF_CONDITION":         AWSStepFunctionsCondition,
	"AWS_ASF_JOIN_STATEMACHINE": AWSStepFunctionsJoinStateMachine,
}

// ContextParsers is a map to link action and conditions with their respective data parser functions
var ContextParsers = map[string]func([]byte) (interface{}, error){
	"PASS":                      PassDataParser,
	"TERMINATE":                 PassDataParser,
	"DAG_DUMMY_TASK":            nil,
	"DAG_TASK_FAILURE_HANDLER":  nil,
	"DAG_TASK_RETRY_HANDLER":    nil,
	"IBM_CF_INVOKE":             nil,
	"AWS_ASF_PASS":              ASFStateParser,
	"AWS_ASF_TASK":              ASFStateParser,
	"AWS_ASF_MAP":               ASFStateParser,
	"AWS_ASF_END_STATEMACHINE":  ASFStateParser,
	"JOIN":                      JoinDataParser,
	"DAG_TASK_JOIN":             nil,
	"COUNTER_THRESHOLD":         nil,
	"AWS_ASF_CONDITION":         ASFStateParser,
	"AWS_ASF_JOIN_STATEMACHINE": ASFStateParser,
}
