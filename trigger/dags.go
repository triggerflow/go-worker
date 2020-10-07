package trigger

import (
	"encoding/json"
	cloudevents "github.com/cloudevents/sdk-go"
	log "github.com/sirupsen/logrus"
	"sync/atomic"
)

type DAGTaskDependency struct {
	Counter uint32 `json:"counter"`
	Join    int32 `json:"join"`
}

type DAGTaskData struct {
	Subject      string                        `json:"subject"`
	Dependencies map[string]*DAGTaskDependency `json:"dependencies"`
	Operator     json.RawMessage               `json:"operator"`
	TaskResult   []interface{}                 `json:"result"`
}

func DAGTaskDataParser(rawData []byte) (interface{}, error) {
	dagTaskData := DAGTaskData{}
	err := json.Unmarshal(rawData, &dagTaskData)
	if err != nil {
		return nil, err
	}
	return &dagTaskData, nil
}

func DAGDummyTaskAction(context *Context, event cloudevents.Event) error {
	return nil
}

func DAGTaskFailureHandlerAction(context *Context, event cloudevents.Event) error {
	return nil
}

func DAGTaskRetryHandlerAction(context *Context, event cloudevents.Event) error {
	return nil
}

func DAGTaskJoinCondition(context *Context, event cloudevents.Event) (bool, error) {
	contextData := (*context).ConditionParsedData.(*DAGTaskData)

	// Increment counter for dependency of received task termination event
	if dependency, ok := (*contextData).Dependencies[event.Subject()]; ok {
		atomic.AddUint32(&(*dependency).Counter, 1)
	} else {
		log.Errorf("[%s] Subject %s not found in dependencies", context.TriggerID, event.Subject())
	}

	// Check if all dependencies have been fulfilled
	// (all counters are equal or greater than expected join termination events count)
	joined := true
	for _, dependency := range (*contextData).Dependencies {
		if dependency.Join == -1 {
			joined = false // If join == -1, the dependency for this upstream task hasn't been set yet
		} else {
			joined = atomic.LoadUint32(&(*dependency).Counter) == uint32(dependency.Join)
		}

		if !joined {
			break
		}
	}

	// TODO Store event result into context data

	return joined, nil
}
