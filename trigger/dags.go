package trigger

import (
	"encoding/json"
	cloudevents "github.com/cloudevents/sdk-go"
	log "github.com/sirupsen/logrus"
	"go.uber.org/atomic"
)

type DAGTaskDependency struct {
	Counter atomic.Uint32
	Join    int
}

type DAGTaskData struct {
	Subject      string
	Operator     json.RawMessage
	Dependencies map[string]*DAGTaskDependency
	TaskResult   map[string]interface{}
}

func DAGTaskDataParser(rawData []byte) interface{} {
	dagTaskData := DAGTaskData{}
	err := json.Unmarshal(rawData, &dagTaskData)
	if err != nil {
		panic(err)
	}
	return &dagTaskData
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
		(*dependency).Counter.Add(1)
	} else {
		log.Errorf("[%s] Subject %s not found in dependencies", context.TriggerID, event.Subject())
	}

	// Check if all dependencies have been fulfilled
	// (all counters are equal or greater than expected join termination events count)
	joined := true
	for _, dependency := range (*contextData).Dependencies {
		if dependency.Join == -1 {
			joined = false  // If join == -1, the dependency for this upstream task hasn't been set yet
		} else {
			joined = int(dependency.Counter.Load()) >= dependency.Join
		}

		if !joined {
			break
		}
	}

	// TODO Store event result into context data
	//if event.Data != nil && event.DataContentType() == "application/json" {
	//	eventData := make(map[string]interface{})
	//	eventDataRaw := event.Data.([]byte)
	//	tmp, _ := strconv.Unquote(string(eventDataRaw))
	//	err := json.Unmarshal([]byte(tmp), &eventData)
	//	if err != nil {
	//		panic(err)
	//		//log.Warnf("[DAGTaskJoinCondition] Could not decode event application/json data from %s", event.Subject())
	//	} else {
	//		if result, ok := context.Data["result"]; ok {
	//			switch result.(type) {
	//			case []interface{}: // Append result to result list
	//				context.Data["result"] = append(result.([]interface{}), event.Data)
	//			case interface{}: // Multiple results: store them in a slice
	//				resultList := make([]interface{}, 0)
	//				resultList = append(resultList, result)
	//				resultList = append(resultList, event.Data)
	//				context.Data["result"] = resultList
	//			}
	//		} else {
	//			context.Data["result"] = event.Data
	//		}
	//	}
	//}

	return joined, nil
}
