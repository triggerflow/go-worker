package trigger

import (
	"encoding/json"
	"fmt"
	cloudevents "github.com/cloudevents/sdk-go"
	"time"
)

type Join struct {
	Join int
}

func SimpleJoinDataParser(rawData json.RawMessage) interface{} {
	data := Join{}
	err := json.Unmarshal(rawData, &data)
	if err != nil {
		panic(err)
	}
	return &data
}

func SimpleJoinCondition(context *Context, event cloudevents.Event) bool {
	(*context).Counter++
	contextData := (*context).ParsedData.(*Join)
	return (*context).Counter >= (*contextData).Join
}

func TrueCondition(context *Context, event cloudevents.Event) bool {
	return true
}

func CounterThresholdCondition(context *Context, event cloudevents.Event) bool {
	//(*context).Counter++
	//
	//totalActivations := 0
	//if val, ok := (*context).Data["threshold"]; ok {
	//	totalActivations = val.(int)
	//}
	//
	//joined := (*context).Counter >= totalActivations
	//
	//return joined
	return true
}

func PassAction(context *Context, event cloudevents.Event) {

}

func TerminateAction(context *Context, event cloudevents.Event) {
	// TODO implement worker halt
	fmt.Println(context.Counter, time.Now().UTC().UnixNano())
	//log.Infof("Terminate worker call")
}
