package trigger

import (
	"encoding/json"
	"fmt"
	cloudevents "github.com/cloudevents/sdk-go"
	log "github.com/sirupsen/logrus"
	"time"
)

type Join struct {
	Join    int32 `json:"join"`
	Counter int32 `json:"counter"`
}

func PassDataParser(rawData []byte) (interface{}, error) {
	return nil, nil
}

func JoinDataParser(rawData []byte) (interface{}, error) {
	data := Join{}
	err := json.Unmarshal(rawData, &data)
	if err != nil {
		return nil, err
	} else {
		return &data, nil
	}
}

func JoinCondition(context *Context, event cloudevents.Event) (bool, error) {
	parsedData := (*context).ConditionParsedData.(*Join)
	//cnt := int(atomic.AddUint32(&parsedData.Counter, 1))
	parsedData.Counter++
	return parsedData.Counter == parsedData.Join, nil
}

func TrueCondition(context *Context, event cloudevents.Event) (bool, error) {
	return true, nil
}

func CounterThresholdCondition(context *Context, event cloudevents.Event) (bool, error) {
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
	return false, nil
}

func PassAction(context *Context, event cloudevents.Event) error {
	return nil
}

func TerminateAction(context *Context, event cloudevents.Event) error {
	// TODO implement worker halt
	fmt.Println(time.Now().UTC().UnixNano())
	log.Infof("Terminate worker call")
	return nil
}
