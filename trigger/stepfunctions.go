package trigger

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/azr/bytes"
	cloudevents "github.com/cloudevents/sdk-go"
	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	log "github.com/sirupsen/logrus"
	"github.com/yalp/jsonpath"
	"os"
	"sync/atomic"
	"time"
)

type BaseState struct {
	Subject string `json:"Subject"`
}

type PassState struct {
	BaseState
}

type TaskState struct {
	BaseState
	State struct {
		TaskType string `json:"Type"`
		Resource string `json:"Resource"`
		Next     string `json:"Next"`
		End      bool   `json:"End"`
	}
}

type MapState struct {
	BaseState
	State struct {
		TaskType  string `json:"Type"`
		ItemsPath string `json:"ItemsPath"`
		InputPath string `json:"InputPath"`
		Next      string `json:"Next"`
		End       bool   `json:"End"`
	}
	JoinTriggerID string `json:"join_state_machine"`
}

type ChoiceState struct {
	BaseState
	State struct {
		TaskType string                   `json:"Type"`
		Next     string                   `json:"Next"`
		End      bool                     `json:"End"`
		Choices  []map[string]interface{} `json:"Choices"`
	}
}

type JoinStateMachine struct {
	BaseState
	Counter uint32        `json:"counter"`
	JoinMax uint32        `json:"join_multiple"`
	Results []interface{} `json:"sm_results"`
}

type EndStateMachine struct {
	BaseState
}

var awsSession *session.Session = nil
var awsLambda *lambda.Lambda = nil

func ASFPassStateDataParser(rawData []byte) (interface{}, error) {
	state := PassState{}
	err := json.Unmarshal(rawData, &state)
	if err != nil {
		return nil, err
	} else {
		return &state, nil
	}
}

func ASFTaskStateDataParser(rawData []byte) (interface{}, error) {
	state := TaskState{}
	err := json.Unmarshal(rawData, &state)
	if err != nil {
		return nil, err
	} else {
		return &state, nil
	}
}

func ASFConditionDataParser(rawData []byte) (interface{}, error) {
	state := ChoiceState{}
	err := json.Unmarshal(rawData, &state)
	if err != nil {
		return nil, err
	} else {
		return &state, nil
	}
}

func ASFMapStateDataParser(rawData []byte) (interface{}, error) {
	state := MapState{}
	err := json.Unmarshal(rawData, &state)
	if err != nil {
		return nil, err
	} else {
		return &state, nil
	}
}

func ASFEndStateMachineDataParser(rawData []byte) (interface{}, error) {
	state := EndStateMachine{}
	err := json.Unmarshal(rawData, &state)
	if err != nil {
		return nil, err
	} else {
		return &state, nil
	}
}

func ASFJoinStateMachineDataParser(rawData []byte) (interface{}, error) {
	state := JoinStateMachine{}
	err := json.Unmarshal(rawData, &state)
	if err != nil {
		return nil, err
	}
	if state.JoinMax > 0 {
		state.Results = make([]interface{}, state.JoinMax)
	}
	return &state, nil
}

func AWSStepFunctionsPass(context *Context, event cloudevents.Event) error {
	parsedData := context.ActionParsedData.(*PassState)


	termEvent, err := createTerminationCloudevent(parsedData.Subject, "lambda.success")
	if err != nil {
		return err
	}

	joinTrg, ok := context.ConditionParsedData.(*JoinStateMachine)
	if ok {
		_ = termEvent.SetData(joinTrg.Results)
	}

	context.EventSink <- termEvent

	return nil
}

func AWSStepFunctionsTask(context *Context, event cloudevents.Event) error {
	var err error

	parsedData := context.ActionParsedData.(*TaskState)

	if awsSession == nil {
		awsCredentials := struct {
			AccessKeyID     string `json:"access_key_id"`
			SecretAccessKey string `json:"secret_access_key"`
			Region          string `json:"region"`
		}{}

		err = json.Unmarshal([]byte(context.GlobalContext["aws_credentials"]), &awsCredentials)
		if err != nil {
			return err
		}

		awsSession, err = session.NewSession(&aws.Config{
			Credentials: credentials.NewStaticCredentials(awsCredentials.AccessKeyID, awsCredentials.SecretAccessKey, ""),
			Region:      &awsCredentials.Region,
		})
		if err != nil {
			return err
		}

		awsLambda = lambda.New(awsSession)
	}

	lambdaContext := struct {
		Subject    string `json:"__TRIGGERFLOW_SUBJECT"`
		LambdaArgs []byte `json:"__EVENT_DATA"`
	}{
		Subject:    parsedData.Subject,
		LambdaArgs: event.Data.([]byte),
	}

	payload, err := json.Marshal(lambdaContext)

	invokeArgs := lambda.InvokeInput{
		FunctionName:   &parsedData.State.Resource,
		InvocationType: aws.String(lambda.InvocationTypeEvent),
		Payload:        payload,
	}

	t0 := float64(time.Now().UTC().UnixNano()) / 1000000000.0
	invokeResult, err := awsLambda.Invoke(&invokeArgs)
	t1 := float64(time.Now().UTC().UnixNano()) / 1000000000.0

	if err != nil {
		return err
	}

	if *invokeResult.StatusCode >= 200 && *invokeResult.StatusCode < 204 {
		log.Infof("Invoked %s lambda -- %f s", parsedData.State.Resource, t1-t0)
	} else {
		return errors.New("oh no :(")
	}

	return nil
}

func AWSStepFunctionsMap(context *Context, event cloudevents.Event) error {
	var (
		eventData interface{}
		items     interface{}
	)

	parsedData := context.ActionParsedData.(*MapState)

	rawEventData, ok := bytes.Unquote(event.Data.([]byte))
	if !ok {
		rawEventData = event.Data.([]byte)
	}
	err := jsoniter.Unmarshal(rawEventData, &eventData)
	if err != nil {
		return err
	}

	if parsedData.State.ItemsPath != "" {
		items, err = jsonpath.Read(eventData, parsedData.State.ItemsPath)
		if err != nil {
			return err
		}
	} else if parsedData.State.InputPath != "" {
		items, err = jsonpath.Read(eventData, parsedData.State.InputPath)
		if err != nil {
			return err
		}
	} else {
		items = eventData
	}

	itemsList, ok := items.([]interface{})
	if !ok {
		return errors.New("input data is not iterable")
	}

	joinTrigger := context.Triggers[parsedData.JoinTriggerID]
	joinContext := joinTrigger.Context.ConditionParsedData.(*JoinStateMachine)
	joinContext.JoinMax = uint32(len(itemsList))
	joinContext.Results = make([]interface{}, len(itemsList))

	for _, elem := range itemsList {
		termEvent, err := createTerminationCloudevent(parsedData.Subject, "lambda.success")
		if err != nil {
			return err
		}

		err = termEvent.SetData(elem)
		if err != nil {
			return err
		}
		termEvent.SetDataContentType("text/plain")

		context.EventSink <- termEvent
	}

	return nil
}

func AWSStepFunctionsEndStateMachine(context *Context, event cloudevents.Event) error {
	parsedData := context.ActionParsedData.(*EndStateMachine)
	termEvent, err := createTerminationCloudevent(parsedData.Subject, "lambda.success")
	if err != nil {
		return err
	}

	joinTrg, ok := context.ConditionParsedData.(*JoinStateMachine)
	if ok {
		_ = termEvent.SetData(joinTrg.Results)
	}

	context.EventSink <- termEvent
	return nil
}

func AWSStepFunctionsCondition(context *Context, event cloudevents.Event) (bool, error) {
	parsedData := context.ConditionParsedData.(*ChoiceState)
	condition := true

	if parsedData.State.Choices != nil {
		// TODO implement choice state logic
	}

	return condition, nil
}

func AWSStepFunctionsJoinStateMachine(context *Context, event cloudevents.Event) (bool, error) {
	parsedData := context.ConditionParsedData.(*JoinStateMachine)

	if parsedData.JoinMax > 0 {
		cnt := atomic.AddUint32(&parsedData.Counter, 1)
		parsedData.Results[cnt-1] = event.Data
		return cnt >= parsedData.JoinMax, nil
	}

	return true, nil
}

func createTerminationCloudevent(subject string, eventType string) (*cloudevents.Event, error) {
	eventUUID, err := uuid.NewRandom()

	if err != nil {
		return nil, err
	}

	uuidstr := eventUUID.String()
	hostname, err := os.Hostname()

	if err != nil {
		hostname = "unknown"
	}

	termEvent := cloudevents.NewEvent()
	termEvent.SetSource(fmt.Sprintf("urn:%s:%s", hostname, uuidstr))
	termEvent.SetSubject(subject)
	termEvent.SetType(eventType)
	termEvent.SetID(uuidstr)

	return &termEvent, nil
}
