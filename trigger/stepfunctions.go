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
	"sync"
	"sync/atomic"
	"time"
)

type State struct {
	Subject  string `json:"Subject"`
	AWSState struct {
		TaskType   string                 `json:"Type"`
		Resource   string                 `json:"Resource"`
		Parameters map[string]interface{} `json:"Parameters"`
		Choices  []map[string]interface{} `json:"Choices"`
		ItemsPath  string                 `json:"ItemsPath"`
		InputPath  string                 `json:"InputPath"`
		Next       string                 `json:"Next"`
		End        bool                   `json:"End"`
	} `json:"State"`
	JoinTriggerID string `json:"join_state_machine"`
	Counter uint32        `json:"counter"`
	JoinMax uint32        `json:"join_multiple"`
	Results []interface{} `json:"sm_results"`
}

var awsSession *session.Session = nil
var awsLambda *lambda.Lambda = nil
var awsSessionLock *sync.Mutex = &sync.Mutex{}

func ASFStateParser(rawData []byte) (interface{}, error) {
	state := State{}
	err := json.Unmarshal(rawData, &state)
	if err != nil {
		return nil, err
	}
	if state.JoinMax > 0 && state.Results == nil {
		state.Results = make([]interface{}, state.JoinMax)
	}
	return &state, nil
}

func AWSStepFunctionsPass(context *Context, event cloudevents.Event) error {
	parsedData := context.ActionParsedData.(*State)

	termEvent, err := createTerminationCloudevent(parsedData.Subject, "lambda.success")
	if err != nil {
		return err
	}

	joinTrg, ok := context.ConditionParsedData.(*State)
	if ok {
		_ = termEvent.SetData(joinTrg.Results)
	}

	context.EventSink <- termEvent

	return nil
}

func AWSStepFunctionsTask(context *Context, event cloudevents.Event) error {
	var err error

	parsedData := context.ActionParsedData.(*State)

	awsSessionLock.Lock()
	if awsSession == nil {
		awsCredentials := context.GlobalContext["aws_credentials"]

		awsSession, err = session.NewSession(&aws.Config{
			Credentials: credentials.NewStaticCredentials(
				awsCredentials["access_key_id"].(string),
				awsCredentials["secret_access_key"].(string),
				""),
			Region: aws.String(awsCredentials["region"].(string)),
		})
		if err != nil {
			return err
		}

		awsLambda = lambda.New(awsSession)
	}
	awsSessionLock.Unlock()

	lambdaContext := struct {
		Subject     string                 `json:"__TRIGGERFLOW_SUBJECT"`
		LambdaArgs  interface{}            `json:"__EVENT_DATA"`
		EventSource map[string]interface{} `json:"__TRIGGERFLOW_EVENT_SOURCE"`
	}{
		Subject: parsedData.Subject,
	}

	if parsedData.AWSState.Parameters != nil {
		lambdaContext.LambdaArgs = parsedData.AWSState.Parameters
	} else {
		lambdaContext.LambdaArgs = event.Data
	}

	if eventSource, ok := context.GlobalContext["event_source"]; ok {
		lambdaContext.EventSource = eventSource
	}

	payload, err := jsoniter.Marshal(lambdaContext)

	invokeArgs := lambda.InvokeInput{
		FunctionName:   &parsedData.AWSState.Resource,
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
		log.Debugf("Invoked %s lambda -- %f s", parsedData.AWSState.Resource, t1-t0)
	} else {
		return errors.New(fmt.Sprintf("invocation failed: %d", *invokeResult.StatusCode))
	}

	return nil
}

func AWSStepFunctionsMap(context *Context, event cloudevents.Event) error {
	var (
		eventData interface{}
		items     interface{}
	)

	parsedData := context.ActionParsedData.(*State)

	rawEventData, ok := bytes.Unquote(event.Data.([]byte))
	if !ok {
		rawEventData = event.Data.([]byte)
	}
	err := jsoniter.Unmarshal(rawEventData, &eventData)
	if err != nil {
		return err
	}

	if parsedData.AWSState.ItemsPath != "" {
		items, err = jsonpath.Read(eventData, parsedData.AWSState.ItemsPath)
		if err != nil {
			return err
		}
	} else if parsedData.AWSState.InputPath != "" {
		items, err = jsonpath.Read(eventData, parsedData.AWSState.InputPath)
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
	joinContext := joinTrigger.Context.ConditionParsedData.(*State)
	joinContext.JoinMax = uint32(len(itemsList))
	//joinContext.Results = make([]interface{}, len(itemsList))

	for _, elem := range itemsList {
		termEvent, err := createTerminationCloudevent(parsedData.Subject, "lambda.success")
		if err != nil {
			return err
		}

		JSONElem, ok := elem.(map[string]interface{})
		if ok {
			termEvent.SetDataContentType(cloudevents.ApplicationJSON)
			termEvent.Data = JSONElem
		} else {
			termEvent.SetDataContentType(cloudevents.Base64)
			err = termEvent.SetData(elem)
		}
		if err != nil {
			return err
		}

		context.EventSink <- termEvent
	}

	return nil
}

func AWSStepFunctionsEndStateMachine(context *Context, event cloudevents.Event) error {
	parsedData := context.ActionParsedData.(*State)
	termEvent, err := createTerminationCloudevent(parsedData.Subject, "lambda.success")
	if err != nil {
		return err
	}

	joinTrg, ok := context.ConditionParsedData.(*State)
	if ok {
		_ = termEvent.SetData(joinTrg.Results)
	}

	context.EventSink <- termEvent
	return nil
}

func AWSStepFunctionsCondition(context *Context, event cloudevents.Event) (bool, error) {
	parsedData := context.ConditionParsedData.(*State)
	condition := true

	if parsedData.AWSState.Choices != nil {
		// TODO implement choice state logic
	}

	return condition, nil
}

func AWSStepFunctionsJoinStateMachine(context *Context, event cloudevents.Event) (bool, error) {
	parsedData := context.ConditionParsedData.(*State)

	if parsedData.JoinMax > 0 {
		cnt := atomic.AddUint32(&parsedData.Counter, 1)
		//parsedData.Results[cnt-1] = event.Data
		log.Debugf("Join %v / %v", cnt, parsedData.JoinMax)
		return cnt == parsedData.JoinMax, nil
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
