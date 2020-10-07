package trigger

import (
	"bytes"
	"encoding/json"
	cloudevents "github.com/cloudevents/sdk-go"
	"github.com/json-iterator/go"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fastjson"
	"io/ioutil"
	"net/http"
	"net/http/cookiejar"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	cookieJar, _ = cookiejar.New(&cookiejar.Options{})
	retries      = 5
)

type IBMCloudFunctionsOperator struct {
	Url          string                   `json:"url"`
	ApiKey       string                   `json:"api_key"`
	InvokeKwargs map[string]interface{}   `json:"invoke_kwargs"`
	IterData     map[string][]interface{} `json:"iter_data"`
	Sink         json.RawMessage          `json:"sink"`
}

func IBMCloudFunctionsInvoke(context *Context, event cloudevents.Event) error {
	var (
		subject         string
		operator        IBMCloudFunctionsOperator
		taskData        *DAGTaskData
		iterDataKwarg   string
		iterDataValues  []interface{}
		wg              sync.WaitGroup
		activationsDone uint32
	)

	taskData = context.ActionParsedData.(*DAGTaskData)
	subject = taskData.Subject

	err := jsoniter.Unmarshal(taskData.Operator, &operator)
	if err != nil {
		return err
	}

	// Get iterdata kwarg and values
	iterDataKwargs := reflect.ValueOf(operator.IterData).MapKeys()
	if len(iterDataKwargs) == 0 {
		// Transform a callasync to a map of a single invocation with no iterdata parameter
		iterDataValues = []interface{}{nil}
		iterDataKwarg = ""
	} else {
		iterDataKwarg = iterDataKwargs[0].String()
		iterDataValues = operator.IterData[iterDataKwarg]
	}

	for i, iterDataValue := range iterDataValues {
		wg.Add(1)
		i := i
		go func(iterDataValue interface{}, act int) {
			defer wg.Done()

			invokePayload := make(map[string]interface{})

			// Update invokePayload with kwargs
			for k, v := range operator.InvokeKwargs {
				invokePayload[k] = v
			}

			// Add iter data Kwarg to payload
			if iterDataKwarg != "" {
				invokePayload[iterDataKwarg] = iterDataValue
			}

			// Add Triggerflow specific metadata (event Sink parameters, etc.)
			triggerflowData := make(map[string]interface{})
			triggerflowData["subject"] = subject
			triggerflowData["sink"] = operator.Sink
			invokePayload["__OW_TRIGGERFLOW"] = triggerflowData

			body, err := jsoniter.Marshal(invokePayload)
			if err != nil {
				log.Errorf("[%s] Could not marshal JSON invoke payload: %s", context.TriggerID, err)
				return
			}

			HTTPClient := &http.Client{
				Jar: cookieJar,
			}

			req, err := http.NewRequest("POST", operator.Url, bytes.NewBuffer(body))
			if err != nil {
				log.Errorf("[%s] Could not make Request object: %s", context.TriggerID, err)
				return
			}

			req.Header.Add("Content-Type", "application/json")
			userPassword := strings.Split(operator.ApiKey, ":")
			req.SetBasicAuth(userPassword[0], userPassword[1])

			done := false
			ret := 0

			t0 := float64(time.Now().UTC().UnixNano()) / 1000000000.0
			for !done && ret < retries {
				res, err := HTTPClient.Do(req)
				t1 := float64(time.Now().UTC().UnixNano()) / 1000000000.0

				if res != nil && (res.StatusCode == 200 || res.StatusCode == 201 || res.StatusCode == 202) {
					defer res.Body.Close()
					body, err := ioutil.ReadAll(res.Body)
					if err == nil {
						activationID := fastjson.GetString(body, "activationId")
						if activationID != "" {
							log.Infof("[%s] IBM Cloud Functions invoke %v success -- activation ID: %s -- %f s", context.TriggerID, i, activationID, t1-t0)
							atomic.AddUint32(&activationsDone, 1)
							done = true
						} else {
							log.Warnf("[%s] IBM Cloud Function invoke %v failure -- activation ID not in response", context.TriggerID, i)
							ret++
							time.Sleep(time.Millisecond * 15)
						}
					} else {
						log.Errorf("[%s] Could not unmarshal JSON response: %s", context.TriggerID, err)
						done = true
					}
				} else {
					log.Errorf("[%s] IBM Cloud Function invoke %i failed: %s", context.TriggerID, i, err)
					ret++
					time.Sleep(time.Millisecond * 15)
				}
			}

		}(iterDataValue, i)
	}

	wg.Wait()

	// Setup dependencies for downstream tasks
	if eventTypes, ok := (*context).TriggerEventMapping[subject]; ok {
		for eventType, downstreamTaskTriggerIDs := range eventTypes {
			if eventType == "event.triggerflow.termination.success" {
				for _, downstreamTaskTrigger := range downstreamTaskTriggerIDs {
					(*downstreamTaskTrigger).Lock.Lock()
					downstreamTaskTriggerData := (*downstreamTaskTrigger).Context.ConditionParsedData.(*DAGTaskData)
					dependency := (*downstreamTaskTriggerData).Dependencies[subject]
					(*dependency).Join = int32(activationsDone)
					(*downstreamTaskTriggerData).Dependencies[subject] = dependency
					(*downstreamTaskTrigger).Context.Modified = true
					(*downstreamTaskTrigger).Lock.Unlock()
				}
			}
		}
	}

	return nil
}
