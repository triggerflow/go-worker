package eventsource

import (
	cloudevents "github.com/cloudevents/sdk-go"
	"github.com/cloudevents/sdk-go/pkg/cloudevents/types"
	"net/url"

	jsoniter "github.com/json-iterator/go"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fastjson"
	"time"
)

func DecodeCloudEventBytes(rawCloudEvent []byte) (*cloudevents.Event, error) {
	t0 := time.Now().UTC().UnixNano()

	schemaUrl, err := url.Parse(fastjson.GetString(rawCloudEvent, "source"))
	if err != nil {
		panic(err)
	}
	source := &types.URIRef{URL: *schemaUrl}

	subject := fastjson.GetString(rawCloudEvent, "subject")

	cloudevent := cloudevents.Event{
		Context:     cloudevents.EventContextV1{
			ID:              fastjson.GetString(rawCloudEvent, "id"),
			Source:          *source,
			Type:            fastjson.GetString(rawCloudEvent, "type"),
			Subject:		 &subject,
		}.AsV1(),
		Data:        nil,
		DataEncoded: false,
		DataBinary:  false,
		FieldErrors: nil,
	}

	dataContentType := fastjson.GetString(rawCloudEvent, "datacontenttype")

	if dataContentType != "" {
		if dataContentType == cloudevents.ApplicationJSON {
			data := make(map[string]interface{})
			err := jsoniter.Unmarshal(fastjson.GetBytes(rawCloudEvent, "data"), &data)
			if err != nil {
				return nil, err
			} else {
				cloudevent.SetDataContentType(dataContentType)
				err = cloudevent.SetData(data)
				if err != nil {
					return nil, err
				}
			}
		} else {
			cloudevent.SetDataContentType(dataContentType)
			err = cloudevent.SetData(fastjson.GetBytes(rawCloudEvent, "data"))
			if err != nil {
				return nil, err
			}
		}
	}

	t1 := time.Now().UTC().UnixNano()

	log.Debugf("Cloudevent decoded in %f ms", float64(t1-t0) / 1000000.0)

	return &cloudevent, nil
}
