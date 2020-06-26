package main

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"triggerflow/definitions"
)

func main() {
	//ctx := context.Background()
	//first := true
	//i := 0
	//r := kafka.NewReader(kafka.ReaderConfig{
	//	Brokers:   []string{"localhost:9092"},
	//	Topic:     "ingestion",
	//	MinBytes: 1,    // 1 B
	//	MaxBytes: 10e6, // 10MB
	//})
	//
	//for {
	//	_, err := r.FetchMessage(ctx)
	//	if first {
	//		fmt.Println(time.Now().UTC().UnixNano())
	//		first = false
	//	}
	//	if err != nil {
	//		panic(err)
	//	}
	//	i++
	//	if i >= 1000000 {
	//		fmt.Println(time.Now().UTC().UnixNano())
	//	}
	//}


	log.SetLevel(log.DebugLevel)

	// Get workspace name from env vars
	workspaceName, ok := os.LookupEnv("TRIGGERFLOW_WORKSPACE")
	//workspaceName := "ingestion-test"
	//ok := true

	// Try to get it from arguments if not found in env
	if !ok || workspaceName == "" {
		args := os.Args[1:]
		fmt.Println(len(args))
		if len(args) == 1 {
			workspaceName = os.Args[1:][0]
		} else {
			panic(fmt.Errorf("missing workspace name, not found in ENV nor in command line arguments"))
		}
	}

	// Set up parameters
	definitions.UpdateParameters()

	// Load triggerflow config map
	yamlFile, err := ioutil.ReadFile(definitions.ConfigMapFilename)
	if err != nil {
		panic(err)
	}

	err = yaml.Unmarshal(yamlFile, &definitions.ConfigMap)
	if err != nil {
		panic(err)
	}

	ProcessWorkspace(workspaceName)
}
