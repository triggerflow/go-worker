# Go Implementation Backend for Triggerflow

This repository contains an alternative backend implementation for [Triggerflow](https://github.com/triggerflow/triggerflow) implemented in the Go language.

Using Go instead of the original Python implementation has 3 main benefits:
- **Concurrent trigger processing**: Go has concurrency built into the language as coroutines, which makes concurrent processing easy and efficient. Furthermore, it benefits from parallel processing when running in a multi-cpu machine. Trigger processing is a task that can be done concurrently, so Go fits nicely for our use case.
- **Better performance**: Go is a statically typed and compiled language, opposed to Python which is dynamic typed and interpreted. This results in greater performance and lower memory footprint in general.
- **A 'cloud' language**: Go was created with simplicity, micro-services, and cloud computing in mind, while Python was originally used as a scripting general purpose language. As a consequence, Go has key design decisions (for example, the lack of OOP in favour of structures to ease concurrent programming) that make it a better language to use for cloud computing and micro-services.  

This is implementation is completely compatible with the rest of the Triggerflow stack. You may swap directly the Python mages used in the original Triggerflow stack with the image containing this implementation and, in most of the cases, there (hopefully) would not be any issue.
Note that triggers that use Python Callable actions and conditions will not work in the Go implementation, so instead use Docker actions/conditions to run Python code.

## Instructions for use

1. Have [Go](https://golang.org/doc/install) installed and set up correctly.

2. Compile the binary: 
```
$ go build
```

3. Run in `local` mode (controller available in port `5000`):
```
$ ./triggerflow
```

Alternatively, you can build the Docker image to use in the Kubernetes deployment:
```
$ docker build . -t triggerflow-go
```

You can directly bootstrap a workflow worker passing the name of the workflow as the first arg:
```
$ ./triggerflow my-workflow
```

ENV vars:
| Env Name                        | Description                                                                                                    | Default           |
|---------------------------------|----------------------------------------------------------------------------------------------------------------|-------------------|
| TRIGGERFLOW_SINK_MAX_SIZE       | Local queue max size                                                                                           | 200000            |
| TRIGGERFLOW_CONFIG_MAP_FILE     | [Config map](https://github.com/triggerflow/triggerflow/blob/master/config/template.config_map.yaml) file name | "config_map.yaml" |
| TRIGGERFLOW_BOOTSTRAP_WORKSPACE | Bootstrap a workflow                                                                                           | ""                |
| TRIGGERFLOW_CONTROLLER_PORT     | Triggerflow controller API port                                                                                | 5000              |

  