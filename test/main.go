package main

import (
	"log"
	"os"
	"runtime/pprof"
	"time"

	"github.com/aws/amazon-cloudwatch-logs-for-fluent-bit/cloudwatch"
)

// TestCycles is how many AddEvent()s to run.
// 30,000,000 is about 1-2 minutes worth on a macbook pro.
const TestCycles = 30000000

var (
	config = cloudwatch.OutputPluginConfig{
		Region:               "us-west-1",
		LogGroupName:         "log-group-name-$(tag)",
		DefaultLogGroupName:  "default-log-group-name",
		LogStreamPrefix:      "",
		LogStreamName:        "log-stream-name-$(tag[0])-$(ident)-$(kubernetes['app'])",
		DefaultLogStreamName: "default-log-stream-name",
		LogKey:               "",
		RoleARN:              "",
		AutoCreateGroup:      false,
		NewLogGroupTags:      "",
		LogRetentionDays:     60,
		CWEndpoint:           "",
		STSEndpoint:          "",
		CredsEndpoint:        "",
		PluginInstanceID:     0,
		LogFormat:            "",
	}
	record = map[interface{}]interface{}{
		"msg":   "log line here",
		"ident": "app-name",
		"kubernetes": map[interface{}]interface{}{
			"app": "mykube",
		},
	}
)

func main() {
	instance, err := cloudwatch.NewOutputPlugin(config)
	if err != nil {
		log.Fatal(err)
	}

	// override all the cloudwatch methods, so they don't do anything.
	instance.Client = &cwMock{}

	f, err := os.Create("cloudwatchlogs.prof")
	if err != nil {
		log.Fatal(err)
	}

	run(f, instance)
}

func run(file *os.File, instance *cloudwatch.OutputPlugin) {
	pprof.StartCPUProfile(file)
	defer pprof.StopCPUProfile()

	start := time.Now()

	addEvents(start, instance)
	log.Println("Elapsed:", time.Since(start))
}

func addEvents(ts time.Time, instance *cloudwatch.OutputPlugin) {
	for i := 0; i <= TestCycles; i++ {
		instance.AddEvent(&cloudwatch.Event{TS: ts, Record: record, Tag: "input.0"})
	}
}
