// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"). You may
// not use this file except in compliance with the License. A copy of the
// License is located at
//
//	http://aws.amazon.com/apache2.0/
//
// or in the "license" file accompanying this file. This file is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
// express or implied. See the License for the specific language governing
// permissions and limitations under the License.

package main

import (
	"C"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/aws/amazon-cloudwatch-logs-for-fluent-bit/cloudwatch"
	"github.com/aws/amazon-kinesis-firehose-for-fluent-bit/plugins"
	"github.com/fluent/fluent-bit-go/output"
	"github.com/sirupsen/logrus"
)

var (
	pluginInstances []*cloudwatch.OutputPlugin // nolint: gochecknoglobals
)

func addPluginInstance(ctx unsafe.Pointer) error {
	pluginID := len(pluginInstances)
	config := getConfiguration(ctx, pluginID)

	if err := config.Validate(); err != nil {
		return err
	}

	instance, err := cloudwatch.NewOutputPlugin(config)
	if err != nil {
		return err
	}

	output.FLBPluginSetContext(ctx, pluginID)

	pluginInstances = append(pluginInstances, instance)

	return nil
}

func getPluginInstance(ctx unsafe.Pointer) *cloudwatch.OutputPlugin {
	pluginID := output.FLBPluginGetContext(ctx).(int)
	return pluginInstances[pluginID]
}

// FLBPluginRegister is exported for fluent-bit.
func FLBPluginRegister(ctx unsafe.Pointer) int {
	return output.FLBPluginRegister(ctx, "cloudwatch", "AWS CloudWatch Fluent Bit Plugin!")
}

func getConfiguration(ctx unsafe.Pointer, pluginID int) cloudwatch.OutputPluginConfig {
	config := cloudwatch.OutputPluginConfig{}
	config.PluginInstanceID = pluginID

	config.LogGroupName = output.FLBPluginConfigKey(ctx, "log_group_name")
	logrus.Infof("[cloudwatch %d] plugin parameter log_group = '%s'", pluginID, config.LogGroupName)

	config.LogStreamPrefix = output.FLBPluginConfigKey(ctx, "log_stream_prefix")
	logrus.Infof("[cloudwatch %d] plugin parameter log_stream_prefix = '%s'", pluginID, config.LogStreamPrefix)

	config.LogStreamName = output.FLBPluginConfigKey(ctx, "log_stream_name")
	logrus.Infof("[cloudwatch %d] plugin parameter log_stream_name = '%s'", pluginID, config.LogStreamName)

	config.Region = output.FLBPluginConfigKey(ctx, "region")
	logrus.Infof("[cloudwatch %d] plugin parameter region = '%s'", pluginID, config.Region)

	config.LogKey = output.FLBPluginConfigKey(ctx, "log_key")
	logrus.Infof("[cloudwatch %d] plugin parameter log_key = '%s'", pluginID, config.LogKey)

	config.RoleARN = output.FLBPluginConfigKey(ctx, "role_arn")
	logrus.Infof("[cloudwatch %d] plugin parameter role_arn = '%s'", pluginID, config.RoleARN)

	config.NewLogGroupTags = output.FLBPluginConfigKey(ctx, "new_log_group_tags")
	logrus.Infof("[cloudwatch %d] plugin parameter new_log_group_tags = '%s'", pluginID, config.NewLogGroupTags)

	config.LogRetentionDays, _ = strconv.ParseInt(output.FLBPluginConfigKey(ctx, "log_retention_days"), 10, 64)
	logrus.Infof("[cloudwatch %d] plugin parameter log_retention_days = '%d'", pluginID, config.LogRetentionDays)

	config.CWEndpoint = output.FLBPluginConfigKey(ctx, "endpoint")
	logrus.Infof("[cloudwatch %d] plugin parameter endpoint = '%s'", pluginID, config.CWEndpoint)

	config.STSEndpoint = output.FLBPluginConfigKey(ctx, "sts_endpoint")
	logrus.Infof("[cloudwatch %d] plugin parameter sts_endpoint = '%s'", pluginID, config.STSEndpoint)

	config.CredsEndpoint = output.FLBPluginConfigKey(ctx, "credentials_endpoint")
	logrus.Infof("[cloudwatch %d] plugin parameter credentials_endpoint = %s", pluginID, config.CredsEndpoint)

	config.LogFormat = output.FLBPluginConfigKey(ctx, "log_format")
	logrus.Infof("[cloudwatch %d] plugin parameter log_format = '%s'", pluginID, config.LogFormat)

	return config
}

//export FLBPluginInit
func FLBPluginInit(ctx unsafe.Pointer) int {
	setupLogger()

	err := addPluginInstance(ctx)
	if err != nil {
		logrus.Errorln("addPluginInstance():", err)
		return output.FLB_ERROR
	}

	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	var (
		count     int
		ret       int
		ts        interface{}
		timestamp time.Time
		record    map[interface{}]interface{}
		// Create Fluent Bit decoder
		dec            = output.NewDecoder(data, int(length))
		cloudwatchLogs = getPluginInstance(ctx)
		fluentTag      = C.GoString(tag)
	)

	logrus.Debugf("[cloudwatch %d] Processing events with tag: %s", cloudwatchLogs.PluginInstanceID, fluentTag)

	for {
		// Extract Record
		ret, ts, record = output.GetRecord(dec)
		if ret != 0 {
			break
		}

		switch tts := ts.(type) {
		case output.FLBTime:
			timestamp = tts.Time
		case uint64:
			// when ts is of type uint64 it appears to
			// be the amount of seconds since unix epoch.
			timestamp = time.Unix(int64(tts), 0)
		default:
			timestamp = time.Now() // This is a lot slower than the above two options, avoid going here.

			logrus.Debugf("[cloudwatch %d] Timestamp format unknown: %v", cloudwatchLogs.PluginInstanceID, tts)
		}

		retCode := cloudwatchLogs.AddEvent(&cloudwatch.Event{Tag: fluentTag, Record: record, TS: timestamp})
		if retCode != output.FLB_OK {
			return retCode
		}

		count++
	}

	err := cloudwatchLogs.Flush()
	if err != nil {
		logrus.Errorln("Flush():", err)
		// XXX: Better error handling.
		// ie. Retry creating missing log group.
		// maybe throw the error message back into a method to handle things better.
		return output.FLB_RETRY
	}

	logrus.Debugf("[cloudwatch %d] Processed %d events.", cloudwatchLogs.PluginInstanceID, count)

	// Return options:
	//
	// output.FLB_OK    = data have been processed.
	// output.FLB_ERROR = unrecoverable error, do not try this again.
	// output.FLB_RETRY = retry to flush later.
	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	return output.FLB_OK
}

func setupLogger() {
	plugins.SetupLogger()

	if !strings.EqualFold(os.Getenv("FLB_LOG_LEVEL"), "DEBUG") {
		return
	}

	// Add the calling method and line number to each debug log line.
	logrus.SetReportCaller(true)
	logrus.SetFormatter(&logrus.TextFormatter{
		CallerPrettyfier: func(f *runtime.Frame) (string, string) {
			return f.Function + "()", path.Base(f.File) + strconv.Itoa(f.Line)
		},
	})
}

func main() {
	// Adding these procedure calls removes linter warnings that they are "unused functions."
	if false == true {
		FLBPluginInit(nil)
		FLBPluginRegister(nil)
		FLBPluginFlushCtx(nil, nil, 0, nil)
		FLBPluginExit()
	}

	logrus.Fatal("ERROR: do not compile and run this code as an application; it's a shared library")
}
