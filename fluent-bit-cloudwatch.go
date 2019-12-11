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
	"fmt"
	"unsafe"

	"time"

	"github.com/aws/amazon-cloudwatch-logs-for-fluent-bit/cloudwatch"
	"github.com/aws/amazon-kinesis-firehose-for-fluent-bit/plugins"
	"github.com/fluent/fluent-bit-go/output"

	"github.com/sirupsen/logrus"
)
import (
	"strings"
)

var (
	pluginInstances []*cloudwatch.OutputPlugin
)

func addPluginInstance(ctx unsafe.Pointer) error {
	pluginID := len(pluginInstances)

	config := getConfiguration(ctx, pluginID)
	err := config.Validate()
	if err != nil {
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

//export FLBPluginRegister
func FLBPluginRegister(ctx unsafe.Pointer) int {
	return output.FLBPluginRegister(ctx, "cloudwatch", "AWS CloudWatch Fluent Bit Plugin!")
}

func getConfiguration(ctx unsafe.Pointer, pluginID int) cloudwatch.OutputPluginConfig {
	config := cloudwatch.OutputPluginConfig{}
	config.PluginInstanceID = pluginID
	config.LogGroupName = output.FLBPluginConfigKey(ctx, "log_group_name")
	logrus.Infof("[cloudwatch %d] plugin parameter log_group = '%s'\n", pluginID, config.LogGroupName)
	config.LogStreamPrefix = output.FLBPluginConfigKey(ctx, "log_stream_prefix")
	logrus.Infof("[cloudwatch %d] plugin parameter log_stream_prefix = '%s'\n", pluginID, config.LogStreamPrefix)
	config.LogStreamName = output.FLBPluginConfigKey(ctx, "log_stream_name")
	logrus.Infof("[cloudwatch %d] plugin parameter log_stream = '%s'\n", pluginID, config.LogStreamName)
	config.Region = output.FLBPluginConfigKey(ctx, "region")
	logrus.Infof("[cloudwatch %d] plugin parameter region = '%s'\n", pluginID, config.Region)
	config.LogKey = output.FLBPluginConfigKey(ctx, "log_key")
	logrus.Infof("[cloudwatch %d] plugin parameter log_key = '%s'\n", pluginID, config.LogKey)
	config.RoleARN = output.FLBPluginConfigKey(ctx, "role_arn")
	logrus.Infof("[cloudwatch %d] plugin parameter role_arn = '%s'\n", pluginID, config.RoleARN)
	config.AutoCreateGroup = getBoolParam(ctx, "auto_create_group", false)
	logrus.Infof("[cloudwatch %d] plugin parameter auto_create_group = '%v'\n", pluginID, config.AutoCreateGroup)
	config.CWEndpoint = output.FLBPluginConfigKey(ctx, "endpoint")
	logrus.Infof("[cloudwatch %d] plugin parameter endpoint = '%s'\n", pluginID, config.CWEndpoint)
	config.CredsEndpoint = output.FLBPluginConfigKey(ctx, "credentials_endpoint")
	logrus.Infof("[cloudwatch %d] plugin parameter credentials_endpoint = %s\n", pluginID, config.CredsEndpoint)

	return config
}

func getBoolParam(ctx unsafe.Pointer, param string, defaultVal bool) bool {
	val := strings.ToLower(output.FLBPluginConfigKey(ctx, param))
	if val == "true" {
		return true
	} else if val == "false" {
		return false
	} else {
		return defaultVal
	}
}

//export FLBPluginInit
func FLBPluginInit(ctx unsafe.Pointer) int {
	plugins.SetupLogger()

	err := addPluginInstance(ctx)
	if err != nil {
		logrus.Error(err)
		return output.FLB_ERROR
	}
	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	var count int
	var ret int
	var ts interface{}
	var record map[interface{}]interface{}

	// Create Fluent Bit decoder
	dec := output.NewDecoder(data, int(length))

	cloudwatchLogs := getPluginInstance(ctx)

	fluentTag := C.GoString(tag)
	logrus.Debugf("[cloudwatch %d] Found logs with tag: %s\n", cloudwatchLogs.PluginInstanceID, fluentTag)

	for {
		// Extract Record
		ret, ts, record = output.GetRecord(dec)
		if ret != 0 {
			break
		}

		var timestamp time.Time
		switch tts := ts.(type) {
		case output.FLBTime:
			timestamp = tts.Time
		case uint64:
			// when ts is of type uint64 it appears to
			// be the amount of seconds since unix epoch.
			timestamp = time.Unix(int64(tts), 0)
		default:
			timestamp = time.Now()
		}

		retCode := cloudwatchLogs.AddEvent(fluentTag, record, timestamp)
		if retCode != output.FLB_OK {
			return retCode
		}
		count++
	}
	err := cloudwatchLogs.Flush(fluentTag)
	if err != nil {
		fmt.Println(err)
		// TODO: Better error handling
		return output.FLB_RETRY
	}

	logrus.Debugf("[cloudwatch %d] Processed %d events with tag %s\n", cloudwatchLogs.PluginInstanceID, count, fluentTag)

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

func main() {
}
