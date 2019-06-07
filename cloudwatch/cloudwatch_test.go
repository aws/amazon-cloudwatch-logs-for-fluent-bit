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

package cloudwatch

import (
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/awslabs/amazon-cloudwatch-logs-for-fluent-bit/cloudwatch/mock_cloudwatch"
	"github.com/awslabs/amazon-cloudwatch-logs-for-fluent-bit/plugins"
	"github.com/golang/mock/gomock"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

const (
	testRegion          = "us-west-2"
	testLogGroup        = "my-logs"
	testLogStreamPrefix = "my-prefix"
	testTag             = "tag"
)

func TestAddEvent(t *testing.T) {
	timer, _ := plugins.NewTimeout(func(d time.Duration) {
		logrus.Errorf("[firehose] timeout threshold reached: Failed to send logs for %v\n", d)
		logrus.Error("[firehose] Quitting Fluent Bit")
		os.Exit(1)
	})

	ctrl := gomock.NewController(t)
	mockCloudWatch := mock_cloudwatch.NewMockCloudWatchLogsClient(ctrl)

	mockCloudWatch.EXPECT().CreateLogStream(gomock.Any()).Do(func(input *cloudwatchlogs.CreateLogStreamInput) {
		assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
		assert.Equal(t, aws.StringValue(input.LogStreamName), testLogStreamPrefix+testTag, "Expected log group name to match")
	}).Return(&cloudwatchlogs.CreateLogStreamOutput{}, nil)

	output := OutputPlugin{
		region:          testRegion,
		logGroupName:    testLogGroup,
		logStreamPrefix: testLogStreamPrefix,
		client:          mockCloudWatch,
		backoff:         plugins.NewBackoff(),
		timer:           timer,
		streams:         make(map[string]*logStream),
	}

	record := map[interface{}]interface{}{
		"somekey": []byte("some value"),
	}

	output.AddEvent(testTag, record, time.Now())

}

func TestAddEventAndFlush(t *testing.T) {
	timer, _ := plugins.NewTimeout(func(d time.Duration) {
		logrus.Errorf("[firehose] timeout threshold reached: Failed to send logs for %v\n", d)
		logrus.Error("[firehose] Quitting Fluent Bit")
		os.Exit(1)
	})

	ctrl := gomock.NewController(t)
	mockCloudWatch := mock_cloudwatch.NewMockCloudWatchLogsClient(ctrl)

	gomock.InOrder(
		mockCloudWatch.EXPECT().CreateLogStream(gomock.Any()).Do(func(input *cloudwatchlogs.CreateLogStreamInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
			assert.Equal(t, aws.StringValue(input.LogStreamName), testLogStreamPrefix+testTag, "Expected log stream name to match")
		}).Return(&cloudwatchlogs.CreateLogStreamOutput{}, nil),
		mockCloudWatch.EXPECT().PutLogEvents(gomock.Any()).Do(func(input *cloudwatchlogs.PutLogEventsInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
			assert.Equal(t, aws.StringValue(input.LogStreamName), testLogStreamPrefix+testTag, "Expected log stream name to match")
		}).Return(&cloudwatchlogs.PutLogEventsOutput{
			NextSequenceToken: aws.String("token"),
		}, nil),
	)

	output := OutputPlugin{
		region:          testRegion,
		logGroupName:    testLogGroup,
		logStreamPrefix: testLogStreamPrefix,
		client:          mockCloudWatch,
		backoff:         plugins.NewBackoff(),
		timer:           timer,
		streams:         make(map[string]*logStream),
	}

	record := map[interface{}]interface{}{
		"somekey": []byte("some value"),
	}

	output.AddEvent(testTag, record, time.Now())
	output.Flush(testTag)

}
