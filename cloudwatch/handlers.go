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

import "github.com/aws/aws-sdk-go/aws/request"

const logFormatHeader = "x-amzn-logs-format"

// LogFormatHandler returns an http request handler that sets an HTTP header.
// The header is used to indicate the format of the logs being sent.
func LogFormatHandler(format string) request.NamedHandler {
	return request.NamedHandler{
		Name: "LogFormatHandler",
		Fn: func(req *request.Request) {
			req.HTTPRequest.Header.Set(logFormatHeader, format)
		},
	}
}
