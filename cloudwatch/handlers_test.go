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
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/stretchr/testify/assert"
)

func TestLogFormatHandler(t *testing.T) {
	httpReq, _ := http.NewRequest("POST", "", nil)
	r := &request.Request{
		HTTPRequest: httpReq,
		Body:        nil,
	}
	r.SetBufferBody([]byte{})

	handler := LogFormatHandler("json/emf")
	handler.Fn(r)

	header := r.HTTPRequest.Header.Get(logFormatHeader)
	assert.Equal(t, "json/emf", header)
}
