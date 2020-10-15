package cloudwatch

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/valyala/bytebufferpool"
)

func TestTagKeysToMap(t *testing.T) {
	t.Parallel()

	// Testable values. Purposely "messed up" - they should all parse out OK.
	values := " key1 =value , key2=value2, key3= value3 ,key4=, key5  = v5,,key7==value7," +
		" k8, k9,key1=value1,space key = space value"
	// The values above should return a map like this.
	expect := map[string]string{"key1": "value1", "key2": "value2", "key3": "value3",
		"key4": "", "key5": "v5", "key7": "=value7", "k8": "", "k9": "", "space key": "space value"}

	for k, v := range tagKeysToMap(values) {
		assert.Equal(t, *v, expect[k], "Tag key or value failed parser.")
	}
}

func TestParseDataMapTags(t *testing.T) {
	t.Parallel()

	template := testTemplate("$(ecs_task_id).$(ecs_cluster).$(ecs_task_arn).$(uuid).$(tag).$(pam['item2']['subitem2']['more']).$(pam['item']).$(pam['item2'])." +
		"$(pam['item2']['subitem'])-$(pam['item2']['subitem2']['more'])-$(tag[1])")
	data := map[interface{}]interface{}{
		"pam": map[interface{}]interface{}{
			"item": "soup",
			"item2": map[interface{}]interface{}{"subitem": []byte("SubIt3m"),
				"subitem2": map[interface{}]interface{}{"more": "final"}},
		},
	}

	s := &sanitizer{buf: bytebufferpool.Get(), sanitize: sanitizeGroup}
	defer bytebufferpool.Put(s.buf)

	_, err := parseDataMapTags(&Event{Record: data, Tag: "syslog.0"}, []string{"syslog", "0"}, template, TaskMetadata{Cluster: "cluster", TaskARN: "taskARN", TaskID: "taskID"}, "123", s)

	assert.Nil(t, err, err)
	assert.Equal(t, "taskID.cluster.taskARN.123.syslog.0.final.soup..SubIt3m-final-0", s.buf.String(), "Rendered string is incorrect.")

	// Test missing variables. These should always return an error and an empty string.
	s.buf.Reset()
	template = testTemplate("$(missing-variable).stuff")
	_, err = parseDataMapTags(&Event{Record: data, Tag: "syslog.0"}, []string{"syslog", "0"}, template, TaskMetadata{Cluster: "cluster", TaskARN: "taskARN", TaskID: "taskID"}, "123", s)
	assert.EqualError(t, err, "missing-variable: "+ErrMissingTagName.Error(), "the wrong error was returned")
	assert.Empty(t, s.buf.String())

	s.buf.Reset()
	template = testTemplate("$(pam['item6']).stuff")
	_, err = parseDataMapTags(&Event{Record: data, Tag: "syslog.0"}, []string{"syslog", "0"}, template, TaskMetadata{}, "", s)
	assert.EqualError(t, err, "item6: "+ErrMissingSubName.Error(), "the wrong error was returned")
	assert.Empty(t, s.buf.String())

	s.buf.Reset()
	template = testTemplate("$(tag[9]).stuff")
	_, err = parseDataMapTags(&Event{Record: data, Tag: "syslog.0"}, []string{"syslog", "0"}, template, TaskMetadata{}, "", s)
	assert.EqualError(t, err, "tag[9]: "+ErrNoTagValue.Error(), "the wrong error was returned")
	assert.Empty(t, s.buf.String())
}

func TestSanitizeGroup(t *testing.T) {
	t.Parallel()

	tests := map[string]string{ // "send": "expect",
		"this.is.a.log.group.name":             "this.is.a.log.group.name",
		"1234567890abcdefghijklmnopqrstuvwxyz": "1234567890abcdefghijklmnopqrstuvwxyz",
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ":           "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
		`!@#$%^&*()_+}{][=-';":/.?>,<~"']}`:    ".........._......-..../..........",
		"":                                     "",
	}

	for send, expect := range tests {
		actual := sanitizeGroup([]byte(send))
		assert.Equal(t, expect, string(actual), "the wrong characters were modified in sanitizeGroup")
	}
}

func TestSanitizeStream(t *testing.T) {
	t.Parallel()

	tests := map[string]string{ // "send": "expect",
		"this.is.a.log.group.name":             "this.is.a.log.group.name",
		"1234567890abcdefghijklmnopqrstuvwxyz": "1234567890abcdefghijklmnopqrstuvwxyz",
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ":           "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
		`!@#$%^&*()_+}{][=-';":/.?>,<~"']}`:    `!@#$%^&.()_+}{][=-';"./.?>,<~"']}`,
		"":                                     "",
	}

	for send, expect := range tests {
		actual := sanitizeStream([]byte(send))
		assert.Equal(t, expect, string(actual), "the wrong characters were modified in sanitizeStream")
	}
}
