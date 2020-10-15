package cloudwatch

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/valyala/bytebufferpool"
	"github.com/valyala/fasttemplate"
)

// Errors output by the help procedures.
var (
	ErrNoTagValue     = fmt.Errorf("not enough dots in the tag to satisfy the index position")
	ErrMissingTagName = fmt.Errorf("tag name not found")
	ErrMissingSubName = fmt.Errorf("sub-tag name not found")
)

// newTemplate is the only place you'll find the template start and end tags.
func newTemplate(template string) (*fastTemplate, error) {
	t, err := fasttemplate.NewTemplate(template, "$(", ")")

	return &fastTemplate{Template: t, String: template}, err
}

// tagKeysToMap converts a raw string into a go map.
// This is used by input data to create AWS tags applied to newly-created log groups.
//
// The input string should be match this: "key=value,key2=value2".
// Spaces are trimmed, empty values are permitted, empty keys are ignored.
// The final value in the input string wins in case of duplicate keys.
func tagKeysToMap(tags string) map[string]*string {
	output := make(map[string]*string)

	for _, tag := range strings.Split(strings.TrimSpace(tags), ",") {
		split := strings.SplitN(tag, "=", 2)
		key := strings.TrimSpace(split[0])
		value := ""

		if key == "" {
			continue
		}

		if len(split) > 1 {
			value = strings.TrimSpace(split[1])
		}

		output[key] = &value
	}

	if len(output) == 0 {
		return nil
	}

	return output
}

// parseKeysTemplate takes in an interface map and a list of nested keys. It returns
// the value of the final key, or the name of the first key not found in the chain.
// example keys := "['level1']['level2']['level3']"
// This is called by parseDataMapTags any time a nested value is found in a log Event.
// This procedure checks if any of the nested values match variable identifiers in the logStream or logGroups.
func parseKeysTemplate(data map[interface{}]interface{}, keys string, w io.Writer) (int64, error) {
	return fasttemplate.ExecuteFunc(keys, "['", "']", w, func(w io.Writer, tag string) (int, error) {
		switch val := data[tag].(type) {
		case []byte:
			return w.Write(val)
		case string:
			return w.Write([]byte(val))
		case map[interface{}]interface{}:
			data = val // drill down another level.
			return 0, nil
		default: // missing
			return 0, fmt.Errorf("%s: %w", tag, ErrMissingSubName)
		}
	})
}

// parseDataMapTags parses the provided tag values in template form,
// from an interface{} map (expected to contain strings or more interface{} maps).
// This runs once for every log line.
// Used to fill in any template variables that may exist in the logStream or logGroup names.
func parseDataMapTags(e *Event, logTags []string, t *fastTemplate, metadata TaskMetadata, uuid string, w io.Writer) (int64, error) {
	return t.ExecuteFunc(w, func(w io.Writer, tag string) (int, error) {
		switch tag {
		case "ecs_task_id":
			if metadata.TaskID != "" {
				return w.Write([]byte(metadata.TaskID))
			}

			return 0, fmt.Errorf("Failed to fetch ecs_task_id; The container is not running in ECS")
		case "ecs_cluster":
			if metadata.Cluster != "" {
				return w.Write([]byte(metadata.Cluster))
			}

			return 0, fmt.Errorf("Failed to fetch ecs_cluster; The container is not running in ECS")
		case "ecs_task_arn":
			if metadata.TaskARN != "" {
				return w.Write([]byte(metadata.TaskARN))
			}

			return 0, fmt.Errorf("Failed to fetch ecs_task_arn; The container is not running in ECS")
		case "uuid":
			return w.Write([]byte(uuid))
		}

		v := strings.Index(tag, "[")
		if v == -1 {
			v = len(tag)
		}

		if tag[:v] == "tag" {
			switch {
			default: // input string is either `tag` or `tag[`, so return the $tag.
				return w.Write([]byte(e.Tag))
			case len(tag) >= 5: // input string is at least "tag[x" where x is hopefully an integer 0-9.
				// The index value is always in the same position: 4:5 (this is why supporting more than 0-9 is rough)
				if v, _ = strconv.Atoi(tag[4:5]); len(logTags) <= v {
					return 0, fmt.Errorf("%s: %w", tag, ErrNoTagValue)
				}

				return w.Write([]byte(logTags[v]))
			}
		}

		switch val := e.Record[tag[:v]].(type) {
		case string:
			return w.Write([]byte(val))
		case map[interface{}]interface{}:
			i, err := parseKeysTemplate(val, tag[v:], w)

			return int(i), err
		case []byte:
			// we should never land here because the interface{} map should have already been converted to strings.
			return w.Write(val)
		default: // missing
			return 0, fmt.Errorf("%s: %w", tag, ErrMissingTagName)
		}
	})
}

// sanitizer implements io.Writer for fasttemplate usage.
// Instead of just writing bytes to a buffer, sanitize them first.
type sanitizer struct {
	sanitize func(b []byte) []byte
	buf      *bytebufferpool.ByteBuffer
}

// Write completes the io.Writer implementation.
func (s *sanitizer) Write(b []byte) (int, error) {
	return s.buf.Write(s.sanitize(b))
}

// sanitizeGroup removes special characters from the log group names bytes.
// https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-logs-loggroup.html
func sanitizeGroup(b []byte) []byte {
	for i, r := range b {
		// 45-47 = / . -
		// 48-57 = 0-9
		// 65-90 = A-Z
		// 95 = _
		// 97-122 = a-z
		if r == 95 || (r > 44 && r < 58) ||
			(r > 64 && r < 91) || (r > 96 && r < 123) {
			continue
		}

		b[i] = '.'
	}

	return b
}

// sanitizeStream removes : and * from the log stream bytes.
// https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-logs-logstream.html
func sanitizeStream(b []byte) []byte {
	for i, r := range b {
		if r == '*' || r == ':' {
			b[i] = '.'
		}
	}

	return b
}
