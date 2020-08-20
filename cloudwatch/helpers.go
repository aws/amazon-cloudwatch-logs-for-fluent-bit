package cloudwatch

import (
	"io"
	"strconv"
	"strings"

	"github.com/valyala/fasttemplate"
)

// tagKeysToMap converts a raw string into a go map.
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
func parseKeysTemplate(data map[interface{}]interface{}, keys string) (string, error) {
	t, err := fasttemplate.NewTemplate(keys, "['", "']")
	if err != nil {
		return "", err
	}

	return t.ExecuteFuncStringWithErr(func(w io.Writer, tag string) (int, error) {
		switch val := data[tag].(type) {
		case []byte:
			return w.Write(val)
		case string:
			return w.Write([]byte(val))
		case map[interface{}]interface{}:
			data = val // drill down another level.
			return 0, nil
		default: // missing
			return w.Write([]byte(tag))
		}
	})
}

// parseDataMapTags parses the provided tag values in template form,
// from an interface map (expected to contains strings or more maps)
func parseDataMapTags(e *Event, logTags []string, template string) (string, error) {
	t, err := fasttemplate.NewTemplate(template, "#(", ")")
	if err != nil {
		return "", err
	}

	return t.ExecuteFuncStringWithErr(func(w io.Writer, tag string) (int, error) {
		v := strings.Index(tag, "[")
		if v == -1 {
			v = len(tag)
		}

		if tag[:v] == "tag" {
			switch {
			default: // input string is either `tag` or `tag[`
				return w.Write([]byte(e.Tag))
			case len(tag) >= 5: // input string is at least tag[x where x is hopefully an integer 0-9.
				// The index value is always in the same position.
				if v, _ = strconv.Atoi(tag[4:5]); len(logTags) <= v {
					return w.Write([]byte("tag" + tag[4:5])) // not enough dots.
				}

				return w.Write([]byte(logTags[v]))
			}
		}

		switch val := e.Record[tag[:v]].(type) {
		case []byte:
			return w.Write(val)
		case string:
			return w.Write([]byte(val))
		case map[interface{}]interface{}:
			keyVal, err := parseKeysTemplate(val, tag[v:])
			if err != nil {
				return 0, err
			}

			return w.Write([]byte(keyVal))
		default: // missing
			return w.Write([]byte(tag))
		}
	})
}
