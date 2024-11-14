package healer

import (
	"reflect"
	"strconv"
	"strings"
)

func healerTags(r any) (fieldsVersions map[string]uint16) {
	fieldsVersions = make(map[string]uint16)

	for i := 0; i < reflect.ValueOf(r).NumField(); i++ {
		field := reflect.TypeOf(r).Field(i)
		healerTag := field.Tag.Get("healer")
		if healerTag != "" {
			parts := strings.Split(healerTag, ":")
			if len(parts) > 1 && parts[1] != "" && parts[0] == "minVersion" {
				if ver, err := strconv.Atoi(parts[1]); err == nil {
					fieldsVersions[field.Name] = uint16(ver)
				}
			}
		}
	}
	return fieldsVersions
}
