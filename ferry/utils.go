package ferry

import (
	"encoding/json"
	"log/slog"
	"reflect"
	"strconv"
)

func safeUnmarshal[T any](data string, defaultT T, logger *slog.Logger, key string) T {
	var result T

	if err := json.Unmarshal([]byte(data), &result); err == nil {
		return result
	}

	valueType := reflect.TypeOf(result)
	if valueType.Kind() == reflect.String {
		if str, ok := any(&result).(*string); ok {
			*str = data
			return result
		}
	}

	if valueType.Kind() == reflect.Int || valueType.Kind() == reflect.Int64 {
		if val, err := strconv.ParseInt(data, 10, 64); err == nil {
			if intVal, ok := any(&result).(*int); ok {
				*intVal = int(val)
				return result
			}
			if int64Val, ok := any(&result).(*int64); ok {
				*int64Val = val
				return result
			}
		}
	}

	if valueType.Kind() == reflect.Float64 {
		if val, err := strconv.ParseFloat(data, 64); err == nil {
			if floatVal, ok := any(&result).(*float64); ok {
				*floatVal = val
				return result
			}
		}
	}

	if valueType.Kind() == reflect.Bool {
		if val, err := strconv.ParseBool(data); err == nil {
			if boolVal, ok := any(&result).(*bool); ok {
				*boolVal = val
				return result
			}
		}
	}

	logger.Warn("Failed to unmarshal value, returning default", "key", key, "data", data, "type", valueType.String())
	return defaultT
}
