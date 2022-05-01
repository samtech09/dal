package dal

import "time"

func toString(t interface{}) (string, bool) {
	value, ok := t.(string)
	if ok {
		return value, true
	}
	if val, ok := t.([]byte); ok {
		return string(val), true
	}
	// return default value
	return "", false
}

func toStringx(t interface{}) string {
	value, ok := t.(string)
	if ok {
		return value
	}
	if val, ok := t.([]byte); ok {
		return string(val)
	}
	// return default value
	return ""
}

func toBool(t interface{}) (bool, bool) {
	value, ok := t.(bool)
	if ok {
		return value, true
	}
	// return default value
	return false, false
}

func toBoolx(t interface{}) bool {
	value, ok := t.(bool)
	if ok {
		return value
	}
	// return default value
	return false
}

func toInt(t interface{}) (int64, bool) {
	switch t := t.(type) {
	case int:
		return int64(t), true
	case int8:
		return int64(t), true
	case int16:
		return int64(t), true
	case int32:
		return int64(t), true
	case int64:
		return int64(t), true
	}
	// return default value
	return 0, false
}

func toIntx(t interface{}) int64 {
	switch t := t.(type) {
	case int:
		return int64(t)
	case int8:
		return int64(t)
	case int16:
		return int64(t)
	case int32:
		return int64(t)
	case int64:
		return int64(t)
	}
	// return default value
	return 0
}

func toIntx32(t interface{}) int {
	switch t := t.(type) {
	case int:
		return int(t)
	case int8:
		return int(t)
	case int16:
		return int(t)
	case int32:
		return int(t)
	case int64:
		return int(t)
	}
	// return default value
	return 0
}

func toUInt(t interface{}) (uint64, bool) {
	switch t := t.(type) {
	case uint:
		return uint64(t), true
	case uintptr:
		return uint64(t), true
	case uint8:
		return uint64(t), true
	case uint16:
		return uint64(t), true
	case uint32:
		return uint64(t), true
	case uint64:
		return uint64(t), true
	}
	// return default value
	return 0, false
}

func toUIntx(t interface{}) uint64 {
	switch t := t.(type) {
	case uint:
		return uint64(t)
	case uintptr:
		return uint64(t)
	case uint8:
		return uint64(t)
	case uint16:
		return uint64(t)
	case uint32:
		return uint64(t)
	case uint64:
		return uint64(t)
	}
	// return default value
	return 0
}

func toFloat(t interface{}) (float64, bool) {
	switch t := t.(type) {
	case float32:
		return float64(t), true
	case float64:
		return float64(t), true
	}
	// return default value
	return 0, false
}

func toFloatx(t interface{}) float64 {
	switch t := t.(type) {
	case float32:
		return float64(t)
	case float64:
		return float64(t)
	}
	// return default value
	return 0
}

func toTime(t interface{}) (time.Time, bool) {
	value, ok := t.(time.Time)
	if ok {
		return value, true
	}
	// return default value
	return time.Time{}, false
}

func toTimex(t interface{}) time.Time {
	value, ok := t.(time.Time)
	if ok {
		return value
	}
	// return default value
	return time.Time{}
}
