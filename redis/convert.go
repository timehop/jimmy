package redis

import "errors"

// Some redis functions, such as HGETALL, return an even-numbered list of strings that represent
// key-value pairs. This converts such a list into a map. It passes through an error value, similar
// to redigo’s convenience conversion functions, so it can be used with a minimum of ceremony.
func stringMap(strings []string, err error) (map[string]string, error) {
	if strings == nil {
		if err == nil {
			return nil, errors.New("redis: cannot convert response slice to map as it is nil")
		}
		return nil, err
	}

	lenStrings := len(strings)

	if lenStrings%2 != 0 {
		if err == nil {
			return nil, errors.New("redis: cannot convert response slice to map as it has an odd number of values")
		}
		return nil, err
	}

	result := map[string]string{}
	for i := 0; i < lenStrings; i += 2 {
		result[strings[i]] = strings[i+1]
	}
	return result, err
}

// Some redis functions, such as HMGET, return a slice of strings that correspond to a
// supplied slice of field names (keys). This splices those two slices into a map. It passes
// through an error value, similar to redigo’s convenience conversion functions, so it can be used
// with a minimum of ceremony.
func spliceMap(keys []string, vals []string, err error) (map[string]string, error) {
	if keys == nil || vals == nil {
		if err == nil {
			return nil, errors.New("redis: cannot splice keys supplied to HMGET with values returned because one or both slices are nil")
		}
		return nil, err
	}

	lenKeys := len(keys)
	lenVals := len(vals)

	if lenKeys != lenVals {
		if err == nil {
			return nil, errors.New("redis: cannot splice keys supplied to HMGET with values returned because their lengths are different")
		}
		return nil, err
	}

	result := map[string]string{}
	for i := 0; i < lenKeys; i++ {
		result[keys[i]] = vals[i]
	}
	return result, err
}

// Some Connection methods, such as HMSet, accept a map[string]interface{} but need to pass the
// values therein as a []interface{} which alternates between keys and values. This converts such
// a map into such a slice.
func mapToSlice(m map[string]interface{}) []interface{} {
	result := make([]interface{}, len(m)*2)

	i := 0
	for k, v := range m {
		result[i] = k
		result[i+1] = v
		i += 2
	}

	return result
}
