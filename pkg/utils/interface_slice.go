package utils

import (
	"reflect"
)

// Converts args to an interface slice using the following rules:
//
// - Returns empty slice if no args are passed
//
// - For a single argument which is of a slice type, the slice
//   is converted and returned.
//
// - For a single argument which is not a slice type, the value is
//   returned within a single-element slice.
//
// - For multiple arguments, returns a slice with all the args
//
func InterfaceSlice(any ...interface{}) []interface{} {
	if len(any) == 0 {
		return []interface{}{}
	}

	if len(any) == 1 {
		// FUTURE: there has to be a faster way to do this right? I guess
		// that under the hood this is what happens to the arguments
		// passed to the function. I'd assume it can elide a bunch of the
		// reflection given it knows the types.
		v := reflect.ValueOf(any[0])
		if v.Type().Kind() == reflect.Slice {
			vLen := v.Len()
			out := make([]interface{}, vLen)
			for i := 0; i < vLen; i++ {
				out[i] = v.Index(i).Interface()
			}
			return out
		}

		return []interface{}{any}
	}

	return any
}
