package dal

import (
	"fmt"
	"reflect"
)

//isValidForPrimaryKey check given type is valid for Primary Key in database/struct
func isValidForPrimaryKey(v reflect.Type) bool {
	switch v.Kind() {
	case reflect.Func, reflect.Map, reflect.Slice, reflect.Array, reflect.Ptr:
		return false
	case reflect.Struct:
		return false
	}
	return true
}

// Check if passed interface is of acceptable type
// for Insert/Update Struct
func checkInterfaceAcceptable(in interface{}, verb string, allowStruct bool, allowSlice bool, allowMap bool) error {
	// Allowed types to pass are
	//  a. *struct
	//  b. []*struct
	//  c. map[string]*struct

	// //acceptStr := "*structs | []*struct | map[string/int]*struct"
	// acceptStr := ""
	// if allowStruct {
	// 	acceptStr += "*structs"
	// }
	// if allowSlice {
	// 	if acceptStr != "" {
	// 		acceptStr += " | "
	// 	}
	// 	acceptStr += "[]*struct"
	// }
	// if allowMap {
	// 	if acceptStr != "" {
	// 		acceptStr += " | "
	// 	}
	// 	acceptStr += "map[string/int]*struct"
	// }

	baseType := reflect.TypeOf(in)
	//fmt.Printf("Kind of passed interface is: %v\n", baseType.Kind())
	var v reflect.Type

	if baseType.Kind() == reflect.Ptr {
		baseType = baseType.Elem()
	}
	//fmt.Printf("    Kind of passed interface is: %v\n", baseType.Kind())

	if baseType.Kind() == reflect.Struct && allowStruct {
		return nil

	} else if baseType.Kind() == reflect.Slice && allowSlice {
		// slice must be of pointer to Structs i.e. []*struct
		v = baseType.Elem()
		if v.Kind() == reflect.Ptr {
			v = v.Elem()
			if v.Kind() != reflect.Struct {
				return fmt.Errorf("[2] %s only accepts %s; got %T", verb, getAcceptStr(allowStruct, allowSlice, allowMap), v)
			}
		} else {
			return fmt.Errorf("[3] %s only accepts %s; got %T", verb, getAcceptStr(allowStruct, allowSlice, allowMap), v)
		}
	} else if baseType.Kind() == reflect.Map && allowMap {
		v = baseType.Elem()
		// it must be map[key]interface
		if v.Kind() == reflect.Interface {
			// now check if each element of map is *struct
			err := checkMapAcceptable(in)
			if err != nil {
				return err
			}
		} else {
			return fmt.Errorf("[4] %s only accepts %s; got %T", verb, getAcceptStr(allowStruct, allowSlice, allowMap), v)
		}
	} else {
		//fmt.Printf("\nBasekind: %v -- %T\n", baseType.Kind(), baseType.Kind())
		return fmt.Errorf("%s only accepts %s; got %T", verb, getAcceptStr(allowStruct, allowSlice, allowMap), in)
	}

	return nil
}

func getAcceptStr(allowStruct bool, allowSlice bool, allowMap bool) string {
	acceptStr := ""
	if allowStruct {
		acceptStr += "*structs"
	}
	if allowSlice {
		if acceptStr != "" {
			acceptStr += " | "
		}
		acceptStr += "[]*struct"
	}
	if allowMap {
		if acceptStr != "" {
			acceptStr += " | "
		}
		acceptStr += "map[string/int]*struct"
	}
	return acceptStr
}

// checkMapAcceptable checks if each element of map is *struct nothing else
func checkMapAcceptable(in interface{}) error {
	v := reflect.ValueOf(in)
	for _, key := range v.MapKeys() {
		value := v.MapIndex(key).Interface()
		// value must be pointer to struct
		typ := reflect.TypeOf(value)
		if typ.Kind() == reflect.Ptr {
			el := typ.Elem()
			//fmt.Printf("Debug: Kind for key[%v]: %v,  value = %v\n", key, el.Kind(), el)
			if el.Kind() != reflect.Struct {
				return fmt.Errorf("Map elements should be *struct; got %v for key %v", el, key)
			}
		} else {
			return fmt.Errorf("Map elements should be *struct; got %v for key %v", typ, key)
		}
	}
	return nil

	// baseType := reflect.TypeOf(in)
	// if baseType.Kind() == reflect.Map {
	// 	el := baseType.Elem()

	// 	// it must be map[key]interface
	// 	if el.Kind() == reflect.Interface {
	// 		// now check if each element of map is *struct

	// 		v := reflect.ValueOf(in)
	// 		for _, key := range v.MapKeys() {
	// 			value := v.MapIndex(key).Interface()
	// 			// strct must be pointer to struct
	// 			typ := reflect.TypeOf(value)
	// 			if typ.Kind() == reflect.Ptr {
	// 				el = typ.Elem()
	// 				fmt.Printf("Kind for key[%v]: %v,  value = %v\n", key, el.Kind(), el)
	// 				if el.Kind() != reflect.Struct {
	// 					return fmt.Errorf("Map elements should be *struct; got %v for key %v\n", el, key)
	// 				}
	// 			} else {
	// 				return fmt.Errorf("Map elements should be *struct; got %v for key %v\n", typ, key)
	// 			}
	// 		}
	// 	} else {
	// 		return fmt.Errorf("Insert only accepts *struct | []*struct | map[string]*struct; got %T", el)
	// 	}
	// }

}
