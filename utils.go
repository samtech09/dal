package dal

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
)

//rowsToMap converts given sqlx.Rows to slice of RowMap
func rowsToMap(rows *sqlx.Rows) ([]RowMap, error) {
	results := []RowMap{}
	for rows.Next() {
		result := make(RowMap)
		err := rows.MapScan(result)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}

	return results, nil
}

//rowsToMap converts given sqlx.Rows to slice of RowMap
func rowsToSliceInt(rows *sqlx.Rows) ([]int, error) {
	results := []int{}
	for rows.Next() {
		result, err := rows.SliceScan()
		if err != nil {
			return nil, err
		}
		results = append(results, toIntx32(result[0]))
	}

	return results, nil
}

//rowsToMap converts given sqlx.Rows to slice of RowMap
func rowsToSliceInt64(rows *sqlx.Rows) ([]int64, error) {
	results := []int64{}
	for rows.Next() {
		result, err := rows.SliceScan()
		if err != nil {
			return nil, err
		}
		results = append(results, toIntx(result[0]))
	}

	return results, nil
}

//rowsToMap converts given sqlx.Rows to slice of RowMap
func rowsToSliceStr(rows *sqlx.Rows) ([]string, error) {
	results := []string{}
	for rows.Next() {
		result, err := rows.SliceScan()
		if err != nil {
			return nil, err
		}
		results = append(results, toStringx(result[0]))
	}

	return results, nil
}

//rowsToStruct converts given sqlx.Rows to slice of given Struct
func rowsToStruct(rows *sqlx.Rows, dest interface{}) error {

	// make sure dst is an appropriate type
	dstVal := reflect.ValueOf(dest)
	if dstVal.Kind() != reflect.Ptr || dstVal.IsNil() {
		return fmt.Errorf("rowsToStruct called with non-pointer destination: %T", dest)
	}
	sliceVal := dstVal.Elem()
	if sliceVal.Kind() != reflect.Slice {
		return fmt.Errorf("rowsToStruct called with pointer to non-slice: %T", dest)
	}
	ptrType := sliceVal.Type().Elem()
	if ptrType.Kind() != reflect.Ptr {
		return fmt.Errorf("rowsToStruct expects element to be pointers, found %T", dest)
	}
	eltType := ptrType.Elem()
	if eltType.Kind() != reflect.Struct {
		return fmt.Errorf("rowsToStruct expects element to be pointers to structs, found %T", dest)
	}

	for rows.Next() {
		eltVal := reflect.New(eltType)
		elt := eltVal.Interface()

		err := rows.StructScan(elt)
		if err != nil {
			return err
		}
		// add to the result slice
		sliceVal.Set(reflect.Append(sliceVal, eltVal))
	}
	return nil

	// defer rows.Close()

	// v := reflect.ValueOf(dest)
	// if v.Kind() != reflect.Ptr {
	// 	return nil, errors.New("must pass a pointer, not a value, to QueryStruct destination")
	// }

	// // Create a slice to begin with
	// vtype := reflect.TypeOf(dest)
	// slice := reflect.MakeSlice(reflect.SliceOf(vtype), 1, 1)
	// for rows.Next() {
	// 	//Create new element of given type
	// 	x := reflect.New(slice.Type())
	// 	err = rows.StructScan(x.Interface())
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	x.Elem().Set(slice)
	// }

	// return slice, nil
}

// //rowsToStruct converts given sqlx.Rows to slice of given Struct
// func rowToStruct(rows *sqlx.Rows, dest interface{}) error {
// 	// make sure dst is an appropriate type
// 	dstVal := reflect.ValueOf(dest)
// 	if dstVal.Kind() != reflect.Ptr || dstVal.IsNil() {
// 		return fmt.Errorf("rowToStruct called with non-pointer destination: %T", dest)
// 	}
// 	eltType := dstVal.Type().Elem()
// 	if eltType.Kind() != reflect.Struct {
// 		return fmt.Errorf("rowToStruct expects element to be pointers to structs, found %T", dest)
// 	}

// 	for rows.Next() {
// 		// eltVal := reflect.New(eltType)
// 		// elt := eltVal.Interface()

// 		err := rows.StructScan(dest)
// 		if err != nil {
// 			return err
// 		}
// 		break
// 	}
// 	return nil
// }

// structToMap converts a struct to a map using the struct's tags.
// a list of fields can also be given with inclusive / exclusive mode
// Default is exlusive mode: All fields will be added to map except those passed in fields
func structToMap(in interface{}, fields []string, fieldsInclude bool) (RowMap, string, error) {
	out := RowMap{}
	idfield := ""

	v := reflect.ValueOf(in)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	// we only accept structs
	if v.Kind() != reflect.Struct {
		return nil, "", fmt.Errorf("structToMap only accepts structs; got %T", v)
	}

	typ := v.Type()

	// loop through all fields in struct
	for i := 0; i < v.NumField(); i++ {
		// gets us a StructField
		fi := typ.Field(i)
		finame := strings.ToLower(fi.Name)

		// skip non-exported fields
		if fi.PkgPath != "" {
			continue
		}

		// examine the tag for metadata
		tag := strings.Split(fi.Tag.Get(tagName), ",")

		// was this field marked for skipping?
		if len(tag) > 0 && tag[0] == "-" {
			continue
		}

		// check if already have idfield, otherwise try to get
		if idfield == "" {
			// check if current field is tagged as primary key 'pk'
			for j := 1; j < len(tag); j++ {
				if tag[j] == "pk" {
					if !isValidForPrimaryKey(typ.Field(i).Type) {
						return nil, "", fmt.Errorf("Field %s tagged as primary key is of invalid type; got %T", fi.Name, v.Field(i))
					}
					idfield = fi.Name
				}
			}
		}

		if len(fields) == 0 {
			// no field set to exclude/include, so all add fields
			// set key of map to value in struct field
			out[fi.Name] = v.Field(i).Interface()
		} else {
			// match StructField with passed fields to include or exclude
			for _, s := range fields {
				if fieldsInclude {
					// Inclusive mode: Only add fields that exist in []fields
					if s != "" && s == finame {
						// set key of map to value in struct field
						out[fi.Name] = v.Field(i).Interface()
					}
				} else {
					if s != "" && s == finame {
						// skip this feiels as marked in []fields to exclude
					} else {
						out[fi.Name] = v.Field(i).Interface()
					}
				}
			}
		}

	}

	if idfield == "" {
		return nil, "", fmt.Errorf("Primary key not set in struct definition")
	}
	return out, idfield, nil
}

// // structToMap converts a struct to a map using the struct's tags.
// // a list of fields can also be given with inclusive / exclusive mode
// // Default is exlusive mode: All fields will be added to map except those passed in fields
// func structToMap_OLD(in interface{}, fields []string, fieldsInclude bool, IDFieldName string, idFieldMode idFieldConvertMode) (RowMap, error) {
// 	out := RowMap{}

// 	v := reflect.ValueOf(in)
// 	if v.Kind() == reflect.Ptr {
// 		v = v.Elem()
// 	}

// 	// we only accept structs
// 	if v.Kind() != reflect.Struct {
// 		return nil, fmt.Errorf("structToMap only accepts structs; got %T", v)
// 	}

// 	typ := v.Type()
// 	IDFieldName = strings.ToLower(IDFieldName)
// 	if len(fields) == 0 {
// 		// no field set to exclude/include, so all add fields
// 		for i := 0; i < v.NumField(); i++ {
// 			// gets us a StructField
// 			fi := typ.Field(i)
// 			finame := strings.ToLower(fi.Name)

// 			//Check if it is ID field
// 			if IDFieldName == finame {
// 				//Include or Exclude ID field as per mode set
// 				if (idFieldMode == idFieldIncludeIfValue && isZero(v.Field(i))) || idFieldMode == idFieldExclude {
// 					// if convert mode is idFieldIncludeIfValue then
// 					// include ID field in map, only if it's value is set
// 					// If it has default Zero value, then exclude it
// 					continue
// 				}
// 			}

// 			// set key of map to value in struct field
// 			out[fi.Name] = v.Field(i).Interface()
// 		}
// 		return out, nil
// 	}

// 	//covert all field names to lowecase for comparison
// 	i := 0
// 	for _, s := range fields {
// 		fields[i] = strings.ToLower(s)
// 	}

// 	for i := 0; i < v.NumField(); i++ {
// 		// gets StructField
// 		fi := typ.Field(i)
// 		finame := strings.ToLower(fi.Name)

// 		//Check if it is ID field
// 		if IDFieldName == finame {
// 			//Include or Exclude ID field as per mode set
// 			if (idFieldMode == idFieldIncludeIfValue && isZero(v.Field(i))) || idFieldMode == idFieldExclude {
// 				// if convert mode is idFieldIncludeIfValue then
// 				// include ID field in map, only if it's value is set
// 				// If it has default Zero value, then exclude it
// 				continue
// 			}
// 		}

// 		for _, s := range fields {
// 			if fieldsInclude {
// 				// Inclusive mode: Only add fields that exist in []fields
// 				if s != "" && s == finame {
// 					// set key of map to value in struct field
// 					out[fi.Name] = v.Field(i).Interface()
// 				}
// 			} else {
// 				if s != "" && s == finame {
// 					// skip this feiels as marked in []fields to exclude
// 				} else {
// 					out[fi.Name] = v.Field(i).Interface()
// 				}
// 			}

// 		}
// 	}

// 	return out, nil
// }

// Check if given value has default value of that type
// e.g. for int default value  is 0
// for string default value is "" etc.
func isZero(v reflect.Value) bool {
	//fmt.Printf("isZero Kind: %v\n", v.Kind())
	switch v.Kind() {
	case reflect.Func, reflect.Map, reflect.Slice:
		return v.IsNil()
	case reflect.Array:
		z := true
		for i := 0; i < v.Len(); i++ {
			z = z && isZero(v.Index(i))
		}
		return z
	case reflect.Struct:
		z := true
		for i := 0; i < v.NumField(); i++ {
			z = z && isZero(v.Field(i))
		}
		return z
	}
	// Compare other types directly:
	z := reflect.Zero(v.Type())
	return v.Interface() == z.Interface()
}

// // Only check basic values like int string etc.
// // For other like struct, map etc it will panic
// func basicIsZero(in interface{}) bool {
// 	v := reflect.ValueOf(in)
// 	//fmt.Printf("isZero Kind: %v\n", v.Kind())
// 	z := reflect.Zero(v.Type())
// 	return v.Interface() == z.Interface()
// }

// Return value of Feild passed as string from given struct
func getFieldValue(in interface{}, field string) (interface{}, error) {
	v := reflect.ValueOf(in)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	// we only accept structs
	if v.Kind() != reflect.Struct {
		return nil, fmt.Errorf("getFieldValue only accepts structs; got %T", v)
	}

	f := reflect.Indirect(v).FieldByName(field)
	return f.Interface(), nil
}

// func dbScan(rows *sqlx.Rows) RowMap {
// 	r := RowMap{}

// 	cols, _ := rows.Columns()
// 	c := len(cols)
// 	vals := make([]interface{}, c)
// 	valPtrs := make([]interface{}, c)

// 	for i := range cols {
// 		valPtrs[i] = &vals[i]
// 	}

// 	rows.Scan(valPtrs...)

// 	for i := range cols {
// 		if val, ok := vals[i].([]byte); ok {
// 			r[cols[i]] = string(val)
// 		} else {
// 			r[cols[i]] = vals[i]
// 		}
// 	}

// 	return r
// }

// func dbScanAll(rows *sqlx.Rows) []RowMap {
// 	RowMaps := []RowMap{}

// 	cols, _ := rows.Columns()
// 	c := len(cols)
// 	vals := make([]interface{}, c)
// 	valPtrs := make([]interface{}, c)

// 	for rows.Next() {
// 		for i := range cols {
// 			valPtrs[i] = &vals[i]
// 		}

// 		rows.Scan(valPtrs...)
// 		r := RowMap{}
// 		for i := range cols {
// 			if val, ok := vals[i].([]byte); ok {
// 				r[cols[i]] = string(val)
// 			} else {
// 				r[cols[i]] = vals[i]
// 			}
// 		}
// 		RowMaps = append(RowMaps, r)
// 	}

// 	return RowMaps
// }

// // ToMap converts a struct to a map using the struct's tags.
// //
// // ToMap uses tags on struct fields to decide which fields to add to the
// // returned map.
// func ToMap(in interface{}, tag string) (map[string]interface[}, error){
//     out := make(map[string]interface{})

//     v := reflect.ValueOf(in)
//     if v.Kind() == reflect.Ptr {
//         v = v.Elem()
//     }

//     // we only accept structs
//     if v.Kind() != reflect.Struct {
//         return nil, fmt.Errorf("ToMap only accepts structs; got %T", v)
//     }

//     typ := v.Type()
//     for i := 0; i < v.NumField(); i++ {
//         // gets us a StructField
//         fi := typ.Field(i)
//         if tagv := fi.Tag.Get(tag); tagv != "" {
//             // set key of map to value in struct field
//             out[tagv] = v.Field(i).Interface()
//         }
//     }
//     return out, nil
// }

//getStruct gives First struct from passed uninitialized *[]*struct
// which have no elements
// first it check if passed interface is strictly *[]*struct
func getStructElement(dest interface{}) (interface{}, error) {

	// make sure dst is an appropriate type
	dstVal := reflect.ValueOf(dest)
	if dstVal.Kind() != reflect.Ptr || dstVal.IsNil() {
		return nil, fmt.Errorf("Invalid destination. Called with non-pointer destination: %T", dest)
	}
	sliceVal := dstVal.Elem()
	if sliceVal.Kind() != reflect.Slice {
		return nil, fmt.Errorf("Invalid destination. Called with pointer to non-slice: %T", dest)
	}
	ptrType := sliceVal.Type().Elem()
	if ptrType.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("Invalid destination. Expected element to be pointers, found %T", dest)
	}
	eltType := ptrType.Elem()
	if eltType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("Invalid destination. Expected element to be pointers to structs, found %T", dest)
	}

	eltVal := reflect.New(eltType)
	elt := eltVal.Interface()

	return elt, nil
}

// func CheckStruct(in interface{}) error {
// 	return checkInterfaceAcceptable(in, "Test", true, false, false)
// }
// func CheckSlice(in interface{}) error {
// 	return checkInterfaceAcceptable(in, "Test", false, true, false)
// }
// func CheckMap(in interface{}) error {
// 	return checkInterfaceAcceptable(in, "Test", false, false, true)
// }

//SliceToStringInt convert slice to int to comma separated string
func SliceToStringInt(a []int, sep string) string {
	if len(a) == 0 {
		return ""
	}

	b := make([]string, len(a))
	for i, v := range a {
		b[i] = strconv.Itoa(v)
	}
	return strings.Join(b, sep)
}

//SliceToStringFloat convert slice to float64 to comma separated string
func SliceToStringFloat(a []float64, sep string) string {
	if len(a) == 0 {
		return ""
	}

	b := make([]string, len(a))
	for i, v := range a {
		b[i] = strconv.FormatFloat(v, 'f', 6, 64)
	}
	return strings.Join(b, sep)
}

//strconv.FormatFloat(input_num, 'f', 6, 64)

func concat(args ...string) string {
	var b strings.Builder
	for _, s := range args {
		b.WriteString(s)
	}
	return b.String()
}
