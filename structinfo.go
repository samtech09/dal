package dal

import (
	//"bytes"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

type structInfo struct {
	Name         string
	Table        string
	PkColName    string
	PkColIndex   int
	Cols         []string
	Indexes      []int
	NoUpdateCols map[string]bool // just to hold name of columns
	//Fields     map[string]*fieldInfo
}

// type fieldInfo struct {
// 	Name     string
// 	Index    int
// 	IsPK     bool
// 	NoUpdate bool
// }

type structValues struct {
	PKColumnVal interface{}
	PkIsZero    bool
	Values      []interface{}
}

//var structInfoCache = make(map[reflect.Type]*structInfo)
var structInfoCache = make(map[string]*structInfo)
var structInfoCacheMutex sync.Mutex

//b := append([]T(nil), a...)

// getQueryCols return slice of all columns that can be used
// to query database for that struct
func (s *structInfo) getQueryCols() []string {
	cols := make([]string, len(s.Cols)+1)
	copy(cols, s.Cols)
	cols[len(s.Cols)] = s.PkColName
	quote(cols)
	return cols
}

// func (s *structInfo) getQueryCols() []string {
// 	cols := make([]string, len(s.Cols))
// 	copy(cols, s.Cols)
// 	cols = append(cols, s.PkColName)
// 	quote(cols)
// 	return cols
// }

//getData parse given struct and return Columns and corresponding values
func (s *structInfo) getData(vals *structValues, fields map[string]bool,
	fieldsInclude bool, idFieldMode idFieldInclusionMode, forUpdate bool) ([]string, []interface{}) {
	// loop through all cols and
	filteredCols := []string{}
	var filteredVals []interface{}

	if len(fields) == 0 {
		// fields to include/exclude not set
		if len(s.NoUpdateCols) == 0 || !forUpdate {
			// neither fields are set nor NoUpdateCols are set
			// so add all fields
			filteredCols = make([]string, len(s.Cols))
			copy(filteredCols, s.Cols)
			filteredVals = make([]interface{}, len(vals.Values))
			copy(filteredVals, vals.Values)

		} else {
			// NoUpdateCols set, exclude them for Update query
			for i, col := range s.Cols {
				// match StructField with Fields in NoUpdateCols
				if _, ok := s.NoUpdateCols[col]; !ok {
					filteredCols = append(filteredCols, col)
					filteredVals = append(filteredVals, vals.Values[i])
				}
			}
		}

	} else {
		// Fields are set,
		for i, col := range s.Cols {
			// match StructField with passed fields to include or exclude
			if fieldsInclude {
				if _, ok := fields[col]; ok {
					// column exist in fields, add it
					filteredCols = append(filteredCols, col)
					filteredVals = append(filteredVals, vals.Values[i])
				}
			} else {
				// Exclude Mode
				if _, ok := fields[col]; !ok {
					// column NOT exist in fields, add it
					//
					// But also check if is exist in NoUpdateCols
					if _, ok1 := s.NoUpdateCols[col]; !ok1 {
						// Column neither exist in Fields nor in NoUpdateCols
						filteredCols = append(filteredCols, col)
						filteredVals = append(filteredVals, vals.Values[i])
					}
				}
			}

			// for _, f := range fields {
			// 	if fieldsInclude {
			// 		// Inclusive mode: Only add fields that exist in []fields
			// 		// It also overwrite NoUpdateCols behaviour
			// 		if f != "" && f == col {
			// 			// set key of map to value in struct field
			// 			filteredCols = append(filteredCols, col)
			// 			filteredVals = append(filteredVals, vals.Values[i])
			// 			break
			// 		}
			// 	} else {
			// 		if f != "" && f == col {
			// 			// skip this fiels, as marked in []fields to exclude
			// 			break
			// 		} else {
			// 			// match StructField with Fields in NoUpdateCols
			// 			if _, ok := s.NoUpdateCols[col]; !ok {
			// 				filteredCols = append(filteredCols, col)
			// 				filteredVals = append(filteredVals, vals.Values[i])
			// 			}
			// 		}
			// 	}
			// }
		}
	}

	// Now add PKColumn
	switch idFieldMode {
	case idFieldInclude:
		filteredCols = append(filteredCols, s.PkColName)
		filteredVals = append(filteredVals, vals.PKColumnVal)

	case idFieldIncludeIfValue:
		if vals.PKColumnVal != nil && !vals.PkIsZero {
			filteredCols = append(filteredCols, s.PkColName)
			filteredVals = append(filteredVals, vals.PKColumnVal)
		}
	}
	quote(filteredCols)
	return filteredCols, filteredVals
}

func setPrimaryKeyValue(v *reflect.Value, val interface{}, si *structInfo) {
	if si.PkColIndex >= 0 {
		f := v.Field(si.PkColIndex)
		if f.IsValid() && f.CanSet() {
			switch f.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				f.SetInt(toIntx(val))
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				f.SetUint(toUIntx(val))
			case reflect.String:
				f.SetString(toStringx(val))
				// case reflect.Uint8:
				// 	//f.SetUint8(val.(uint8))
				// 	f.Set(reflect.ValueOf(val))
			}
		}
	}

}

func parseStruct(in interface{}) (*structInfo, *structValues, *reflect.Value, error) {
	v := reflect.ValueOf(in)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	// we only accept structs
	if v.Kind() != reflect.Struct {
		return nil, nil, nil, fmt.Errorf("parseStruct only accepts structs; got %T", v)
	}
	//fmt.Printf("parseStruct: Kind is %v\n", v.Kind())
	// fmt.Printf("parseStruct: Type is %v\n", reflect.TypeOf(in).Elem())

	//reflect structure of struct
	//si, err := _reflectStruct(reflect.TypeOf(in))
	si, err := _reflectStruct(v.Type())
	if err != nil {
		return nil, nil, nil, err
	}
	// Get Values of struct fields
	vals := _getStructValues(v, si.Indexes, si.PkColIndex)
	//fmt.Printf("Debug: NoUpdateCols = %v\n", si.NoUpdateCols)
	return si, vals, &v, nil
}

// Do not call it directly
// Instead call parseStruct
func _reflectStruct(typ reflect.Type) (*structInfo, error) {
	//func reflectStruct(in interface{}) (*structInfo, error) {
	structInfoCacheMutex.Lock()
	defer structInfoCacheMutex.Unlock()

	// var buffer bytes.Buffer
	// buffer.WriteString(typ.PkgPath())
	// buffer.WriteString(".")
	// buffer.WriteString(typ.Name())

	structName := concat(typ.PkgPath(), ".", typ.Name()) //typ.Name()
	//structName := buffer.String()
	//fmt.Printf("_reflectStruct: requested %T, name=%s\n", typ, structName)

	//TODO: Enable it after benchmark
	if sinfo, present := structInfoCache[structName]; present {
		//fmt.Printf("    _reflectStruct: %s found in cache.\n", structName)
		return sinfo, nil
	}
	//fmt.Printf("    _reflectStruct: %s\n", structName)

	v := typ
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	// we only accept structs
	if v.Kind() != reflect.Struct {
		return nil, fmt.Errorf("reflectStruct only accepts structs; got %T", v)
	}

	si := &structInfo{}
	si.Cols = []string{}
	si.Indexes = []int{}
	si.NoUpdateCols = make(map[string]bool)

	// get name of struct
	si.Name = typ.Name()
	si.PkColIndex = -1
	//si.Fields = make(map[string]*fieldInfo)

	// loop through all fields in struct
	for i := 0; i < v.NumField(); i++ {
		// gets us a StructField
		fi := v.Field(i)
		finame := strings.ToLower(fi.Name)
		noupdate := false

		// examine the tag for metadata
		tag := strings.Split(fi.Tag.Get(tagName), ",")

		// skip non-exported fields
		if fi.PkgPath != "" {
			//check if field is meta field
			if finame == "meta" {
				// meta tag contails name of table that this struct represent
				//tag := strings.Split(fi.Tag.Get(tagName), ",")
				if len(tag) > 0 {
					si.Table = tag[0]
				}
			}
			continue
		}

		// was this field marked for skipping?
		if len(tag) > 0 && tag[0] == "-" {
			continue
		}

		for j := 0; j < len(tag); j++ {
			// check if current field is tagged as primary key 'pk'
			if tag[j] == "pk" {
				// check if already have PkColumn, otherwise try to get
				if si.PkColIndex < 0 {
					if !isValidForPrimaryKey(fi.Type) {
						return nil, fmt.Errorf("Field %s tagged as primary key is of invalid type; got %T", fi.Name, v.Field(i))
					}
					si.PkColName = finame
					si.PkColIndex = i
				}

			} else if tag[j] == "noupdate" {
				//this field should be skippid for update
				noupdate = true
			} else if tag[j] != "" {
				// if it is PK column, update it's name too
				if si.PkColIndex == i {
					si.PkColName = tag[j]
				}
				finame = tag[j]
			}
			//fmt.Printf("tag: %s\n", tag[j])
		}

		// add all fields except PKColumn
		// as we already have PK column in separate field
		if si.PkColIndex != i {
			// si.Fields[finame] = &fieldInfo{
			// 	Name:  finame,
			// 	Index: i,
			// 	IsPK:  i == si.PkColIndex,
			// }
			si.Cols = append(si.Cols, finame)
			if noupdate {
				si.NoUpdateCols[finame] = true
			}
			si.Indexes = append(si.Indexes, i)
		}

	}

	structInfoCache[structName] = si
	return si, nil
}

// Do not call it directly
// Instead call parseStruct
func _getStructValues(v reflect.Value, indexes []int, pkColIndex int) *structValues {
	vals := structValues{}
	vals.Values = make([]interface{}, len(indexes))
	for i := 0; i < len(indexes); i++ {
		//vals.Values = append(vals.Values, v.Field(indexes[i]).Interface())
		vals.Values[i] = v.Field(indexes[i]).Interface()
	}

	vals.PKColumnVal = nil
	if pkColIndex >= 0 {
		//fmt.Printf("PkIndex: %d\n", pkColIndex)
		vals.PKColumnVal = v.Field(pkColIndex).Interface()
		//fmt.Printf("Pkval: %v\n", vals.PKColumnVal)
		vals.PkIsZero = isZero(v.Field(pkColIndex))
		//vals.PkIsZero = basicIsZero(vals.PKColumnVal)

	}
	return &vals
}
func quote(cols []string) {
	var b strings.Builder
	for i, s := range cols {
		b.WriteString(dbconfig.quote)
		b.WriteString(s)
		b.WriteString(dbconfig.quote)
		cols[i] = b.String()
		b.Reset()
	}
}
