package dal

import (
	"reflect"
	"strconv"
	"strings"
)

//SQLBuilder allow to dynamically build SQL to query database-tables
type SQLBuilder struct {
	selectsql []string
	fromsql   []string
	wheresql  []string
	groupBy   []string
	orderBy   []string
	insql     []string
	limitRows int
	rawsql    string
	tables    map[string]string
}

//NewSQLBuilder returns new instance of SQLBuilder
func NewSQLBuilder() *SQLBuilder {
	s := SQLBuilder{}
	s.tables = make(map[string]string)
	s.limitRows = 0
	return &s
}

//Select specifies the SELECT, INSERT, UPDATE or DELETE clause
func (s *SQLBuilder) Select(fields string) *SQLBuilder {
	s.selectsql = append(s.selectsql, strings.Trim(fields, " "))
	return s
}

//From specifies the FROM clause of sql, it appends FROM keyword itself.
func (s *SQLBuilder) From(fromclause string) *SQLBuilder {
	s.fromsql = append(s.fromsql, strings.Trim(fromclause, " "))
	return s
}

//Where specifies the WHERE clause of sql, it appends WHERE keyword itself.
func (s *SQLBuilder) Where(whereclause string) *SQLBuilder {
	s.wheresql = append(s.wheresql, strings.Trim(whereclause, " "))
	return s
}

//GroupBy specifies the GROUP BY clause of sql, it appends GROUP BY keyword itself.
func (s *SQLBuilder) GroupBy(groupbyclause string) *SQLBuilder {
	s.groupBy = append(s.groupBy, strings.Trim(groupbyclause, " "))
	return s
}

//OrderBy specifies the ORDER BY clause of sql, it appends ORDER BY keyword itself.
func (s *SQLBuilder) OrderBy(fieldname string, descending bool) *SQLBuilder {
	if descending {
		s.orderBy = append(s.orderBy, strings.Trim(fieldname, " ")+" desc")
	} else {
		s.orderBy = append(s.orderBy, strings.Trim(fieldname, " ")+" asc")
	}
	return s
}

//Table adds table that is being used in sql, also allow to replace alias withtable name
func (s *SQLBuilder) Table(modelstruct interface{}, alias string) *SQLBuilder {
	tblname := getDbModelTable(modelstruct)
	if tblname != "" {
		s.tables[alias] = tblname
	}
	return s
}

//Limit limits number of resultant rows
func (s *SQLBuilder) Limit(numRows int) *SQLBuilder {
	s.limitRows = numRows
	return s
}

//WhereIntIN returns IN clause for given field and array (slice) for selected DBMS format
func (s *SQLBuilder) WhereIntIN(fieldName string, in []int) *SQLBuilder {
	csv := SliceToStringInt(in, ",")
	//fmt.Printf("New: %s\n", csv)

	if dbconfig.useArrayForIN {
		s.insql = append(s.insql, fieldName+"=ANY('{"+csv+"}'::integer[])")
	} else {
		s.insql = append(s.insql, fieldName+" IN ("+csv+")")
	}
	return s
}

//WhereFloatIN returns IN clause for given field and array (slice) for selected DBMS format
func (s *SQLBuilder) WhereFloatIN(fieldName string, in []float64) *SQLBuilder {
	//csv := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(in)), ","), "[]")
	csv := SliceToStringFloat(in, ",")
	if dbconfig.useArrayForIN {
		s.insql = append(s.insql, fieldName+"=ANY('{"+csv+"}'::numeric[])")
	} else {
		s.insql = append(s.insql, fieldName+" IN ("+csv+")")
	}
	return s
}

//WhereStrIN returns IN clause for given field and array (slice) for selected DBMS format
func (s *SQLBuilder) WhereStrIN(fieldName string, in []string) *SQLBuilder {
	if dbconfig.useArrayForIN {
		csv := strings.Join(in, ",")
		s.insql = append(s.insql, fieldName+"=ANY('{"+csv+"}'::text[])")
	} else {
		csv := strings.Join(in, "','")
		s.insql = append(s.insql, fieldName+" IN ('"+csv+"')")
	}
	return s
}

//Raw specifies the raw sql where only table and alias replacement to be made
func (s *SQLBuilder) Raw(raw string) *SQLBuilder {
	s.rawsql = strings.Trim(raw, " ")
	return s
}

// Build finally build return SQL
func (s *SQLBuilder) Build() string {
	//sql := ""
	//wheresql := ""
	var sql strings.Builder
	haswheresql := false
	comma := []byte(", ")
	space := []byte(" ")
	and := []byte(" and ")

	if len(s.selectsql) > 0 {
		sql.WriteString("select ")
		if s.limitRows > 0 && dbconfig.useTopToLimitRows {
			sql.WriteString("top " + strconv.Itoa(s.limitRows) + " ")
		}

		for i, str := range s.selectsql {
			if i > 0 {
				sql.Write(comma)
			}
			sql.WriteString(str)
		}
		sql.Write(space)
	}

	if len(s.fromsql) > 0 {
		sql.WriteString("from ")
		for i, str := range s.fromsql {
			if i > 0 {
				sql.Write(comma)
			}
			sql.WriteString(str)
		}
		sql.Write(space)
	}

	if len(s.wheresql) > 0 {
		haswheresql = true
		sql.WriteString("where ")
		for i, str := range s.wheresql {
			if i > 0 {
				sql.Write(and)
			}
			sql.WriteString(str)
		}
		sql.Write(space)
	}

	if len(s.insql) > 0 {
		if !haswheresql {
			sql.WriteString("where ")
		} else {
			sql.Write(and)
		}
		for i, str := range s.insql {
			if i > 0 {
				sql.Write(and)
			}
			sql.WriteString(str)
		}
		sql.Write(space)
	}

	if len(s.groupBy) > 0 {
		sql.WriteString("group by ")
		for i, str := range s.groupBy {
			if i > 0 {
				sql.Write(comma)
			}
			sql.WriteString(str)
		}
		sql.Write(space)
	}

	if len(s.orderBy) > 0 {
		sql.WriteString("order by ")
		for i, str := range s.orderBy {
			if i > 0 {
				sql.Write(comma)
			}
			sql.WriteString(str)
		}
		sql.Write(space)
	}

	if s.limitRows > 0 && !dbconfig.useTopToLimitRows {
		sql.WriteString("limit " + strconv.Itoa(s.limitRows) + " ")
		// sql.WriteString(strconv.Itoa(s.limitRows))
		// sql.WriteString(" ")
	}

	sqlstr := prepareSQL(sql.String(), s.tables)
	return strings.Trim(sqlstr, " ")
}

//RawBuild finally replace alias with tables and return RawSQL
func (s *SQLBuilder) RawBuild() string {
	sql := prepareSQL(s.rawsql, s.tables)
	return strings.Trim(sql, " ")
}

//IntIN returns IN clause for given field and array (slice) for selected DBMS format
func (s *SQLBuilder) IntIN(fieldName string, in []int) string {
	//csv := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(in)), ","), "[]")
	csv := SliceToStringInt(in, ",")

	if dbconfig.useArrayForIN {
		return fieldName + "=ANY('{" + csv + "}'::integer[])"
	}
	return fieldName + " IN (" + csv + ")"
}

//FloatIN returns IN clause for given field and array (slice) for selected DBMS format
func (s *SQLBuilder) FloatIN(fieldName string, in []float64) string {
	//csv := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(in)), ","), "[]")
	csv := SliceToStringFloat(in, ",")

	if dbconfig.useArrayForIN {
		return fieldName + "=ANY('{" + csv + "}'::numeric[])"
	}
	return fieldName + " IN (" + csv + ")"
}

//StrIN returns IN clause for given field and array (slice) for selected DBMS format
func (s *SQLBuilder) StrIN(fieldName string, in []string) string {
	if dbconfig.useArrayForIN {
		//csv := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(in)), ","), "[]")
		csv := strings.Join(in, ",")
		return fieldName + "=ANY('{" + csv + "}'::text[])"
	}
	//csv := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(in)), "','"), "[]")
	csv := strings.Join(in, "','")
	return fieldName + " IN ('" + csv + "')"
}

//***************************************
//
//            PRIVATE FUNCTIONS
//
//***************************************

// //prepareSQL_regex replaces placeholders with table names
// func prepareSQL_regex(sql string, tables map[string]string) string {
// 	//regex is too costly, doing too many allocations (in benchmark with two tables taking 82 allocations)
// 	// NOT USING THIS FUNCTION
// 	//
// 	//

// 	// add space at beginning to allow regex to replace keywords that are at the very beginning
// 	s := " " + sql
// 	//fmt.Println(s)
// 	for k, v := range tables {
// 		//key is alias that is to be replaced
// 		// value is table name
// 		rx, err := regexp.Compile("([^A-Za-z0-9])" + k + "([^A-Za-z0-9])")
// 		if err != nil {
// 			log.Fatal(err)
// 		}
// 		//s = rx.ReplaceAllString(s, fmt.Sprintf("${1}%s$2", v))
// 		s = rx.ReplaceAllString(s, "${1}" + v + "$2")
// 		// fmt.Printf("Key: %s, Value: %s\n", k, v)
// 		// fmt.Println(s)
// 		//s = strings.Replace(s, " "+k+".", v, -1)
// 	}
// 	return strings.Trim(s, " ")
// }

//PrepareSQL replaces placeholders with table names
func prepareSQL(sql string, tables map[string]string) string {
	// add space at beginning to allow regex to replace keywords that are at the very beginning
	s := " " + sql
	//fmt.Println(s)
	for k, v := range tables {
		// //key is alias that is to be replaced
		// // value is table name
		s = strings.Replace(s, " "+k+".", " "+v+".", -1)
		s = strings.Replace(s, "="+k+".", "="+v+".", -1)
		s = strings.Replace(s, "<"+k+".", "<"+v+".", -1)
		s = strings.Replace(s, ">"+k+".", ">"+v+".", -1)
		s = strings.Replace(s, ","+k+".", ","+v+".", -1)
		s = strings.Replace(s, ","+k+",", ","+v+",", -1)
		s = strings.Replace(s, "("+k+".", "("+v+".", -1)

		s = strings.Replace(s, " "+k+",", " "+v+",", -1)
		s = strings.Replace(s, " "+k+" ", " "+v+" ", -1)
		s = strings.Replace(s, ","+k+" ", ","+v+" ", -1)
		s = strings.Replace(s, " "+k+")", " "+v+")", -1)
	}
	return strings.Trim(s, " ")
}

//GetDbModelTable returns name of associated table with given DB-model
func getDbModelTable(in interface{}) string {
	v := reflect.ValueOf(in)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	// we only accept structs
	if v.Kind() != reflect.Struct {
		//return fmt.Errorf("getDbModelTable only accepts structs; got %T", v)
		return ""
	}

	sinfo, err := _reflectStruct(v.Type())
	if err != nil {
		return ""
	}
	return sinfo.Table

	// //strct must be pointer to struct
	// typ := reflect.ValueOf(strct).Elem().Type()
	// //tmp := strings.Split(typ.PkgPath(), fmt.Sprintf("%c", os.PathSeparator))
	// //pkg := tmp[len(tmp)-1]

	// //generate fully qualified name of struct like library.struct
	// structName := typ.PkgPath() + "." + typ.Name()

	// tableNameCacheMutex.Lock()
	// defer tableNameCacheMutex.Unlock()

	// // check name already exist in cache
	// if tblName, present := tableNameCache[structName]; present {
	// 	return tblName
	// }

	// field, ok := reflect.TypeOf(strct).Elem().FieldByName("meta")
	// if !ok {
	// 	return ""
	// }
	// tag := field.Tag.Get("dal")
	// tableNameCache[structName] = tag
	// return tag
}
