package dal

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

//TODO: InsertGraph pass
//			a. parent struct, chield map[string]struct
//				i.  It will first insert parent, get new ID
//				ii. Fill new ID in FK field of every child then insert.
//				iii. There should be some mechanism to identify correct FK in childs to set new ID

// InsertStruct inserts data into table using Struct.
func (p *Dbal) InsertStruct(rowStruct interface{}, useTrans bool) error {
	// Allowed types to pass are
	//  a. *struct
	//  b. []*struct
	//  c. map[string]*struct

	//fmt.Printf("Type of passed interface is: %v\n", reflect.TypeOf(in))
	baseType := reflect.TypeOf(rowStruct)
	err := checkInterfaceAcceptable(rowStruct, "Insert", true, true, true)
	if err != nil {
		return err
	}

	// Got supported type
	switch baseType.Kind() {
	case reflect.Ptr:
		// Process *Struct, pass rowStruct
		if useTrans {
			return insertStructTx(p, rowStruct, false)
		}
		return insertStruct(p, rowStruct, false)

	case reflect.Slice:
		// Process []*struct
		if useTrans {
			return insertStructTx(p, rowStruct, true)
		}
		return insertStruct(p, rowStruct, true)

	case reflect.Map:
		// Process map[string]*struct
		// Process []*struct
		if useTrans {
			return insertStructMapTx(p, rowStruct)
		}
		return insertStructMap(p, rowStruct)

	default:
		// unsupported
	}

	return nil
}

// UpdateStruct updates data into table using struct.
// fieldCSV: list of fields to be excluded / included for update.
// fieldsInclude: If false fields will be excluded, if true then struct tags 'noupdate' will be ignored and only fields given in fieldsCSV will be updated.
func (p *Dbal) UpdateStruct(rowStruct interface{}, fieldsCSV string, fieldsInclude bool, useTrans bool) (int64, error) {
	baseType := reflect.TypeOf(rowStruct)
	err := checkInterfaceAcceptable(rowStruct, "Update", true, true, false)
	if err != nil {
		return 0, err
	}

	// Got supported type

	// Convert fieldCSV to map[string]bool
	fields := make(map[string]bool)
	if fieldsCSV != "" {
		flds := strings.Split(fieldsCSV, ",")
		for _, s := range flds {
			fields[s] = true
		}
	}

	switch baseType.Kind() {
	case reflect.Ptr:
		// Process *Struct, pass rowStruct
		if useTrans {
			return updateStructTx(p, rowStruct, false, fields, fieldsInclude)
		}
		return updateStruct(p, rowStruct, false, fields, fieldsInclude)

	case reflect.Slice:
		// Process []*struct
		if useTrans {
			return updateStructTx(p, rowStruct, true, fields, fieldsInclude)
		}
		return updateStruct(p, rowStruct, true, fields, fieldsInclude)

	default:
		// unsupported
	}

	return 0, nil

}

// UpdateStructMap updates data into table using map[string]*struct.
func (p *Dbal) UpdateStructMap(structMap interface{}, useTrans bool) (int64, error) {
	//baseType := reflect.TypeOf(structMap)
	err := checkInterfaceAcceptable(structMap, "UpdateMap", false, false, true)
	if err != nil {
		return 0, err
	}

	// Got supported type
	if useTrans {
		return updateStructMapTx(p, structMap)
	}
	return updateStructMap(p, structMap)
}

// ExecProcStruct executes given Stored procedure, fills passed slice of struct.
// dest: Slice of pointers to struct.
func (p *Dbal) ExecProcStruct(proc string, dest interface{}, args ...interface{}) error {
	code := fmt.Sprintf(dbconfig.sqlProc, proc)
	rows, err := query(p, code, args...)
	if err != nil {
		return err
	}
	// make sure we always close rows
	defer rows.Close()

	return rowsToStruct(rows, dest)
}

// QueryStructBySQL executes given query, fills passed slice of struct.
// dest: Slice of pointers to struct.
func (p *Dbal) QueryStructBySQL(code string, dest interface{}, args ...interface{}) error {
	rows, err := query(p, code, args...)
	if err != nil {
		return err
	}
	// make sure we always close rows
	defer rows.Close()

	return rowsToStruct(rows, dest)
}

//QueryStructByID query corresponsing table of given struct by Primary Key
//Dest should be *struct as it will return single record
func (p *Dbal) QueryStructByID(dest interface{}, id interface{}) error {
	err := checkInterfaceAcceptable(dest, "QueryStructByID", true, false, false)
	if err != nil {
		return err
	}

	si, _, _, err := parseStruct(dest)
	if err != nil {
		return fmt.Errorf("Error parsing struct.\n%v", err)
	}

	// prepare SQL Statement
	var code string
	var params []interface{}
	cols := si.getQueryCols()
	params = append(params, id)
	if dbconfig.paramNumeric {
		code = fmt.Sprintf(dbconfig.sqlQueryByID, strings.Join(cols, ", "), si.Table, si.PkColName, dbconfig.param+"1")
	} else {
		code = fmt.Sprintf(dbconfig.sqlQueryByID, strings.Join(cols, ", "), si.Table, si.PkColName, dbconfig.param)
	}

	//fmt.Printf("SQL: %s\n", code)
	rows, err := query(p, code, params...)
	if err != nil {
		return err
	}

	// make sure we always close rows
	defer rows.Close()

	rows.Next()

	err = rows.StructScan(dest)
	if err != nil {
		return err
	}

	return nil
	//return rowToStruct(rows, dest)
}

//QueryStruct query corresponsing table of given struct by Primary Key
//Dest should be []'struct as it will return single record
func (p *Dbal) QueryStruct(dest interface{}, condition string, args ...interface{}) error {
	var (
		firstStruct interface{}
		code        string
	)

	firstStruct, err := getStructElement(dest)
	if err != nil {
		return err
	}

	si, _, _, err := parseStruct(firstStruct)
	if err != nil {
		return fmt.Errorf("Error parsing struct.\n%v", err)
	}

	// Get list of colums for query
	cols := si.getQueryCols()

	// prepare SQL Statement
	code = fmt.Sprintf(dbconfig.sqlQuery, strings.Join(cols, ", "), si.Table, condition)

	rows, err := query(p, code, args...)
	if err != nil {
		return err
	}
	// make sure we always close rows
	defer rows.Close()

	return rowsToStruct(rows, dest)
}

// FirstStruct query corresponding table of given struct,
// fill given struct with first row of result.
// It will be good to pass sql that only select single row
// instead of loading thousands of rows and then discarding them.
// dest: Pointer to struct.
func (p *Dbal) FirstStruct(dest interface{}, condition string, args ...interface{}) error {
	si, _, _, err := parseStruct(dest)
	if err != nil {
		return fmt.Errorf("Error parsing struct.\n%v", err)
	}

	// Get list of colums for query
	cols := si.getQueryCols()

	// prepare SQL Statement
	code := fmt.Sprintf(dbconfig.sqlQueryFirst, strings.Join(cols, ", "), si.Table, condition)

	rows, err := query(p, code, args...)
	if err != nil {
		return err
	}
	// make sure we always close rows
	defer rows.Close()

	rows.Next()

	err = rows.StructScan(dest)
	if err != nil {
		return err
	}
	return nil
}

// CountStruct query corresponding table of given struct and count number of records for given condition
// dest: Pointer to struct.
func (p *Dbal) CountStruct(dest interface{}, condition string, args ...interface{}) (int64, error) {
	si, _, _, err := parseStruct(dest)
	if err != nil {
		return 0, fmt.Errorf("Error parsing struct.\n%v", err)
	}

	// prepare SQL Statement
	code := fmt.Sprintf(dbconfig.sqlCount, si.PkColName, si.Table, condition)

	rows, err := query(p, code, args...)
	if err != nil {
		return 0, err
	}
	// make sure we always close rows
	defer rows.Close()

	rows.Next()

	var count int64
	if err = rows.Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

//DeleteStruct delete row from corresponding table of given struct for given ID field
//Dest should be *struct
func (p *Dbal) DeleteStruct(dest interface{}, id interface{}) (rowsAffected int64, err error) {
	err = checkInterfaceAcceptable(dest, "DeleteStruct", true, false, false)
	if err != nil {
		return 0, err
	}

	si, _, _, err := parseStruct(dest)
	if err != nil {
		return 0, fmt.Errorf("Error parsing struct.\n%v", err)
	}

	// prepare SQL Statement
	var code string
	var params []interface{}
	params = append(params, id)
	if dbconfig.paramNumeric {
		code = fmt.Sprintf(dbconfig.sqlDelete, si.Table, si.PkColName+"="+dbconfig.param+"1")
	} else {
		code = fmt.Sprintf(dbconfig.sqlDelete, si.Table, si.PkColName+"="+dbconfig.param)
	}

	res, err := p.Db.(DbWrapper).Exec(code, params...)
	if err != nil {
		return 0, err
	}
	rowsAffected, err = res.RowsAffected()
	return
}

// ************************
//
//    PRIVATE FUNCTIONS
//
// ************************

func insertStruct(p *Dbal, in interface{}, isSlice bool) error {
	/*
		1. prepare list of columns
		2. prepare parameter string
		3. Prepare statement
		4. pass values as arg...
	*/

	var (
		firstStruct interface{}
		retid       interface{}
		structSlice reflect.Value
		//params      []string
		code string
	)

	if isSlice {
		structSlice = reflect.ValueOf(in)
		for i := 0; i < structSlice.Len(); i++ {
			firstStruct = structSlice.Index(i).Interface()
			break
		}
	} else {
		firstStruct = in
	}
	//_ = firstStruct
	//fmt.Printf("Debug: firstStruct: %v\n", firstStruct)

	//si *structInfo, vals *structValues
	si, vals, v, err := parseStruct(firstStruct)
	if err != nil {
		return fmt.Errorf("Error parsing struct.\n%v", err)
	}

	//fmt.Printf("Debug: si:\n %v\n", si)
	//fmt.Printf("Debug: vals:\n %v\n", vals)

	// Get columns and their values
	fields := make(map[string]bool)
	cols, args := si.getData(vals, fields, false, idFieldIncludeIfValue, false)
	//fmt.Printf("\tDebug: cols: %v\n", cols)

	// prepare parameter string and SQL Statement
	//params = make([]string, len(cols))
	var b strings.Builder
	if dbconfig.paramNumeric {
		for i := 1; i <= len(cols); i++ {
			//params = append(params, dbconfig.param+strconv.Itoa(i))
			//params[i-1] = Concat(dbconfig.param, strconv.Itoa(i))
			b.WriteString(dbconfig.param)
			b.WriteString(strconv.Itoa(i))
			b.WriteString(",")
		}
		params := b.String()
		params = string(params[0 : len(params)-1])
		code = fmt.Sprintf(dbconfig.sqlInsert, si.Table, strings.Join(cols, ", "), params, si.PkColName)
	} else {
		for i := 1; i <= len(cols); i++ {
			//params = append(params, dbconfig.param)
			//params[i-1] = dbconfig.param
			b.WriteString(dbconfig.param)
			b.WriteString(",")
		}
		params := b.String()
		params = string(params[0 : len(params)-1])
		code = fmt.Sprintf(dbconfig.sqlInsert, si.Table, strings.Join(cols, ", "), params)
	}

	//fmt.Printf("Debug: Code: %s\n", code)

	if isSlice {
		// create prepared statement
		stmt, err := p.Db.(DbWrapper).Preparex(code)
		if err != nil {
			return err
		}

		// now execute statement and insert data
		for i := 0; i < structSlice.Len(); i++ {
			s := structSlice.Index(i).Interface()
			// get struct values for all columns
			si, vals, v, err := parseStruct(s)
			// Get args to pass
			_, args := si.getData(vals, fields, false, idFieldIncludeIfValue, false)

			if dbconfig.useReturningID {
				err = stmt.QueryRow(args...).Scan(&retid)
				if err != nil {
					return err
				}
			} else {
				res, err := stmt.Exec(args...)
				if err != nil {
					return err
				}
				retid, _ = res.LastInsertId()
			}

			setPrimaryKeyValue(v, retid, si)
		}
	} else {
		// there is single struct
		if dbconfig.useReturningID {
			err = p.Db.(DbWrapper).QueryRowx(code, args...).Scan(&retid)
			if err != nil {
				return err
			}
		} else {
			//res, err := p.rawDb.Exec(code, args...)
			res, err := p.Db.(DbWrapper).Exec(code, args...)
			if err != nil {
				return err
			}
			retid, _ = res.LastInsertId()
		}
		setPrimaryKeyValue(v, retid, si)
	}

	return nil
}

func insertStructTx(p *Dbal, in interface{}, isSlice bool) error {
	/*
		1. prepare list of columns
		2. prepare parameter string
		3. Prepare statement
		4. pass values as arg...
	*/

	var (
		firstStruct interface{}
		retid       interface{}
		structSlice reflect.Value
		//params      []string
		code string
	)

	if isSlice {
		structSlice = reflect.ValueOf(in)
		for i := 0; i < structSlice.Len(); i++ {
			firstStruct = structSlice.Index(i).Interface()
			break
		}
	} else {
		firstStruct = in
	}
	//_ = firstStruct
	//fmt.Printf("Debug: firstStruct: %v\n", firstStruct)

	//si *structInfo, vals *structValues
	si, vals, v, err := parseStruct(firstStruct)
	if err != nil {
		return fmt.Errorf("Error parsing struct.\n%v", err)
	}

	//fmt.Printf("Debug: si:\n %v\n", si)
	//fmt.Printf("Debug: vals:\n %v\n", vals)

	// Get columns and their values
	fields := make(map[string]bool)
	cols, args := si.getData(vals, fields, false, idFieldIncludeIfValue, false)
	//fmt.Printf("\tDebug: cols: %v\n", cols)

	// prepare parameter string and SQL Statement
	//params = make([]string, len(cols))
	var b strings.Builder
	if dbconfig.paramNumeric {
		for i := 1; i <= len(cols); i++ {
			//params = append(params, dbconfig.param+strconv.Itoa(i))
			//params[i-1] = dbconfig.param + strconv.Itoa(i)
			b.WriteString(dbconfig.param)
			b.WriteString(strconv.Itoa(i))
			b.WriteString(",")
		}
		params := b.String()
		params = string(params[0 : len(params)-1])
		code = fmt.Sprintf(dbconfig.sqlInsert, si.Table, strings.Join(cols, ", "), params, si.PkColName)
	} else {
		for i := 1; i <= len(cols); i++ {
			//params = append(params, dbconfig.param)
			//params[i-1] = dbconfig.param
			b.WriteString(dbconfig.param)
			b.WriteString(",")
		}
		params := b.String()
		params = string(params[0 : len(params)-1])
		code = fmt.Sprintf(dbconfig.sqlInsert, si.Table, strings.Join(cols, ", "), params)
	}

	if isSlice {
		// begin transaction
		tx, err := p.rawDb.Beginx()
		if err != nil {
			return err
		}

		// create prepared statement
		stmt, err := tx.Prepare(code)
		if err != nil {
			tx.Rollback()
			return err
		}

		// now execute statement and insert data
		for i := 0; i < structSlice.Len(); i++ {
			s := structSlice.Index(i).Interface()
			// get struct values for all columns
			si, vals, v, err := parseStruct(s)
			// Get args to pass
			_, args := si.getData(vals, fields, false, idFieldIncludeIfValue, false)

			if dbconfig.useReturningID {
				err = stmt.QueryRow(args...).Scan(&retid)
				if err != nil {
					tx.Rollback()
					return err
				}
			} else {
				res, err := stmt.Exec(args...)
				if err != nil {
					tx.Rollback()
					return err
				}
				retid, _ = res.LastInsertId()
			}

			setPrimaryKeyValue(v, retid, si)
		}
		err = tx.Commit()
		if err != nil {
			return err
		}
	} else {
		// there is single struct
		if dbconfig.useReturningID {
			err = p.Db.(DbWrapper).QueryRowx(code, args...).Scan(&retid)
			if err != nil {
				return err
			}
		} else {
			//res, err := p.rawDb.Exec(code, args...)
			res, err := p.Db.(DbWrapper).Exec(code, args...)
			if err != nil {
				return err
			}
			retid, _ = res.LastInsertId()
		}
		setPrimaryKeyValue(v, retid, si)
	}

	return nil
}

func insertStructMap(p *Dbal, in interface{}) error {
	/*
		1. prepare list of columns
		2. prepare parameter string
		3. Prepare statement
		4. pass values as arg...
	*/

	var (
		retid     interface{}
		structMap reflect.Value
		//params    []string
		code string
	)

	structMap = reflect.ValueOf(in)
	fields := make(map[string]bool)

	// now execute statement and insert data
	for _, key := range structMap.MapKeys() {
		//si *structInfo, vals *structValues
		s := structMap.MapIndex(key).Interface()
		si, vals, v, err := parseStruct(s)
		if err != nil {
			//tx.Rollback()
			return fmt.Errorf("Error parsing struct.\n%v", err)
		}

		// Get columns and their values
		cols, args := si.getData(vals, fields, false, idFieldIncludeIfValue, false)
		//fmt.Printf("\tDebug: cols: %v\n", cols)

		// prepare parameter string and SQL Statement
		//params = make([]string, len(cols))
		var b strings.Builder
		if dbconfig.paramNumeric {
			for i := 1; i <= len(cols); i++ {
				//params = append(params, dbconfig.param+strconv.Itoa(i))
				//params[i-1] = dbconfig.param + strconv.Itoa(i)
				b.WriteString(dbconfig.param)
				b.WriteString(strconv.Itoa(i))
				b.WriteString(",")
			}
			params := b.String()
			params = string(params[0 : len(params)-1])
			code = fmt.Sprintf(dbconfig.sqlInsert, si.Table, strings.Join(cols, ", "), params, si.PkColName)
		} else {
			for i := 1; i <= len(cols); i++ {
				//params = append(params, dbconfig.param)
				//params[i-1] = dbconfig.param
				b.WriteString(dbconfig.param)
				b.WriteString(",")
			}
			params := b.String()
			params = string(params[0 : len(params)-1])
			code = fmt.Sprintf(dbconfig.sqlInsert, si.Table, strings.Join(cols, ", "), params)
		}

		if dbconfig.useReturningID {
			err = p.Db.(DbWrapper).QueryRow(code, args...).Scan(&retid)
			if err != nil {
				return err
			}
		} else {
			res, err := p.Db.(DbWrapper).Exec(code, args...)
			if err != nil {
				return err
			}
			retid, _ = res.LastInsertId()
		}

		setPrimaryKeyValue(v, retid, si)
	}

	return nil
}

func insertStructMapTx(p *Dbal, in interface{}) error {
	/*
		1. prepare list of columns
		2. prepare parameter string
		3. Prepare statement
		4. pass values as arg...
	*/

	var (
		retid     interface{}
		structMap reflect.Value
		//params    []string
		code string
	)

	structMap = reflect.ValueOf(in)
	fields := make(map[string]bool)

	// begin transaction
	tx, err := p.rawDb.Beginx()
	if err != nil {
		return err
	}

	// now execute statement and insert data
	for _, key := range structMap.MapKeys() {
		//si *structInfo, vals *structValues
		s := structMap.MapIndex(key).Interface()
		si, vals, v, err := parseStruct(s)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("Error parsing struct.\n%v", err)
		}

		// Get columns and their values
		cols, args := si.getData(vals, fields, false, idFieldIncludeIfValue, false)
		//fmt.Printf("\tDebug: cols: %v\n", cols)

		// prepare parameter string and SQL Statement
		//params = make([]string, len(cols))
		var b strings.Builder
		if dbconfig.paramNumeric {
			for i := 1; i <= len(cols); i++ {
				//params = append(params, dbconfig.param+strconv.Itoa(i))
				//params[i-1] = dbconfig.param + strconv.Itoa(i)
				b.WriteString(dbconfig.param)
				b.WriteString(strconv.Itoa(i))
				b.WriteString(",")
			}
			params := b.String()
			params = string(params[0 : len(params)-1])
			code = fmt.Sprintf(dbconfig.sqlInsert, si.Table, strings.Join(cols, ", "), params, si.PkColName)
		} else {
			for i := 1; i <= len(cols); i++ {
				// = append(params, dbconfig.param)
				//params[i-1] = dbconfig.param
				b.WriteString(dbconfig.param)
				b.WriteString(",")
			}
			params := b.String()
			params = string(params[0 : len(params)-1])
			code = fmt.Sprintf(dbconfig.sqlInsert, si.Table, strings.Join(cols, ", "), params)
		}

		if dbconfig.useReturningID {
			err = p.Db.(DbWrapper).QueryRow(code, args...).Scan(&retid)
			if err != nil {
				tx.Rollback()
				return err
			}
		} else {
			res, err := p.Db.(DbWrapper).Exec(code, args...)
			if err != nil {
				tx.Rollback()
				return err
			}
			retid, _ = res.LastInsertId()
		}

		setPrimaryKeyValue(v, retid, si)
	}
	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func updateStruct(p *Dbal, in interface{}, isSlice bool, fields map[string]bool, fieldsInclude bool) (int64, error) {
	/*
		1. prepare list of columns
		2. prepare parameter string
		3. Prepare statement
		4. pass values as arg...
	*/

	var (
		firstStruct       interface{}
		rowsAffected      int64
		totalRowsAffected int64
		structSlice       reflect.Value
		//params            []string
		code      string
		condition string
	)

	if isSlice {
		structSlice = reflect.ValueOf(in)
		for i := 0; i < structSlice.Len(); i++ {
			firstStruct = structSlice.Index(i).Interface()
			break
		}
	} else {
		firstStruct = in
	}
	//_ = firstStruct
	//fmt.Printf("Debug: firstStruct: %v\n", firstStruct)

	si, vals, _, err := parseStruct(firstStruct)
	if err != nil {
		return 0, fmt.Errorf("Error parsing struct.\n%v", err)
	}

	//fmt.Printf("Debug: si:\n %v\n", si)
	//fmt.Printf("Debug: vals:\n %v\n", vals)

	// Get columns and their values
	cols, args := si.getData(vals, fields, fieldsInclude, idFieldExclude, true)
	// append PKColumn Value to args, as used later in Where clause
	args = append(args, vals.PKColumnVal)
	//fmt.Printf("Debug: cols: %v\n", cols)

	// prepare parameter string and SQL statement
	//params = make([]string, len(cols))
	var b strings.Builder
	if dbconfig.paramNumeric {
		i := 1
		for i = 1; i <= len(cols); i++ {
			//params = append(params, fmt.Sprintf("%s = "+dbconfig.param+strconv.Itoa(i), cols[i-1]))
			//params[i-1] = fmt.Sprintf("%s = "+dbconfig.param+strconv.Itoa(i), cols[i-1])

			b.WriteString(cols[i-1])
			b.WriteString(" = ")
			b.WriteString(dbconfig.param)
			b.WriteString(strconv.Itoa(i))
			b.WriteString(",")
		}
		//condition = si.PkColName + " = " + dbconfig.param + strconv.Itoa(i)
		condition = concat(si.PkColName, " = ", dbconfig.param, strconv.Itoa(i))

	} else {
		for i := 1; i <= len(cols); i++ {
			//params = append(params, fmt.Sprintf("%s = "+dbconfig.param, cols[i-1]))
			//params[i-1] = fmt.Sprintf("%s = "+dbconfig.param, cols[i-1])

			b.WriteString(cols[i-1])
			b.WriteString(" = ")
			b.WriteString(dbconfig.param)
			b.WriteString(",")
		}
		//condition = si.PkColName + " = " + dbconfig.param
		condition = concat(si.PkColName, " = ", dbconfig.param)
	}

	params := b.String()
	params = string(params[0 : len(params)-1])
	code = fmt.Sprintf(dbconfig.sqlUpdate, si.Table, params, condition)
	//fmt.Printf("Debug: Code = %s\n\n", code)

	if isSlice {
		// create prepared statement
		stmt, err := p.Db.(DbWrapper).Preparex(code)
		if err != nil {
			return 0, err
		}

		// now execute statement and insert data
		for i := 0; i < structSlice.Len(); i++ {
			s := structSlice.Index(i).Interface()
			// get struct values for all columns
			si, vals, _, err := parseStruct(s)
			// Get args to pass
			_, args := si.getData(vals, fields, fieldsInclude, idFieldExclude, true)
			// append PKColumn Value to args, as used later in Where clause
			args = append(args, vals.PKColumnVal)

			res, err := stmt.Exec(args...)
			if err != nil {
				return 0, err
			}
			rowsAffected, _ = res.RowsAffected()
			totalRowsAffected += rowsAffected
		}

	} else {
		// there is single struct
		res, err := p.Db.(DbWrapper).Exec(code, args...)
		if err != nil {
			return 0, err
		}
		rowsAffected, _ = res.RowsAffected()
		totalRowsAffected = rowsAffected
	}

	return totalRowsAffected, nil
}

func updateStructTx(p *Dbal, in interface{}, isSlice bool, fields map[string]bool, fieldsInclude bool) (int64, error) {
	/*
		1. prepare list of columns
		2. prepare parameter string
		3. Prepare statement
		4. pass values as arg...
	*/

	var (
		firstStruct       interface{}
		rowsAffected      int64
		totalRowsAffected int64
		structSlice       reflect.Value
		//params            []string
		code      string
		condition string
	)

	if isSlice {
		structSlice = reflect.ValueOf(in)
		for i := 0; i < structSlice.Len(); i++ {
			firstStruct = structSlice.Index(i).Interface()
			break
		}
	} else {
		firstStruct = in
	}
	//_ = firstStruct
	//fmt.Printf("Debug: firstStruct: %v\n", firstStruct)

	si, vals, _, err := parseStruct(firstStruct)
	if err != nil {
		return 0, fmt.Errorf("Error parsing struct.\n%v", err)
	}

	// Get columns and their values
	cols, args := si.getData(vals, fields, fieldsInclude, idFieldExclude, true)
	// append PKColumn Value to args, as used later in Where clause
	args = append(args, vals.PKColumnVal)
	//fmt.Printf("Debug: cols: %v\n", cols)

	// prepare parameter string and SQL statement
	//params = make([]string, len(cols))
	var b strings.Builder
	if dbconfig.paramNumeric {
		i := 1
		for i = 1; i <= len(cols); i++ {
			//params = append(params, fmt.Sprintf("%s = "+dbconfig.param+strconv.Itoa(i), cols[i-1]))
			//params[i-1] = fmt.Sprintf("%s = "+dbconfig.param+strconv.Itoa(i), cols[i-1])

			b.WriteString(cols[i-1])
			b.WriteString(" = ")
			b.WriteString(dbconfig.param)
			b.WriteString(strconv.Itoa(i))
			b.WriteString(",")
		}
		//condition = si.PkColName + " = " + dbconfig.param + strconv.Itoa(i)
		condition = concat(si.PkColName, " = ", dbconfig.param, strconv.Itoa(i))

	} else {
		for i := 1; i <= len(cols); i++ {
			//params = append(params, fmt.Sprintf("%s = "+dbconfig.param, cols[i-1]))
			//params[i-1] = fmt.Sprintf("%s = "+dbconfig.param, cols[i-1])

			b.WriteString(cols[i-1])
			b.WriteString(" = ")
			b.WriteString(dbconfig.param)
			b.WriteString(",")
		}
		//condition = si.PkColName + " = " + dbconfig.param
		condition = concat(si.PkColName, " = ", dbconfig.param)
	}

	params := b.String()
	params = string(params[0 : len(params)-1])
	code = fmt.Sprintf(dbconfig.sqlUpdate, si.Table, params, condition)

	if isSlice {
		// begin transaction
		tx, err := p.rawDb.Beginx()
		if err != nil {
			return 0, err
		}

		// create prepared statement
		stmt, err := tx.Prepare(code)
		if err != nil {
			tx.Rollback()
			return 0, err
		}

		// now execute statement and insert data
		for i := 0; i < structSlice.Len(); i++ {
			s := structSlice.Index(i).Interface()
			// get struct values for all columns
			si, vals, _, err := parseStruct(s)
			// Get args to pass
			_, args := si.getData(vals, fields, fieldsInclude, idFieldExclude, true)
			// append PKColumn Value to args, as used later in Where clause
			args = append(args, vals.PKColumnVal)

			res, err := stmt.Exec(args...)
			if err != nil {
				tx.Rollback()
				return 0, err
			}
			rowsAffected, _ = res.RowsAffected()
			totalRowsAffected += rowsAffected
		}
		err = tx.Commit()
		if err != nil {
			return 0, err
		}
	} else {
		// there is single struct
		res, err := p.Db.(DbWrapper).Exec(code, args...)
		if err != nil {
			return 0, err
		}
		rowsAffected, _ = res.RowsAffected()
		totalRowsAffected = rowsAffected
	}

	return totalRowsAffected, nil
}

func updateStructMap(p *Dbal, in interface{}) (int64, error) {
	/*
		1. prepare list of columns
		2. prepare parameter string
		3. Prepare statement
		4. pass values as arg...
	*/

	var (
		rowsAffected      int64
		totalRowsAffected int64
		structMap         reflect.Value
		//params            []string
		code      string
		condition string
	)

	structMap = reflect.ValueOf(in)
	fields := make(map[string]bool)

	// now execute statement and insert data
	for _, key := range structMap.MapKeys() {
		//si *structInfo, vals *structValues
		s := structMap.MapIndex(key).Interface()
		si, vals, _, err := parseStruct(s)
		if err != nil {
			return 0, fmt.Errorf("Error parsing struct.\n%v", err)
		}

		// Get columns and their values
		cols, args := si.getData(vals, fields, false, idFieldExclude, true)
		// append PKColumn Value to args, as used later in Where clause
		args = append(args, vals.PKColumnVal)

		// prepare parameter string and SQL statement
		//params = make([]string, len(cols))
		var b strings.Builder
		if dbconfig.paramNumeric {
			i := 1
			for i = 1; i <= len(cols); i++ {
				//params = append(params, fmt.Sprintf("%s = "+dbconfig.param+strconv.Itoa(i), cols[i-1]))
				//params[i-1] = fmt.Sprintf("%s = "+dbconfig.param+strconv.Itoa(i), cols[i-1])

				b.WriteString(cols[i-1])
				b.WriteString(" = ")
				b.WriteString(dbconfig.param)
				b.WriteString(strconv.Itoa(i))
				b.WriteString(",")
			}
			//condition = si.PkColName + " = " + dbconfig.param + strconv.Itoa(i)
			condition = concat(si.PkColName, " = ", dbconfig.param, strconv.Itoa(i))

		} else {
			for i := 1; i <= len(cols); i++ {
				//params = append(params, fmt.Sprintf("%s = "+dbconfig.param, cols[i-1]))
				//params[i-1] = fmt.Sprintf("%s = "+dbconfig.param, cols[i-1])

				b.WriteString(cols[i-1])
				b.WriteString(" = ")
				b.WriteString(dbconfig.param)
				b.WriteString(",")
			}
			//condition = si.PkColName + " = " + dbconfig.param
			condition = concat(si.PkColName, " = ", dbconfig.param)
		}

		params := b.String()
		params = string(params[0 : len(params)-1])
		code = fmt.Sprintf(dbconfig.sqlUpdate, si.Table, params, condition)
		//fmt.Printf("Debug: Code = %s\n\n", code)

		res, err := p.Db.(DbWrapper).Exec(code, args...)
		if err != nil {
			return 0, err
		}
		rowsAffected, _ = res.RowsAffected()
		totalRowsAffected += rowsAffected
	}

	return totalRowsAffected, nil
}

func updateStructMapTx(p *Dbal, in interface{}) (int64, error) {
	/*
		1. prepare list of columns
		2. prepare parameter string
		3. Prepare statement
		4. pass values as arg...
	*/

	var (
		rowsAffected      int64
		totalRowsAffected int64
		structMap         reflect.Value
		//params            []string
		code      string
		condition string
	)

	structMap = reflect.ValueOf(in)
	fields := make(map[string]bool)

	// begin transaction
	tx, err := p.rawDb.Beginx()
	if err != nil {
		return 0, err
	}

	// now execute statement and insert data
	for _, key := range structMap.MapKeys() {
		//si *structInfo, vals *structValues
		s := structMap.MapIndex(key).Interface()
		si, vals, _, err := parseStruct(s)
		if err != nil {
			tx.Rollback()
			return 0, fmt.Errorf("Error parsing struct.\n%v", err)
		}

		// Get columns and their values
		cols, args := si.getData(vals, fields, false, idFieldExclude, true)
		// append PKColumn Value to args, as used later in Where clause
		args = append(args, vals.PKColumnVal)

		// prepare parameter string and SQL statement
		//params = make([]string, len(cols))
		var b strings.Builder
		if dbconfig.paramNumeric {
			i := 1
			for i = 1; i <= len(cols); i++ {
				//params = append(params, fmt.Sprintf("%s = "+dbconfig.param+strconv.Itoa(i), cols[i-1]))
				//params[i-1] = fmt.Sprintf("%s = "+dbconfig.param+strconv.Itoa(i), cols[i-1])

				b.WriteString(cols[i-1])
				b.WriteString(" = ")
				b.WriteString(dbconfig.param)
				b.WriteString(strconv.Itoa(i))
				b.WriteString(",")
			}
			//condition = si.PkColName + " = " + dbconfig.param + strconv.Itoa(i)
			condition = concat(si.PkColName, " = ", dbconfig.param, strconv.Itoa(i))

		} else {
			for i := 1; i <= len(cols); i++ {
				//params = append(params, fmt.Sprintf("%s = "+dbconfig.param, cols[i-1]))
				//params[i-1] = fmt.Sprintf("%s = "+dbconfig.param, cols[i-1])

				b.WriteString(cols[i-1])
				b.WriteString(" = ")
				b.WriteString(dbconfig.param)
				b.WriteString(",")
			}
			//condition = si.PkColName + " = " + dbconfig.param
			condition = concat(si.PkColName, " = ", dbconfig.param)
		}

		params := b.String()
		params = string(params[0 : len(params)-1])
		code = fmt.Sprintf(dbconfig.sqlUpdate, si.Table, params, condition)
		//fmt.Printf("Debug: Code = %s\n\n", code)

		res, err := p.Db.(DbWrapper).Exec(code, args...)
		if err != nil {
			tx.Rollback()
			return 0, err
		}
		rowsAffected, _ = res.RowsAffected()
		totalRowsAffected += rowsAffected
	}
	err = tx.Commit()
	if err != nil {
		return 0, err
	}

	return totalRowsAffected, nil
}
