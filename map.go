package dal

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

/* MAP are used only for query purpose
 * for insert update, use struct
 */

// // InsertMap inserts data into table using RowMap
// func (p *Dbal) InsertMap(table string, row RowMap, idFieldname string) (interface{}, error) {
// 	return insertMap(p, table, row, idFieldname, idFieldIncludeIfValue)
// }

// // UpdateMap updates data into table using RowMap.
// // fieldCSV: list of fields to be excluded / included for updateMap.
// // fieldsInclude: If false fields will be excluded, if true only fields given in fieldsCSV will be updated.
// // idFieldName: name of ID field in struct.
// func (p *Dbal) UpdateMap(table string, row RowMap, fieldsCSV string, fieldsInclude bool, idFieldName string) (int64, error) {
// 	return updateMap(p, table, row, idFieldName, idFieldExclude)
// }

// // UpdateMap updates data into table using RowMap
// // fieldCSV: list of fields to be excluded / included for updateMap
// // fieldsInclude: If false fields will be excluded, if true only fields given in fieldsCSV will be updated
// // condition: ??
// // args: arguments for condition
// func (p *Dbal) UpdateMap(table string, row RowMap, fieldsCSV string, fieldsInclude bool, idFieldName string, args ...interface{}) (int64, error) {
// 	return updateMap(p, table, row, idFieldName, idFieldExclude, args...)
// }

// FirstMap executes given query, return Rowmap for first row from result.
// It will be good to pass sql that only select single row
// instead of loading thousands of rows and then discarding them.
func (p *Dbal) FirstMap(code string, args ...interface{}) (RowMap, error) {
	rows, err := p.Db.(DbWrapper).Queryx(code, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	rows.Next()
	rowmap := RowMap{}
	err = rows.MapScan(rowmap)
	if err != nil {
		return nil, err
	}
	return rowmap, nil
}

// ExecProcMap executes given Stored procedure, returns slice of RowMap.
func (p *Dbal) ExecProcMap(proc string, args ...interface{}) ([]RowMap, error) {
	code := fmt.Sprintf(dbconfig.sqlProc, proc)
	rows, err := query(p, code, args...)
	if err != nil {
		return nil, err
	}
	// make sure we always close rows
	defer rows.Close()
	return rowsToMap(rows)
}

// QueryMap executes given query, return map[string]interface.
func (p *Dbal) QueryMap(code string, args ...interface{}) ([]RowMap, error) {
	rows, err := query(p, code, args...)
	if err != nil {
		return nil, err
	}
	// make sure we always close rows
	defer rows.Close()
	return rowsToMap(rows)
}

// QuerySliceInt executes given query, return slice []int.
func (p *Dbal) QuerySliceInt(code string, args ...interface{}) ([]int, error) {
	rows, err := query(p, code, args...)
	if err != nil {
		return nil, err
	}
	// make sure we always close rows
	defer rows.Close()
	return rowsToSliceInt(rows)
}

// QuerySliceInt64 executes given query, return slice []int64.
func (p *Dbal) QuerySliceInt64(code string, args ...interface{}) ([]int64, error) {
	rows, err := query(p, code, args...)
	if err != nil {
		return nil, err
	}
	// make sure we always close rows
	defer rows.Close()
	return rowsToSliceInt64(rows)
}

// QuerySliceStr executes given query, return slice []string.
func (p *Dbal) QuerySliceStr(code string, args ...interface{}) ([]string, error) {
	rows, err := query(p, code, args...)
	if err != nil {
		return nil, err
	}
	// make sure we always close rows
	defer rows.Close()
	return rowsToSliceStr(rows)
}

//
//
// ***************************
// PRIVATE FUNCTIONS - NOT IN USE NOW
// ***************************
//
//
func insertMap(p *Dbal, table string, row RowMap, idFieldName string, idFieldMode idFieldInclusionMode) (retid interface{}, err error) {
	// Check if given ID field name is valid and exist in map
	var id interface{}
	var ok bool
	if id, ok = row[idFieldName]; !ok {
		retid = -1
		err = fmt.Errorf("Invalid or non-existent ID field")
		return
	}

	//v := reflect.ValueOf(id)
	//Include or Exclude ID field as per mode set
	if (idFieldMode == idFieldIncludeIfValue && isZero(reflect.ValueOf(id))) || idFieldMode == idFieldExclude {
		// if convert mode is idFieldIncludeIfValue then
		// include ID field in map, only if it's value is set
		// If it has default Zero value, then exclude it
		//
		// Remove ID field from Map
		delete(row, idFieldName)
	}

	var (
		fields []string
		values []string
		args   []interface{}
	)
	i := 1
	for field, value := range row {
		fields = append(fields, field)
		if dbconfig.paramNumeric {
			values = append(values, dbconfig.param+strconv.Itoa(i))
		} else {
			values = append(values, dbconfig.param)
		}
		args = append(args, value)
		i++
	}

	code := ""
	if dbconfig.useReturningID {
		// In Postgres database, LastInsertID doesn't work.
		// We need to use RETURNING statement instead
		code = fmt.Sprintf(dbconfig.sqlInsert, table, strings.Join(fields, ", "), strings.Join(values, ", "), idFieldName)
		//return retid, errors.Errorf(code)
		err = p.Db.(DbWrapper).QueryRow(code, args...).Scan(&retid)
		if err != nil {
			return 0, err
		}
		if val, ok := retid.([]byte); ok {
			return string(val), nil
		}
		return
	}

	code = fmt.Sprintf(dbconfig.sqlInsert, table, strings.Join(fields, ", "), strings.Join(values, ", "))
	res, err := p.Db.(DbWrapper).Exec(code, args...)
	if err != nil {
		return 0, err
	}
	retid, _ = res.LastInsertId()
	return

}

func updateMap(p *Dbal, table string, row RowMap, idFieldName string, idFieldMode idFieldInclusionMode) (rowsAffected int64, err error) {
	// Check if given ID field name is valid and exist in map
	var id interface{}
	var ok bool
	if id, ok = row[idFieldName]; !ok {
		rowsAffected = -1
		err = fmt.Errorf("Invalid or non-existent ID field")
		return
	}

	//v := reflect.ValueOf(id)
	//Include or Exclude ID field as per mode set
	if (idFieldMode == idFieldIncludeIfValue && isZero(reflect.ValueOf(id))) || idFieldMode == idFieldExclude {
		// if convert mode is idFieldIncludeIfValue then
		// include ID field in map, only if it's value is set
		// If it has default Zero value, then exclude it
		//
		// Remove ID field from Map
		delete(row, idFieldName)
	}

	var (
		fields    []string
		values    []interface{}
		condition string
	)
	i := 1
	for field, value := range row {
		if dbconfig.paramNumeric {
			fields = append(fields, fmt.Sprintf("%s="+dbconfig.param+strconv.Itoa(i), field))
		} else {
			fields = append(fields, fmt.Sprintf("%s="+dbconfig.param, field))
		}
		values = append(values, value)
		i++
	}

	if dbconfig.paramNumeric {
		condition = idFieldName + " = " + dbconfig.param + strconv.Itoa(i)
	} else {
		condition = idFieldName + " = " + dbconfig.param
	}

	args := append(values, id)
	// generate SQL
	code := fmt.Sprintf(dbconfig.sqlUpdate, table, strings.Join(fields, ", "), condition)

	res, err := p.Db.(DbWrapper).Exec(code, args...)
	if err != nil {
		return 0, err
	}

	rowsAffected, _ = res.RowsAffected()
	return rowsAffected, nil
}
