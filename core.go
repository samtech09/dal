package dal

import (
	"fmt"

	"github.com/jmoiron/sqlx"
)

// Query executes given query, return raw *sqlx.Rows.It is responsibility of caller to close rows.
func (p *Dbal) Query(code string, args ...interface{}) (*sqlx.Rows, error) {
	return query(p, code, args)
}

// NonQuery allow to execute DML statements like modifying/creating a table.
func (p *Dbal) NonQuery(sql string) (rowsAffected int64, err error) {
	// //MustExec Panic on error
	// // Handle pannic and return err instead
	// defer func() {
	// 	if r := recover(); r != nil {
	// 		var ok bool
	// 		err, ok = r.(error)
	// 		if !ok {
	// 			err = fmt.Errorf("NonQuery failed: %v", r)
	// 		}
	// 	}
	// }()

	rowsAffected = -1
	res, err := p.Db.(DbWrapper).Exec(sql)
	if err != nil {
		return 0, err
	}
	rowsAffected, err = res.RowsAffected()
	return
}

// ExecProc executes given Stored procedure, return raw *sqlx.Rows.It is responsibility of caller to close rows.
func (p *Dbal) ExecProc(proc string, args ...interface{}) (*sqlx.Rows, error) {
	code := fmt.Sprintf(dbconfig.sqlProc, proc)
	return query(p, code, args)
}

// ExecProcScalar executes given Stored procedure, return return first-most value from proc result
func (p *Dbal) ExecProcScalar(proc string, args ...interface{}) (interface{}, error) {
	code := fmt.Sprintf(dbconfig.sqlProc, proc)
	rows, err := query(p, code, args...)
	if err != nil {
		return nil, err
	}
	// make sure we always close rows
	defer rows.Close()

	rows.Next()

	var r interface{}
	if err = rows.Scan(&r); err != nil {
		return nil, err
	}

	if val, ok := r.([]byte); ok {
		return string(val), nil
	}

	return r, nil
}

// Delete removes data from table.
// condition: WHERE condition to filter rows. Do NOT pass 'WHERE' keyword itseelf in condition.
// args: parameters for condition
func (p *Dbal) Delete(table, condition string, args ...interface{}) (rowsAffected int64, err error) {
	// //MustExec Panic on error
	// // Handle pannic and return err instead
	// defer func() {
	// 	if r := recover(); r != nil {
	// 		var ok bool
	// 		err, ok = r.(error)
	// 		if !ok {
	// 			err = fmt.Errorf("Delete failed: %v", r)
	// 		}
	// 	}
	// }()

	code := fmt.Sprintf(dbconfig.sqlDelete, table, condition)
	rowsAffected = -1
	res, err := p.Db.(DbWrapper).Exec(code, args...)
	if err != nil {
		return 0, err
	}
	rowsAffected, err = res.RowsAffected()
	return
}

// NamedExec execute query and replace named parameters with value from args.
func (p *Dbal) NamedExec(query string, args ...interface{}) (rowsAffected int64, err error) {
	rowsAffected = -1
	res, err := p.rawDb.NamedExec(query, args)
	if err != nil {
		return 0, err
	}
	rowsAffected, err = res.RowsAffected()
	return rowsAffected, err
}

// NamedExec execute query and replace named parameters with value from args.
func (p *Dbal) Exec(query string, args ...interface{}) (rowsAffected int64, err error) {
	rowsAffected = -1
	res, err := p.rawDb.Exec(query, args)
	if err != nil {
		return 0, err
	}
	rowsAffected, err = res.RowsAffected()
	return rowsAffected, err
}

// Scalar executes given query and return value of first column from first row.
// It will be good to pass sql that only select single column, single row
// instead of loading thousands of rows and then discarding them.
func (p *Dbal) Scalar(code string, args ...interface{}) (interface{}, error) {
	rows, err := query(p, code, args...)
	if err != nil {
		return nil, err
	}
	// make sure we always close rows
	defer rows.Close()

	rows.Next()

	var r interface{}
	if err = rows.Scan(&r); err != nil {
		return nil, err
	}

	if val, ok := r.([]byte); ok {
		return string(val), nil
	}

	return r, nil
}

// // Transaction executes given function inside database Transaction.
// // Not tested.
// func (p *Dbal) Transaction(fn func(*sqlx.Tx) error) error {
// 	if tx, err := p.rawDb.Beginx(); err != nil {
// 		return err
// 	} else {
// 		if err = fn(tx); err != nil {
// 			tx.Rollback()
// 			return err
// 		} else {
// 			tx.Commit()
// 		}
// 	}
// 	return nil
// }

// Transaction executes given function inside database Transaction.
func (p *Dbal) Transaction(fn func(*Dbal) error) error {
	if db, ok := p.Db.(*sqlx.DB); ok {
		if tx, err := db.Beginx(); err != nil {
			return err
		} else {
			if err = fn(WrapDbal(tx)); err != nil {
				tx.Rollback()
				return err
			} else {
				tx.Commit()
			}
		}
	}
	return nil
}

// Query executes given query, return raw *sqlx.Rows.It is responsibility of caller to close rows.
func query(p *Dbal, code string, args ...interface{}) (*sqlx.Rows, error) {
	rows, err := p.Db.(DbWrapper).Queryx(code, args...)
	if err != nil {
		return nil, fmt.Errorf("Query error. %v", err)
	}
	return rows, nil
}
