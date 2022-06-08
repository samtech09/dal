package dal

import (
	"fmt"

	"github.com/jmoiron/sqlx"
)

var prepstmts = make(map[int]*sqlx.Stmt)
var prepstmtsS = make(map[string]*sqlx.Stmt)

//MaxPrepId gives ID of last-most prepared statement
func (p *Dbal) MaxPrepId() int {
	return len(prepstmts)
}

//Prepare create prepared statement for given query
func (p *Dbal) Prepare(sql string) (int, error) {
	// create Prepared statement
	s, err := p.Db.(DbWrapper).Preparex(sql)
	if err != nil {
		return -1, err
	}
	id := len(prepstmts)
	prepstmts[id] = s
	return id, nil
}

//PrepareEx create prepared statement with given key that can be referenced later, for given query
func (p *Dbal) PrepareEx(sql, key string) error {
	// create Prepared statement
	s, err := p.Db.(DbWrapper).Preparex(sql)
	if err != nil {
		return err
	}
	prepstmtsS[key] = s
	return nil
}

//ExecPrepScalar execute given prepared statement and return scalar value
func (p *Dbal) ExecPrepScalar(id int, args ...interface{}) (interface{}, error) {
	// find stmt by id
	stmt, ok := prepstmts[id]
	if !ok {
		return nil, fmt.Errorf("invalid id")
	}
	return p.execPrepScalar(stmt, args...)
}

//ExecPrepScalarEx execute given prepared statement found by key and return scalar value
func (p *Dbal) ExecPrepScalarEx(key string, args ...interface{}) (interface{}, error) {
	// find stmt by id
	stmt, ok := prepstmtsS[key]
	if !ok {
		return nil, fmt.Errorf("invalid key")
	}
	return p.execPrepScalar(stmt, args...)
}

//ExecPrepStruct executes given prepared statement and returns struct
func (p *Dbal) ExecPrepStruct(id int, dest interface{}, args ...interface{}) error {
	// find stmt by id
	stmt, ok := prepstmts[id]
	if !ok {
		return fmt.Errorf("invalid id")
	}
	return p.execPrepStruct(stmt, dest, args...)
}

//ExecPrepStructEx executes given prepared statement found by key and returns struct
func (p *Dbal) ExecPrepStructEx(key string, dest interface{}, args ...interface{}) error {
	// find stmt by id
	stmt, ok := prepstmtsS[key]
	if !ok {
		return fmt.Errorf("invalid key")
	}
	return p.execPrepStruct(stmt, dest, args...)
}

//ExecPrepScalar execute given prepared statement and return sclar value
func (p *Dbal) execPrepScalar(stmt *sqlx.Stmt, args ...interface{}) (interface{}, error) {
	rows, err := stmt.Queryx(args...)
	if err != nil {
		return nil, fmt.Errorf("Query error. %v", err)
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

//ExecPrepStruct executes given prepared statement and returns struct
func (p *Dbal) execPrepStruct(stmt *sqlx.Stmt, dest interface{}, args ...interface{}) error {
	rows, err := stmt.Queryx(args...)
	if err != nil {
		return err
	}
	// make sure we always close rows
	defer rows.Close()

	return rowsToStruct(rows, dest)
}
