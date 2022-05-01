package dal

import (
	"fmt"

	"github.com/jmoiron/sqlx"
)

var prepstmts = make(map[int]*sqlx.Stmt)

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

//ExecPrepScalar execute given prepared statement and return sclar value
func (p *Dbal) ExecPrepScalar(id int, args ...interface{}) (interface{}, error) {
	// find stmt by id
	stmt, ok := prepstmts[id]
	if !ok {
		return nil, fmt.Errorf("invalid id")
	}

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
func (p *Dbal) ExecPrepStruct(id int, dest interface{}, args ...interface{}) error {
	// find stmt by id
	stmt, ok := prepstmts[id]
	if !ok {
		return fmt.Errorf("invalid id")
	}

	rows, err := stmt.Queryx(args...)
	if err != nil {
		return err
	}
	// make sure we always close rows
	defer rows.Close()

	return rowsToStruct(rows, dest)
}
