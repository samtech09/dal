/*Package dal is Database Abstration Layer for different DBMS (Postgres, SQL Server etc.)
It allow to use Struct and Map to insert update data.

Auther: Santosh Gupta.
Company: Mahendra Educational Pvt. Ltd.
Date: 2016-06-28.

To generate doc
godoc -html mahendras/dal.orig > dal.html

*/
package dal

import (
	"database/sql"

	"github.com/jmoiron/sqlx"
)

// Hold current database type in use
//var _dbms DBMS
var dbconfig *dbConfig

// RowMap is type for map[string]interface for easy accessing row data as Map.
type RowMap map[string]interface{}

// DbWrapper defines interface that provider must support.
type DbWrapper interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Queryx(query string, args ...interface{}) (*sqlx.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	QueryRowx(query string, args ...interface{}) *sqlx.Row
	Preparex(query string) (*sqlx.Stmt, error)
}

// Dbal Struct represents Core Database Abstraction Layer.
type Dbal struct {
	Db    interface{}
	rawDb *sqlx.DB
}

// NewDbal creates a new Dbal object and initialize with Db connection.
func NewDbal(dbms DBMS, connString string, fn func(*sqlx.DB)) (*Dbal, error) {
	// prepare connection
	// sql.Open() does not establish any connections to the database
	// It simply prepares the database abstraction for later use
	// e.g.  sql.Open("postgres", connString)
	db, err := sqlx.Open(string(dbms), connString)
	if err != nil {
		return nil, err
	}

	if fn != nil {
		fn(db)
	}

	p := WrapDbal(db)
	p.rawDb = db
	//_dbms = dbms
	dbconfig = getDbConfig(dbms)
	return p, nil
}

// WrapDbal Creates new Dbal instance and initialize with given db connection.
func WrapDbal(db interface{}) *Dbal {
	p := new(Dbal)
	p.Db = db
	return p
}

// Close closes database connection.
func (p *Dbal) Close() {
	if db, ok := p.Db.(*sqlx.DB); ok {
		db.Close()
	}
}

// Ping tries to immediately connect to database and report if any connection error.
func (p *Dbal) Ping() error {
	return p.rawDb.Ping()
}

// func (p *Dbal) Reflect() {
// 	v := reflect.ValueOf(p.Db).Elem()
// 	fmt.Printf("p = %v\n", reflect.TypeOf(v))
// 	fmt.Printf("p = %v\n", reflect.TypeOf(v).Kind())
// 	fmt.Printf("p = %v\n", reflect.ValueOf(v))
// 	fmt.Printf("p = %v\n", reflect.ValueOf(v).Kind())
// }
