package dal

// the name of our struct tag
const tagName = "dal"

type idFieldInclusionMode int16

const (
	idFieldInclude        idFieldInclusionMode = 1
	idFieldExclude        idFieldInclusionMode = 2
	idFieldIncludeIfValue idFieldInclusionMode = 3
)

type dbConfig struct {
	driver            string
	sqlInsert         string
	sqlUpdate         string
	sqlDelete         string
	sqlQueryByID      string
	sqlQuery          string
	sqlQueryFirst     string
	sqlProc           string
	sqlCount          string
	param             string
	paramNumeric      bool
	quote             string
	useReturningID    bool
	useArrayForIN     bool
	useTopToLimitRows bool
}

// DBMS is type to facilitate using supported Dbms constants.
type DBMS string

// Define DBMS and set driver name.
const (
	DbmsPostgreSQL DBMS = "postgres"
	DbmsSQLServer  DBMS = "mssql"
	DbmsSQLite     DBMS = "sqlite3"
)

func getDbConfig(dbms DBMS) *dbConfig {
	dbc := dbConfig{}
	switch dbms {
	case DbmsPostgreSQL:
		dbc.driver = "postgres"
		dbc.sqlInsert = "INSERT INTO %s (%s) VALUES (%s) RETURNING %s"
		dbc.sqlUpdate = "UPDATE %s SET %s WHERE %s"
		dbc.sqlDelete = "DELETE FROM %s WHERE %s"
		dbc.sqlQueryByID = "Select %s FROM %s WHERE %s=%s"
		dbc.sqlQuery = "Select %s FROM %s WHERE %s"
		dbc.sqlQueryFirst = "Select %s FROM %s WHERE %s LIMIT 1"
		dbc.sqlCount = "Select count(%s) from %s WHERE %s"
		// execute proc for Postgresql
		dbc.sqlProc = "SELECT * from %s"
		dbc.quote = `"`
		dbc.param = "$"
		dbc.paramNumeric = true
		dbc.useReturningID = true
		dbc.useArrayForIN = true
		dbc.useTopToLimitRows = false

	case DbmsSQLServer:
		dbc.driver = "mssql"
		dbc.sqlInsert = "INSERT INTO %s (%s) VALUES (%s)"
		dbc.sqlUpdate = "UPDATE %s SET %s WHERE %s"
		dbc.sqlDelete = "DELETE FROM %s WHERE %s"
		dbc.sqlQueryByID = "Select %s FROM %s WHERE %s=%s"
		dbc.sqlQuery = "Select %s FROM %s WHERE %s"
		dbc.sqlQueryFirst = "Select TOP 1 %s FROM %s WHERE %s"
		dbc.sqlCount = "Select count(%s) from %s WHERE %s"
		// execute proc for SQL Server
		dbc.sqlProc = "exec %s"
		dbc.quote = `"`
		dbc.param = "?"
		dbc.paramNumeric = false
		dbc.useReturningID = false
		dbc.useArrayForIN = false
		dbc.useTopToLimitRows = true

	case DbmsSQLite:
		dbc.driver = "sqlite3"
		dbc.sqlInsert = "INSERT INTO %s (%s) VALUES (%s)"
		dbc.sqlUpdate = "UPDATE %s SET %s WHERE %s"
		dbc.sqlDelete = "DELETE FROM %s WHERE %s"
		dbc.sqlQueryByID = "Select %s FROM %s WHERE %s=%s"
		dbc.sqlQuery = "Select %s FROM %s WHERE %s"
		dbc.sqlQueryFirst = "Select %s FROM %s WHERE %s LIMIT 1"
		dbc.sqlCount = "Select count(%s) from %s WHERE %s"
		// stored proc in SQLite are not available
		dbc.sqlProc = "not supported"
		dbc.quote = `"`
		dbc.param = "?"
		dbc.paramNumeric = false
		dbc.useReturningID = false
		dbc.useArrayForIN = false
		dbc.useTopToLimitRows = false

	}
	return &dbc
}
