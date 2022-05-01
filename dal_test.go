package dal

import (
	"fmt"
	"strconv"

	//"strconv"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

const connStr = "postgres://zmlmbu:123456@tantor.db.elephantsql.com:5432/zmlmbu"
const layout = "2006-01-02"

// DalTest - fields names are similar to one in datbase
type DalTest struct {
	meta      bool `dal:"daltest"`
	ID        int  `dal:"pk"`
	Name      string
	Dob       time.Time
	CreatedOn time.Time `dal:"noupdate"`
	Approved  bool
}

type DalTestNoTime struct {
	meta      bool `dal:"daltest"`
	ID        int  `dal:"pk"`
	Name      string
	Dob       string
	CreatedOn string `dal:"noupdate"`
	Approved  bool
}

type DalTestTimeUnix struct {
	meta      bool `dal:"daltest"`
	ID        int  `dal:"pk"`
	Name      string
	Dob       string
	CreatedOn int64 `dal:"noupdate"`
	Approved  bool
}

var dbal *Dbal

// func TestStruct(t *testing.T) {
// 	daltest := DalTest{}
// 	err := CheckStruct(&daltest)
// 	if err != nil {
// 		t.Errorf("TestStruct failed. %v", err)
// 	}
// }

// func TestSlice(t *testing.T) {
// 	daltest := []*DalTest{}
// 	err := CheckSlice(&daltest)
// 	if err != nil {
// 		t.Errorf("TestSlice failed. %v", err)
// 	}
// }

// func TestMap(t *testing.T) {
// 	daltest := make(map[int]interface{})
// 	d := DalTest{}
// 	daltest[1] = &d
// 	err := CheckMap(daltest)
// 	if err != nil {
// 		t.Errorf("TestMap failed. %v", err)
// 	}
// }

// func TestSliceUninit(t *testing.T) {
// 	var daltest []*DalTest
// 	err := CheckSlice(&daltest)
// 	if err != nil {
// 		t.Errorf("TestSliceUninit failed. %v", err)
// 	}
// }

// func TestMapUninit(t *testing.T) {
// 	var daltest map[int]interface{}
// 	err := CheckMap(daltest)
// 	if err != nil {
// 		t.Errorf("TestMapUninit failed. %v", err)
// 	}
// }

// func TestParseStruct(t *testing.T) {
// 	dt := DalTest{}
// 	dt.ID = 1
// 	dt.Name = "Santosh"
// 	dt.Dob, _ = time.Parse(layout, "2010-10-10")
// 	dt.CreatedOn, _ = time.Parse(layout, "2016-07-15")

// 	si, vals, _, err := parseStruct(dt)
// 	if err != nil {
// 		t.Errorf("ParseStruct failed. %v", err)
// 	}

// 	fmt.Printf("Struct-Info: \n"+
// 		"\tName: %s\n"+
// 		"\tTable: %s\n"+
// 		"\tPkColName: %s\n"+
// 		"\tPkColIndex: %d\n"+
// 		"\tColumns: %v\n"+
// 		"\tIndexes: %v\n"+
// 		"\tNoUpdateCols: %v\n", si.Name, si.Table, si.PkColName, si.PkColIndex, si.Cols, si.Indexes, si.NoUpdateCols)

// 	fmt.Printf("Values: %v\n", vals)

// 	fields, args := si.getData(vals, []string{"name"}, true, idFieldIncludeIfValue, true)
// 	fmt.Printf("Fields: %v\n", fields)
// 	fmt.Printf("Args: %v\n---\n\n", args)

// }

// func BenchmarkParseStruct(b *testing.B) {
// 	dt := DalTest{}
// 	dt.ID = 1
// 	dt.Name = "Santosh"
// 	dt.Dob, _ = time.Parse(layout, "2010-10-10")
// 	dt.CreatedOn, _ = time.Parse(layout, "2016-07-15")

// 	for n := 0; n < b.N; n++ {
// 		_, _, _, err := parseStruct(dt)
// 		if err != nil {
// 			b.Errorf("ParseStruct failed. %v", err)
// 		}
// 	}
// }

// func BenchmarkGetData(b *testing.B) {
// 	dt := DalTest{}
// 	dt.ID = 1
// 	dt.Name = "Santosh"
// 	dt.Dob, _ = time.Parse(layout, "2010-10-10")
// 	dt.CreatedOn, _ = time.Parse(layout, "2016-07-15")

// 	fields := make(map[string]bool)
// 	si, vals, _, err := parseStruct(dt)

// 	for n := 0; n < b.N; n++ {
// 		si.getData(vals, fields, true, idFieldIncludeIfValue, true)
// 		if err != nil {
// 			b.Errorf("ParseStruct failed. %v", err)
// 		}
// 	}
// }

//

// func TestStructToMap(t *testing.T) {
// 	row := DalTest{}
// 	row.ID = 101
// 	row.Name = "Unit-Test-2"
// 	row.Dob, _ = time.Parse(layout, "2010-10-10")

// 	rowmap, err := structToMap(row, []string{}, false, "ID")
// 	if err != nil {
// 		t.Errorf("Struct to Map conversion failed. %v", err)
// 	}

// 	fmt.Println("\t---\nStruct TO Map [ID: 101]")
// 	fmt.Printf("\t%v\n", rowmap)
// 	fmt.Println("")
// }

// func TestgetFieldValue(t *testing.T) {
// 	row := DalTest{}
// 	row.ID = 11
// 	row.Name = "Unit-Test-2"
// 	row.Dob, _ = time.Parse(layout, "2010-10-10")

// 	x, err := getFieldValue(row, "ID")
// 	if err != nil {
// 		t.Errorf("getFieldValue failed. %v", err)
// 	}
// 	v := x.(int)
// 	if v != 11 {
// 		t.Errorf("\nExpected: %d\tReceived: %d", row.ID, v)
// 	}
// }

//
//----------------------------
//
//
//  WOrking test
//
//----------------------------

func TestConnection(t *testing.T) {
	fmt.Println("\n\nTestConnection ***")
	err := initConnect()
	if err != nil {
		t.Errorf("Connection failed. %v", err)
	}

	err = dbal.Ping()
	if err != nil {
		t.Errorf("Ping failed. %v", err)
	}
}

func TestNonQuery(t *testing.T) {
	fmt.Println("\n\nTestNonQuery ***")
	//Drop previous table, error expected, ignore error
	sql := "drop table daltest; drop function getData(nam character varying); drop function getDataScalar(nam character varying);"
	dbal.NonQuery(sql)

	//Create table daltest
	sql = "Create table daltest( " +
		"ID serial not null," +
		"name character varying(50) NOT NULL," +
		"dob date NOT NULL," +
		"createdon timestamp without time zone NOT NULL," +
		"approved boolean NOT NULL default false" +
		")"
	_, err := dbal.NonQuery(sql)
	if err != nil {
		t.Errorf("NonQuery 'Create table' failed. %v", err)
	}

	//Create proc getData
	sql2 := "CREATE OR REPLACE FUNCTION getData(nam character varying)" +
		"  RETURNS Table(id integer, name character varying, dob date, createdon date)" +
		"  AS $BODY$ " +
		" begin	" +
		"	return QUERY " +
		"		select a.id, a.name, a.dob, a.createdon from daltest a " +
		"		where a.name like '%' || nam || '%'; " +
		" end; $BODY$" +
		"  LANGUAGE plpgsql VOLATILE STRICT"
	_, err2 := dbal.NonQuery(sql2)
	if err2 != nil {
		//t.Errorf("NonQuery 'Create proc' failed. %v", err2)
		t.Errorf("NonQuery 'Create proc' failed.")
	}

	//Create proc getDataScalar
	sql3 := "CREATE OR REPLACE FUNCTION getDataScalar(nam character varying)" +
		"  RETURNS integer" +
		"  AS $BODY$ " +
		"	Declare retid integer:=0;" +
		" begin	" +
		"		select into retid a.id from daltest a " +
		"		where a.name like '%' || nam || '%' limit 1; " +
		"   return retid; " +
		" end; $BODY$" +
		"  LANGUAGE plpgsql VOLATILE STRICT"
	_, err3 := dbal.NonQuery(sql3)
	if err3 != nil {
		//t.Errorf("NonQuery 'Create proc' failed. %v", err2)
		t.Errorf("NonQuery 'Create Scalar proc' failed.\n%v", err3)
	}
}

func TestInsertStructSingle(t *testing.T) {
	fmt.Println("\n\nTestInsertStructSingle ***")
	row := DalTestNoTime{}
	row.Name = "Single-Struct"
	// row.Dob, _ = time.Parse(layout, "2010-10-10")
	// row.CreatedOn, _ = time.Parse(layout, "2016-07-15")

	row.Dob = "2010-10-10"
	row.CreatedOn = "2016-07-15 21:34:54"

	// Try to insrt data through struct
	err := dbal.InsertStruct(&row, false)
	if err != nil {
		t.Errorf("\tInsertStructSingle failed. %v", err)
	}

	//fmt.Printf("Returned ID: %d\n", toIntx(id))
	//fmt.Printf("\tStruct: %v\n", row)

	// Id field in struct must be non zero
	if row.ID < 1 {
		t.Errorf("\n\tExpected: %s\tReceived: %d", ">0", row.ID)
	}
}

// func BenchmarkInsertStructSingle(b *testing.B) {
// 	initConnect()

// 	//fmt.Println("\n\nTestInsertStructSingle ***")
// 	row := DalTest{}
// 	row.Name = "Single-Struct"
// 	row.Dob, _ = time.Parse(layout, "2010-10-10")
// 	row.CreatedOn, _ = time.Parse(layout, "2016-07-15")

// 	b.ReportAllocs()
// 	b.ResetTimer()
// 	for n := 0; n < b.N; n++ {
// 		// Try to insrt data through struct
// 		err := dbal.InsertStruct(&row, false)
// 		if err != nil {
// 			b.Errorf("\tInsertStructSingle failed. %v", err)
// 		}
// 	}

// 	dbal.Close()
// }

func TestInsertStructSlice(t *testing.T) {
	fmt.Println("\n\nTestInsertStructSlice ***")
	rows := []*DalTestNoTime{}
	for i := 1; i < 3; i++ {
		row := DalTestNoTime{}
		row.Name = "Slice-" + strconv.Itoa(i)
		// row.Dob, _ = time.Parse(layout, "2010-10-"+strconv.Itoa(10+i))
		// row.CreatedOn, _ = time.Parse(layout, "2016-07-15")

		row.Dob = "2010-10-11"
		row.CreatedOn = "2016-07-16 11:14:51"

		rows = append(rows, &row)
	}

	// Try to insrt data through struct
	err := dbal.InsertStruct(rows, false)
	if err != nil {
		t.Errorf("\tInsertStructSlice failed. %v", err)
	}
}

// 	for _, row := range rows {
// 		//fmt.Printf("\tStruct: %v\n", row)
// 		// Id field in struct must be non zero
// 		if row.ID < 1 {
// 			t.Errorf("\n\tExpected: %s\tReceived: %d", ">0", row.ID)
// 		}
// 	}
// }

// func TestInsertStructMap(t *testing.T) {
// 	fmt.Println("\n\nTestInsertStructMap ***")
// 	rows := make(map[int]interface{})
// 	for i := 5; i < 8; i++ {
// 		row := DalTest{}
// 		row.Name = "MapTest-" + strconv.Itoa(i)
// 		row.Dob, _ = time.Parse(layout, "2010-10-"+strconv.Itoa(10+i))
// 		row.CreatedOn, _ = time.Parse(layout, "2016-07-15")
// 		rows[i] = &row
// 	}

// 	// Try to insrt data through struct
// 	err := dbal.InsertStruct(rows, true)
// 	if err != nil {
// 		t.Errorf("\tInsertStruct failed. %v", err)
// 	}

// 	for _, row := range rows {
// 		//fmt.Printf("\tStruct: %v\n", row)
// 		// Id field in struct must be non zero
// 		if row.(*DalTest).ID < 1 {
// 			t.Errorf("\n\tExpected: %s\tReceived: %d", ">0", row.(*DalTest).ID)
// 		}
// 	}
// }

// func TestQueryStructBySQL(t *testing.T) {
// 	fmt.Println("\n\nTestQueryStructBySQL ***")

// 	sql := "Select * from daltest where id=$1"
// 	var daltest []*DalTest
// 	err := dbal.QueryStructBySQL(sql, &daltest, 1)
// 	if err != nil {
// 		t.Errorf("\tQueryStructBySQL failed. %v", err)
// 	}
// 	if len(daltest) != 1 {
// 		t.Errorf("\n\tExpected: %d\tReceived: %d", 1, len(daltest))
// 	}
// }

// func TestQueryStructByID(t *testing.T) {
// 	fmt.Println("\n\nTestQueryStructByID ***")

// 	daltest := DalTest{}
// 	err := dbal.QueryStructByID(&daltest, 1)
// 	if err != nil {
// 		t.Errorf("\tQueryStructByID failed. %v", err)
// 	}

// 	//fmt.Printf("Struct: %v\n", daltest)

// 	if daltest.ID != 1 {
// 		t.Errorf("\n\tExpected: %d\tReceived: %d", 1, daltest.ID)
// 	}
// }

func TestQueryStruct(t *testing.T) {
	fmt.Println("\n\nTestQueryStruct ***")

	var daltest []*DalTest
	err := dbal.QueryStruct(&daltest, "name like $1", "Slice%")
	if err != nil {
		t.Errorf("\tQueryStruct failed. %v", err)
	}
	for i, d := range daltest {
		fmt.Printf("%d- %v\n", i, d)
	}

	// there should be exactly 2 records
	if len(daltest) != 2 {
		t.Errorf("\n\tExpected: %d\tReceived: %d", 1, len(daltest))
	}
}

func TestQueryStructNoTime(t *testing.T) {
	fmt.Println("\n\nTestQueryStructNoTime ***")

	var daltest []*DalTestNoTime
	err := dbal.QueryStruct(&daltest, "name like $1", "Slice%")
	if err != nil {
		t.Errorf("\tQueryStruct failed. %v", err)
	}
	for i, d := range daltest {
		fmt.Printf("%d- %v\n", i, d)
	}

	// there should be exactly 2 records
	if len(daltest) != 2 {
		t.Errorf("\n\tExpected: %d\tReceived: %d", 1, len(daltest))
	}
}

func TestQueryStructTimeUnix(t *testing.T) {
	fmt.Println("\n\nTestQueryStructNoTime ***")

	var daltest []*DalTestTimeUnix
	err := dbal.QueryStruct(&daltest, "name like $1", "Slice%")
	if err != nil {
		t.Errorf("\tQueryStruct failed. %v", err)
	}
	for i, d := range daltest {
		fmt.Printf("%d- %v\n", i, d)
	}

	// there should be exactly 2 records
	if len(daltest) != 2 {
		t.Errorf("\n\tExpected: %d\tReceived: %d", 1, len(daltest))
	}
}

// func TestFirstStruct(t *testing.T) {
// 	fmt.Println("\n\nTestFirstStruct ***")

// 	daltest := DalTest{}
// 	err := dbal.FirstStruct(&daltest, "1=1")
// 	if err != nil {
// 		t.Errorf("\tFirstStruct failed. %v", err)
// 	}
// 	if daltest.ID < 1 {
// 		t.Errorf("\n\tExpected: %s\tReceived: %d", ">0", daltest.ID)
// 	}
// }

// func TestUpdateStructSingle(t *testing.T) {
// 	fmt.Println("\n\nTestUpdateStructSingle ***")

// 	daltest := DalTest{}
// 	err := dbal.FirstStruct(&daltest, "ID=$1", 1)
// 	if err != nil {
// 		t.Errorf("Update-QueryStruct failed. %v", err)
// 	}

// 	daltest.Name = daltest.Name + "-Updated"
// 	daltest.CreatedOn, _ = time.Parse(layout, "2017-08-18")
// 	ra, err := dbal.UpdateStruct(&daltest, "", false, false)
// 	if err != nil {
// 		t.Errorf("\tUpdateStruct failed. %v", err)
// 	}
// 	if ra != 1 {
// 		t.Errorf("\n\tExpected: %d\tReceived: %d", 1, ra)
// 	}
// }

// func TestUpdateStructSlice(t *testing.T) {
// 	fmt.Println("\n\nTestUpdateStructSlice ***")

// 	sql := "Select * from daltest where Name like $1"
// 	var daltest []*DalTest
// 	err := dbal.QueryStructBySQL(sql, &daltest, "Slice%")
// 	if err != nil {
// 		t.Errorf("Update-QueryStructBySQL failed. %v", err)
// 	}

// 	for _, s := range daltest {
// 		s.Name = s.Name + "Updated"
// 		s.CreatedOn, _ = time.Parse(layout, "2018-08-18")

// 	}
// 	ra, err := dbal.UpdateStruct(daltest, "", false, false)
// 	if err != nil {
// 		t.Errorf("\tUpdateStruct failed. %v", err)
// 	}
// 	if ra != int64(len(daltest)) {
// 		t.Errorf("\n\tExpected: %d\tReceived: %d", int64(len(daltest)), ra)
// 	}
// }

// func TestUpdateStructMap(t *testing.T) {
// 	fmt.Println("\n\nTestUpdateStructMap ***")

// 	sql := "Select * from daltest where Name like $1"
// 	var daltest []*DalTest
// 	dalMap := make(map[int]interface{})
// 	err := dbal.QueryStructBySQL(sql, &daltest, "MapTest%")
// 	if err != nil {
// 		t.Errorf("Update-QueryStructBySQL failed. %v", err)
// 	}

// 	for i, s := range daltest {
// 		s.Name = s.Name + "Updated"
// 		s.CreatedOn, _ = time.Parse(layout, "2018-08-18")
// 		dalMap[i] = s
// 	}
// 	ra, err := dbal.UpdateStructMap(dalMap, true)
// 	if err != nil {
// 		t.Errorf("\tUpdateStruct failed. %v", err)
// 	}
// 	if ra != int64(len(daltest)) {
// 		t.Errorf("\n\tExpected: %d\tReceived: %d", int64(len(daltest)), ra)
// 	}
// }

// func TestQuerymap(t *testing.T) {
// 	fmt.Println("\n\nTestQuerymap ***")

// 	sql := "Select * from daltest where id=$1"
// 	rowmap, err := dbal.QueryMap(sql, 1)
// 	if err != nil {
// 		t.Errorf("QueryMap failed. %v", err)
// 	}

// 	fmt.Printf("\nrowmap:\n%v\n\n", rowmap)

// 	// print keys
// 	keys := reflect.ValueOf(rowmap[0]).MapKeys()
// 	fmt.Println(keys) // [a b c]

// 	if len(rowmap) != 1 {
// 		t.Errorf("\nExpected: %d\tReceived: %d", 1, len(rowmap))
// 	}
// }

// func TestQuerySliceInt(t *testing.T) {
// 	fmt.Println("\n\nTestQuerySliceInt ***")

// 	sql := "Select id from daltest"
// 	ids, err := dbal.QuerySliceInt(sql)
// 	if err != nil {
// 		t.Errorf("QuerySliceInt failed. %v", err)
// 	}

// 	fmt.Printf("\nids:\n%v\n", ids)

// 	if len(ids) < 1 {
// 		t.Errorf("\nExpected: %d\tReceived: %d", 1, len(ids))
// 	}
// }

// func TestQuerySliceInt64(t *testing.T) {
// 	fmt.Println("\n\nTestQuerySliceInt64 ***")

// 	sql := "Select id from daltest"
// 	ids, err := dbal.QuerySliceInt64(sql)
// 	if err != nil {
// 		t.Errorf("QuerySliceInt64 failed. %v", err)
// 	}

// 	fmt.Printf("\nids:\n%v\n", ids)

// 	if len(ids) < 1 {
// 		t.Errorf("\nExpected: %d\tReceived: %d", 1, len(ids))
// 	}
// }

// func TestQuerySliceStr(t *testing.T) {
// 	fmt.Println("\n\nTestQuerySliceStr ***")

// 	sql := "Select name from daltest"
// 	names, err := dbal.QuerySliceStr(sql)
// 	if err != nil {
// 		t.Errorf("QuerySliceStr failed. %v", err)
// 	}

// 	fmt.Printf("\nnames:\n%v\n", names)

// 	if len(names) < 1 {
// 		t.Errorf("\nExpected: %d\tReceived: %d", 1, len(names))
// 	}
// }

// func TestFirstMap(t *testing.T) {
// 	fmt.Println("\n\nTestFirstMap ***")

// 	sql := "Select * from daltest order by ID"
// 	rowmap, err := dbal.FirstMap(sql)
// 	if err != nil {
// 		t.Errorf("FirstMap failed. %v", err)
// 	}
// 	if toIntx(rowmap["id"]) != 1 {
// 		t.Errorf("\nExpected: %d\tReceived: %d", 1, toIntx(rowmap["id"]))
// 	}

// 	// fmt.Println("\t---\nFirstMap:")
// 	// fmt.Printf("\t%v\n", rowmap)
// 	// fmt.Println("")
// }

// func TestExecProcStruct(t *testing.T) {
// 	fmt.Println("\n\nTestExecProcStruct ***")

// 	proc := "getData ($1)"
// 	var daltest []*DalTest
// 	err := dbal.ExecProcStruct(proc, &daltest, "Updat")
// 	if err != nil {
// 		t.Errorf("ExecProcStruct failed. %v", err)
// 	}
// 	if len(daltest) < 1 {
// 		t.Errorf("\nExpected: %s\tReceived: %d", ">0", len(daltest))
// 	}
// }

// func TestExecProcMap(t *testing.T) {
// 	fmt.Println("\n\nTestExecProcMap ***")

// 	proc := "getData ($1)"
// 	rowmaps, err := dbal.ExecProcMap(proc, "Updat")
// 	if err != nil {
// 		t.Errorf("ExecProcStruct failed. %v", err)
// 	}
// 	if len(rowmaps) < 1 {
// 		t.Errorf("\nExpected: %s\tReceived: %d", ">0", len(rowmaps))
// 	}
// }

// func TestScalar(t *testing.T) {
// 	fmt.Println("\n\nTestScalar ***")

// 	sql := "Select name from daltest;"
// 	nm, err := dbal.Scalar(sql)
// 	if err != nil {
// 		t.Errorf("TestScalar failed. %v", err)
// 	}
// 	expected := "Single-Struct-Updated"
// 	if nm.(string) != expected {
// 		t.Errorf("\nExpected: %s\tReceived: %s", expected, nm.(string))
// 	}
// }

// func TestDelete(t *testing.T) {
// 	fmt.Println("\n\nTestDelete ***")

// 	nm, err := dbal.Delete("daltest", "ID=$1", 1)
// 	if err != nil {
// 		t.Errorf("nTestDelete failed. %v", err)
// 	}
// 	if nm != 1 {
// 		t.Errorf("\nExpected: %d\tReceived: %d", 1, nm)
// 	}
// }

// func TestDeleteStruct(t *testing.T) {
// 	fmt.Println("\n\nTestDeleteStruct ***")

// 	daltest := DalTest{}
// 	nm, err := dbal.DeleteStruct(&daltest, 2)
// 	if err != nil {
// 		t.Errorf("\nTestDeleteStruct failed. %v", err)
// 	}
// 	if nm != 1 {
// 		t.Errorf("\nExpected: %d\tReceived: %d", 1, nm)
// 	}
// }

// func TestTransaction(t *testing.T) {
// 	fmt.Println("\n\nTestTransaction ***")

// 	f := func(p *Dbal) error {
// 		row := DalTest{}
// 		row.Name = "Single-Struct-Trans"
// 		row.Dob, _ = time.Parse(layout, "2010-11-10")
// 		row.CreatedOn, _ = time.Parse(layout, "2016-07-15")

// 		// Try to insrt data through struct
// 		err := p.InsertStruct(&row, false)
// 		if err != nil {
// 			return err
// 		}
// 		// // force return err so trans can be rolebacked
// 		// return fmt.Errorf("error forced")

// 		// Must return err or nil
// 		// if err is returned then transaction will be rolebacked.
// 		return nil
// 	}

// 	err := dbal.Transaction(f)
// 	if err != nil {
// 		t.Errorf("Transaction failed. %v", err)
// 	}
// }

// func TestTransactionMulti(t *testing.T) {
// 	fmt.Println("\n\nTestTransactionMulti ***")

// 	f := func(p *Dbal) error {
// 		rows := []*DalTest{}
// 		for i := 1; i < 3; i++ {
// 			row := DalTest{}
// 			row.Name = "Trans-" + strconv.Itoa(i)
// 			row.Dob, _ = time.Parse(layout, "2010-10-"+strconv.Itoa(10+i))
// 			row.CreatedOn, _ = time.Parse(layout, "2016-07-15")
// 			rows = append(rows, &row)
// 		}

// 		// Try to insrt data through struct
// 		err := p.InsertStruct(rows, false)
// 		if err != nil {
// 			return err
// 		}

// 		// // force return err so trans can be rolebacked
// 		// return fmt.Errorf("error forced")

// 		// Must return err or nil
// 		// if err is returned then transaction will be rolebacked.
// 		return nil
// 	}

// 	err := dbal.Transaction(f)
// 	if err != nil {
// 		t.Errorf("Transaction failed. %v", err)
// 	}
// }

// func TestQueryStructBySQLAll(t *testing.T) {
// 	fmt.Println("\n\nFinal rows in database ***")

// 	sql := "Select * from daltest"
// 	var daltest []*DalTest
// 	err := dbal.QueryStructBySQL(sql, &daltest)
// 	if err != nil {
// 		t.Errorf("\tQueryStructBySQL failed. %v", err)
// 	}
// 	for _, s := range daltest {
// 		fmt.Printf("\t%v\n", s)
// 	}
// }

// func TestExecProcSalar(t *testing.T) {
// 	fmt.Println("\n\nTestExecProcScalar ***")

// 	proc := "getDataScalar ($1)"
// 	vlu, err := dbal.ExecProcScalar(proc, "Updat")
// 	if err != nil {
// 		t.Errorf("ExecProcScalar failed. %v", err)
// 	}
// 	//convert vlu to int
// 	intVlu := toIntx(vlu)
// 	if intVlu < 1 {
// 		t.Errorf("\nExpected: %s\tReceived: %d", ">0", intVlu)
// 	}
// }

// func TestIntIN(t *testing.T) {
// 	fmt.Println("\n\nTestStrIN ***")
// 	ids := []int{1, 2}
// 	sql := "Select * from daltest where " + NewSQLBuilder().IntIN("id", ids)
// 	fmt.Println(sql)
// 	var daltest []*DalTest
// 	err := dbal.QueryStructBySQL(sql, &daltest)
// 	if err != nil {
// 		t.Errorf("\nTestStrIN failed. %v", err)
// 	}
// 	for _, s := range daltest {
// 		fmt.Printf("\t%v\n", s)
// 	}
// }

// func TestStrIN(t *testing.T) {
// 	fmt.Println("\n\nTestStrIN ***")
// 	names := []string{"MapTest-5Updated", "Trans-2"}
// 	sql := "Select * from daltest where " + NewSQLBuilder().StrIN("name", names)
// 	fmt.Println(sql)
// 	var daltest []*DalTest
// 	err := dbal.QueryStructBySQL(sql, &daltest)
// 	if err != nil {
// 		t.Errorf("\nTestStrIN failed. %v", err)
// 	}
// 	for _, s := range daltest {
// 		fmt.Printf("\t%v\n", s)
// 	}
// }

// ----------------------------
//
//  BENCHMARKING
//
// -----------------------------

func BenchmarkParseStruct(b *testing.B) {
	dt := DalTest{}
	dt.ID = 1
	dt.Name = "Santosh"
	dt.Dob, _ = time.Parse(layout, "2010-10-10")
	dt.CreatedOn, _ = time.Parse(layout, "2016-07-15")

	for n := 0; n < b.N; n++ {
		_, _, _, _ = parseStruct(&dt)
	}
}

func BenchmarkGetQueryCols(b *testing.B) {
	dt := DalTest{}
	dt.ID = 1
	dt.Name = "Santosh"
	dt.Dob, _ = time.Parse(layout, "2010-10-10")
	dt.CreatedOn, _ = time.Parse(layout, "2016-07-15")

	si, _, _, _ := parseStruct(&dt)

	for n := 0; n < b.N; n++ {
		_ = si.getQueryCols()
	}
}

// func BenchQueryStructByID(b *testing.B) {
// 	for n := 0; n < b.N; n++ {
// 		daltest := DalTest{}
// 		_ = dbal.QueryStructByID(&daltest, 1)
// 	}
// }

func TestClose(t *testing.T) {
	dbClose()
}

func initConnect() error {
	var err error
	dbal, err = NewDbal(DbmsPostgreSQL, connStr, nil)
	if err != nil {
		return err
	}
	return nil
}

func dbClose() {
	dbal.Close()
}
