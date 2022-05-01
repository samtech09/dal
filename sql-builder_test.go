package dal

import (
	//"strings"
	"fmt"
	//"strings"

	//"regexp"
	"testing"
)

//QuestionData table
type QuestionData struct {
	meta     bool `dal:"questiondata"`
	ID       int  `dal:"pk"`
	QID      int
	LangCode string
}

//Questions table
type Question struct {
	meta            bool `dal:"questions"`
	ID              int  `dal:"pk"`
	QType           string
	DifficultyLevel string
	OptionNumbering string //whether option will be in number (1,2,3) or alphabet ( A,B,C) or upper alpha ( I,II,III) or loweralpha(i,ii,iii)
	TopicID         int
	PassageID       int
}

func TestGetDbModelTable(t *testing.T) {
	fmt.Println("\n\nTestGetDbModelTable ***")
	tag := getDbModelTable(&Question{})
	exp := "questions"
	if tag != exp {
		t.Errorf("Expected %s,\nGot %s", exp, tag)
	}
}

func TestGetDbModelTable2(t *testing.T) {
	// at console it should ptint 'struct found in cache'
	fmt.Println("\n\nTestGetDbModelTable2 ***")
	tag := getDbModelTable(&Question{})
	exp := "questions"
	if tag != exp {
		t.Errorf("Expected %s,\nGot %s", exp, tag)
	}
}

// func TestPrepareSQL(t *testing.T) {
// 	fmt.Println("\n\nTestPrepareSQL ***")

// 	sql := "Select q.ID, qd.QID where q.ID=qd.QID and q.TopicID=$1;"
// 	tables := make(map[string]string)
// 	tables["q"] = getDbModelTable(&Question{})
// 	tables["qd"] = getDbModelTable(&QuestionData{})
// 	sql = prepareSQL(sql, tables)
// 	//sql = PrepareSQL(sql, []string{"a1", "a1"})
// 	exp := "Select questions.ID, questiondata.QID where questions.ID=questiondata.QID and questions.TopicID=$1;"
// 	if sql != exp {
// 		t.Errorf("Expected %s\nGot %s", exp, sql)
// 	}
// }

// func TestBuilder(t *testing.T) {
// 	fmt.Println("\n\nTestBuilder ***")

// 	//builder := SQLBuilder{}

// 	sql := NewSQLBuilder().Select("q.ID, qd.QID").
// 		From("q, qd").
// 		Where("q.ID=qd.QID and q.TopicID=$1").
// 		OrderBy("qd.QID", true).
// 		Table(&Question{}, "q").
// 		Table(&QuestionData{}, "qd").
// 		Build()

// 	exp := "select questions.ID, questiondata.QID from questions, questiondata where questions.ID=questiondata.QID and questions.TopicID=$1 order by questiondata.QID desc"
// 	if sql != exp {
// 		fmt.Printf("Sql: %d, Exp: %d\n", len(sql), len(exp))
// 		t.Errorf("Expected\n %s\nGot\n %s", exp, sql)
// 	}
// }

// func TestBuilder2(t *testing.T) {
// 	fmt.Println("\n\nTestBuilder ***")

// 	sqlAction := "tq.ID as QID,(select left(Qdata,50) from QuestionData where QID=q.ID and DataType=1 limit 1) As Tquestion ," +
// 		"(select s.Title from Subjects s where s.ID=t.SubjectID) as TSubject, q.QType, q.DifficultyLevel," +
// 		"tq.CorrectMarks,tq.NegativeMarks,tq.QCancelMarks,tq.seqno,q.ID,t.SubjectID,ts.SeqNo AS SeqNoSubject,tq.Addedon,tq.Addedby, " +
// 		"getquestionlanguages(q.ID) as Languages "
// 	sqlWhere := "ts.TestID=tq.TestID and t.SubjectID=ts.SubjectID and t.ID=q.TopicID and  " +
// 		"tq.QID=q.ID "
// 	sql := NewSQLBuilder().Select(sqlAction).
// 		From("tq,q,t,ts").Where(sqlWhere).OrderBy("tq.seqno", false).
// 		Table(Question{}, "tq").
// 		Table(Question{}, "q").
// 		Table(Question{}, "t").
// 		Table(Question{}, "ts").
// 		Build()

// 	fmt.Println(sql)

// 	exp := "select questions.ID, questiondata.QID from questions, questiondata where questions.ID=questiondata.QID and questions.TopicID=$1 order by questiondata.QID desc"
// 	if sql != exp {
// 		fmt.Printf("Sql: %d, Exp: %d\n", len(sql), len(exp))
// 		t.Errorf("Expected\n %s\nGot\n %s", exp, sql)
// 	}
// }

// func TestBuilderMultipleClause(t *testing.T) {
// 	fmt.Println("\n\nTestBuilderMultipleClause ***")

// 	sql := NewSQLBuilder().
// 		Select("q.ID").
// 		Select("qd.QID").
// 		From("q").
// 		From("qd").
// 		Where("q.ID=qd.QID").
// 		Where("q.TopicID=$1").
// 		WhereIntIN("q.ID", []int{2, 4}).
// 		OrderBy("qd.QID", false).
// 		OrderBy("q.ID", true).
// 		Table(&Question{}, "q").
// 		Table(&QuestionData{}, "qd").
// 		Limit(2).
// 		Build()

// 	exp := "select questions.ID, questiondata.QID from questions, questiondata where questions.ID=questiondata.QID and questions.TopicID=$1  and questions.ID=ANY('{2,4}'::integer[]) order by questiondata.QID asc, questions.ID desc limit 2"
// 	if sql != exp {
// 		fmt.Printf("Sql: %d, Exp: %d\n", len(sql), len(exp))
// 		t.Errorf("Expected\n %s\nGot\n %s", exp, sql)
// 	}
// }

// ------------------------------
//
// Benchmarkking
//
// ------------------------------

func BenchmarkConnect(b *testing.B) {
	initConnect()
}

func BenchmarkGetDbModelTable(b *testing.B) {
	exp := "questions"

	for n := 0; n < b.N; n++ {
		tag := getDbModelTable(&Question{})
		if tag != exp {
			b.Errorf("Expected %s,\nGot %s", exp, tag)
		}
	}

}

func BenchmarkPrepareSQL(b *testing.B) {
	exp := "Select questions.ID, questiondata.QID where questions.ID=questiondata.QID and questions.TopicID=$1;"
	sql := "Select q.ID, qd.QID where q.ID=qd.QID and q.TopicID=$1;"
	tables := make(map[string]string)
	tables["q"] = getDbModelTable(&Question{})
	tables["qd"] = getDbModelTable(&QuestionData{})

	for n := 0; n < b.N; n++ {
		sql = prepareSQL(sql, tables)
		if sql != exp {
			b.Errorf("Expected %s\nGot %s", exp, sql)
		}
	}
}

func BenchmarkBuilder(b *testing.B) {
	exp := "select questions.ID, questiondata.QID from questions, questiondata where questions.ID=questiondata.QID and questions.TopicID=$1 order by questiondata.QID desc"

	for n := 0; n < b.N; n++ {
		sql := NewSQLBuilder().Select("q.ID, qd.QID").
			From("q, qd").
			Where("q.ID=qd.QID and q.TopicID=$1").
			OrderBy("qd.QID", true).
			Table(&Question{}, "q").
			Table(&QuestionData{}, "qd").
			Build()

		if sql != exp {
			b.Errorf("Expected\n %s\nGot\n %s", exp, sql)
		}
	}
}

// func BenchmarkBuilderINOld(b *testing.B) {
// 	for n := 0; n < b.N; n++ {
// 		NewSQLBuilder().WhereIntINOld("q.ID", []int{2, 4})
// 	}
// }

// func BenchmarkBuilderIN(b *testing.B) {
// 	for n := 0; n < b.N; n++ {
// 		NewSQLBuilder().WhereIntIN("q.ID", []int{2, 4})
// 	}
// }

// func BenchmarkBuilderSprintf(b *testing.B) {
// 	for n := 0; n < b.N; n++ {
// 		fmt.Sprintf(dbconfig.sqlQueryByID, "a,b,c", "Table1", "Col1", "$")
// 	}
// }

// func BenchmarkBuilderReplace(b *testing.B) {
// 	for n := 0; n < b.N; n++ {
// 		//fmt.Sprintf(dbconfig.sqlQueryByID, "a,b,c", "Table1", "Col1", "$")
// 		strings.Replace(dbconfig.sqlProc, "%s", "proc", -1)
// 	}
// }

func BenchmarkBuilderMultipleClause(b *testing.B) {
	exp := "select questions.ID, questiondata.QID from questions, questiondata where questions.ID=questiondata.QID and questions.TopicID=$1  and questions.ID=ANY('{2,4}'::integer[]) order by questiondata.QID asc, questions.ID desc limit 2"

	for n := 0; n < b.N; n++ {
		sql := NewSQLBuilder().
			Select("q.ID").
			Select("qd.QID").
			From("q").
			From("qd").
			Where("q.ID=qd.QID").
			Where("q.TopicID=$1").
			WhereIntIN("q.ID", []int{2, 4}).
			OrderBy("qd.QID", false).
			OrderBy("q.ID", true).
			Table(&Question{}, "q").
			Table(&QuestionData{}, "qd").
			Limit(2).
			Build()

		if sql != exp {
			//fmt.Printf("Sql: %d, Exp: %d\n", len(sql), len(exp))
			b.Errorf("Expected\n %s\nGot\n %s", exp, sql)
		}
	}
}

// func BenchmarkRegexpCompile(b *testing.B) {
// 	k := "X"
// 	for n := 0; n < b.N; n++ {
// 		_, err := regexp.Compile("([^A-Za-z0-9])" + k + "([^A-Za-z0-9])")
// 		if err != nil {
// 			b.Fatal(err)
// 		}
// 	}
// }

// func BenchmarkRegexpReplace(b *testing.B) {
// 	k := "q"
// 	s := "Select q.ID, qd.QID where q.ID=qd.QID and q.TopicID=$1;"
// 	rx, err := regexp.Compile("([^A-Za-z0-9])" + k + "([^A-Za-z0-9])")

// 	for n := 0; n < b.N; n++ {
// 		s = rx.ReplaceAllString(s, fmt.Sprintf("${1}%s$2", "Question"))
// 		if err != nil {
// 			b.Fatal(err)
// 		}
// 	}
// }

func BenchmarkClose(b *testing.B) {
	dbClose()
}
