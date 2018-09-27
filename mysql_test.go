package goshard

import (
	"fmt"
	"testing"
)

func TestConnection(t *testing.T) {
	c := &ConnectionParams{
		Dbname:      "events",
		User:        "mon",
		Password:    "!mon!",
		QueryParams: "timeout=90s&collation=utf8mb4_unicode_ci&autocommit=true",
	}

	err, conn := NewConnection(c)

	if err != nil {
		fmt.Printf("ERROR!!: %s", err.Error())
		return
	}

	defer conn.Close()

	err, _ = conn.SelectRow("SELECT * FROM events ORDER BY id DESC limit 1")
	if err != nil {
		return
	}

	var r Row = make(Row)
	r["id"] = "2"
	err, _ = conn.InsertIgnore("test.unique_test", &r)
	if err != nil {
		return
	}

	var where_binds []interface{}
	where_binds = append(where_binds, r["id"])

	err, _ = conn.Update("test.unique_test", &Row{"create_date": "NOW()"}, "id=?", where_binds)
	//conn.Delete("test.unique_test", "id=?", where_binds)

	return

}
