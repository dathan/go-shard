package goshard

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
)

// connection struct which holds the database handle
type Connection struct {
	DSN        string
	DBH        *sql.DB
	connparams *ConnectionParams
}

// connection params which tells you about the server to connect to
type ConnectionParams struct {
	Host        string
	Dbname      string
	User        string
	Password    string
	QueryParams string
	ShardId     uint
}

// alias :)
type Row map[string]interface{}

// Open a New Connection
func NewConnection(c *ConnectionParams) (error, *Connection) {

	i := &Connection{DSN: c.User + ":" + c.Password + "@tcp(" + c.Host + ":3600)/" + c.Dbname + "?" + c.QueryParams}

	logrus.Infof("DSN:%s", i.DSN)

	db, err := sql.Open("mysql", i.DSN)
	if err != nil {
		return err, nil
	}

	db.SetMaxOpenConns(5)

	i.connparams = c
	i.DBH = db

	return nil, i
}

// close the Connection Database Handle if one exists
func (c *Connection) Close() {
	if c.DBH != nil {
		c.DBH.Close()
		//c.DBH = nil
	}
}

// get Data in Row format
func (c *Connection) SelectRow(query string, args ...interface{}) (error, Row) {

	err, r := c.SelectAll(query, args...)
	if err != nil {
		return err, nil
	}

	var rw Row

	if r != nil && len(r) > 0 {
		rw = r[0]
	}

	return nil, rw

}

// get all data
func (c *Connection) SelectAll(query string, args ...interface{}) (error, []Row) {

	var rows []Row

	row, err := c.DBH.Query(query, args...)
	if err != nil {
		return err, nil
	}
	defer row.Close()

	colNames, err := row.Columns()
	if err != nil {
		return err, nil
	}

	//stack overflow optimization
	cols := make([]interface{}, len(colNames))
	colPtrs := make([]interface{}, len(colNames))
	for i := 0; i < len(colNames); i++ {
		colPtrs[i] = &cols[i]
	}

	for row.Next() {
		err = row.Scan(colPtrs...)
		if err != nil {
			return err, nil
		}

		var results Row = make(Row)

		for i, col := range cols {
			results[colNames[i]] = col
		}

		rows = append(rows, results)
	}
	return nil, rows

}

//BuildBinds
func BuildBinds(row *Row) ([]string, []string, []interface{}) {
	var cols_to_join []string
	var args []interface{}
	var binds []string

	for col, val := range *row {
		cols_to_join = append(cols_to_join, col)
		if val == "NOW()" || val == "CURDATE()" || val == "UUID_TO_BIN(UUID())" || val == "UUID()" {
			binds = append(binds, val.(string))
		} else {
			binds = append(binds, "?")
			args = append(args, val)
		}
	}

	return cols_to_join, binds, args

}

//Insert
func (c *Connection) Insert(table string, row *Row) (error, sql.Result) {

	var cols_to_join []string
	var args []interface{}
	var binds []string

	cols_to_join, binds, args = BuildBinds(row)

	query := "INSERT INTO " + table
	query += " (" + strings.Join(cols_to_join, ",") + ") VALUES(" + strings.Join(binds, ",") + ")"

	res, err := c.DBH.Exec(query, args...)
	if err != nil {
		return err, nil
	}

	return nil, res

}

//InsertIgnore
func (c *Connection) InsertIgnore(table string, row *Row) (error, sql.Result) {

	var cols_to_join []string
	var args []interface{}
	var binds []string

	cols_to_join, binds, args = BuildBinds(row)

	query := "INSERT IGNORE INTO " + table
	query += " (" + strings.Join(cols_to_join, ",") + ") VALUES(" + strings.Join(binds, ",") + ")"

	res, err := c.DBH.Exec(query, args...)
	if err != nil {
		return err, nil
	}

	return nil, res

}

//replace contents in a table with db.Row
func (c *Connection) Replace(table string, row *Row) (error, int64) {

	var cols_to_join []string
	var args []interface{}
	var binds []string

	cols_to_join, binds, args = BuildBinds(row)

	query := "REPLACE INTO " + table
	query += " (" + strings.Join(cols_to_join, ",") + ") VALUES(" + strings.Join(binds, ",") + ")"

	res, err := c.DBH.Exec(query, args...)
	if err != nil {
		return err, 0
	}

	ra, err := res.RowsAffected()
	if err != nil {
		return err, 0
	}

	return nil, ra

}

//replace contents in a table with db.Row
func (c *Connection) ReplaceStmt(table string, row *Row) (error, *sql.Stmt) {

	var cols_to_join []string
	//var args []interface{}
	var binds []string

	cols_to_join, binds, _ = BuildBinds(row)

	query := "REPLACE INTO " + table
	query += " (" + strings.Join(cols_to_join, ",") + ") VALUES(" + strings.Join(binds, ",") + ")"

	fmt.Printf("Q: %s\n", query)
	stmt, err := c.DBH.Prepare(query)
	if err != nil {

		return err, nil
	}

	return nil, stmt

}

//update the where clase with a row
func (c *Connection) Update(table string, row *Row, where string, where_bind []interface{}) (error, int64) {

	var args []interface{}
	var binds []string

	for col, val := range *row {

		if val == "NOW()" || val == "CURDATE()" {
			binds = append(binds, col+"="+val.(string)) // sprintf??
			continue
		}
		binds = append(binds, col+"=?")
		args = append(args, val)
	}

	for _, val := range where_bind {

		args = append(args, val)
	}

	query := "UPDATE " + table + " SET " + strings.Join(binds, ",") + " WHERE " + where

	stmt, err := c.DBH.Prepare(query)
	if err != nil {

		return err, -1
	}
	defer stmt.Close()

	res, err := stmt.Exec(args...)
	if err != nil {
		return err, -1
	}

	ra, err := res.RowsAffected()
	if err != nil {
		return err, -1
	}

	return nil, ra

}

//update the where clase with a row
func (c *Connection) Delete(table string, where string, where_bind []interface{}) (error, int64) {

	var args []interface{}

	for _, val := range where_bind {

		args = append(args, val)
	}

	query := "DELETE FROM " + table + " WHERE " + where

	stmt, err := c.DBH.Prepare(query)
	if err != nil {

		return err, -1
	}
	defer stmt.Close()

	res, err := stmt.Exec(args...)
	if err != nil {
		return err, -1
	}

	ra, err := res.RowsAffected()
	if err != nil {
		return err, -1
	}

	return nil, ra
}
