package main

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"time"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

type ScriptArgs interface {
	ToScriptArgs() map[string]any
}

type SQLiteResultTransformer[T any] interface {
	FromSQLiteStmt(stmt *sqlite.Stmt) (*T, error)
}

type DB struct {
	pool *sqlitex.Pool
	fs   fs.FS
}

type ScriptArgsMap map[string]any

func (sam *ScriptArgsMap) ToScriptArgs() map[string]any {
	return *sam
}

func (fs *NodeState) ToScriptArgs() map[string]any {
	return map[string]any{
		":path":    fs.Path,
		":hash":    fs.Hash,
		":size":    fs.Size,
		":modtime": ToSQLTime(fs.ModTime),
		// ":timestamp": ToSQLTime(fs.Timestamp),
	}
}

func (fs NodeState) FromSQLiteStmt(stmt *sqlite.Stmt) (*NodeState, error) {
	hash := stmt.GetText("hash")
	size := stmt.GetInt64("size")
	return &NodeState{
		Path:    stmt.GetText("path"),
		Hash:    &hash,
		Size:    size,
		ModTime: FromSQLTime(stmt.GetInt64("modtime")),
		// Timestamp: FromSQLTime(stmt.GetInt64("timestamp")),
	}, nil
}

func makeExecOptions(args ScriptArgs) *sqlitex.ExecOptions {
	execOptions := &sqlitex.ExecOptions{}
	if args != nil {
		execOptions.Named = args.ToScriptArgs()
	}
	return execOptions
}

// TODO: handle processing results
// TODO: create non-Transient version w/ helper to create execOptions
// TODO: properly use contexts: https://pkg.go.dev/zombiezen.com/go/sqlite@v1.4.0#Conn.SetInterrupt
func (db *DB) ExecScriptTransient(script_path string, args ScriptArgs) error {
	context.TODO()
	conn, err := db.pool.Take(context.TODO())
	if err != nil {
		return err
	}
	defer db.pool.Put(conn)

	execOptions := makeExecOptions(args)
	logger.Trace(fmt.Sprintf("Calling script %q w/ params\n%+v", script_path, execOptions))
	return sqlitex.ExecuteTransientFS(conn, db.fs, script_path, execOptions)
}

// TODO: figure out how to break this into multiple lines
func ExecScript[T SQLiteResultTransformer[T]](db *DB, script_path string, args ScriptArgs) ([]*T, error) {
	var results []*T

	// TODO: create a method to get a conn, put context TODO inside that
	context.TODO()
	conn, err := db.pool.Take(context.TODO())
	if err != nil {
		return nil, err
	}
	defer db.pool.Put(conn)

	execOptions := makeExecOptions(args)

	resultFunc := func(stmt *sqlite.Stmt) error {
		var instance T
		result, err := instance.FromSQLiteStmt(stmt)
		if err != nil {
			return err
		}
		results = append(results, result)
		return nil
	}

	execOptions.ResultFunc = resultFunc

	logger.Trace(fmt.Sprintf("Calling script %q w/ params\n%+v", script_path, execOptions))

	err = sqlitex.ExecuteFS(conn, db.fs, script_path, execOptions)
	if err != nil {
		return nil, err
	}

	return results, nil
}

func ToSQLTime(t time.Time) int64 {
	return t.UnixMilli()
}

func FromSQLTime(t int64) time.Time {
	return time.UnixMilli(t)
}

func initDb(sqlite_dsn string) (*DB, error) {
	// conn, err := sqlite.OpenConn(sqlite_dsn, sqlite.OpenReadWrite, sqlite.OpenCreate)
	// conn, err := sqlite.OpenConn(sqlite_dsn)
	// if err != nil {
	// 	return nil, err
	// }

	pool, err := sqlitex.NewPool(sqlite_dsn, sqlitex.PoolOptions{})
	if err != nil {
		return nil, err
	}

	db := DB{
		pool: pool,
		fs:   os.DirFS("./sql/"),
	}

	if db.ExecScriptTransient("setup_db.sql", nil) != nil {
		return nil, err
	}

	return &db, nil
}

func (db *DB) UpsertFileState(fileState *NodeState) error {
	_, err := ExecScript[NodeState](db, "upsert_filestate.sql", fileState)
	return err
}

func (db *DB) GetFileStateByPath(path string) ([]*NodeState, error) {
	scriptArgs := ScriptArgsMap{":path": path}
	results, err := ExecScript[NodeState](db, "query_filestate.sql", &scriptArgs)
	return results, err
}
