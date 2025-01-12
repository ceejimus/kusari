package main

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"time"

	"zombiezen.com/go/sqlite/sqlitex"
)

type DB struct {
	pool *sqlitex.Pool
	fs   fs.FS
}

type ScriptArgs interface {
	ToScriptArgs() map[string]any
}

func (fs *FileState) ToScriptArgs() map[string]any {
	return map[string]any{
		":path":      fs.Path,
		":hash":      fs.Hash,
		":size":      fs.Size,
		":modtime":   ToSQLTime(fs.ModTime),
		":timestamp": ToSQLTime(fs.Timestamp),
	}
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

func (db *DB) ExecScript(script_path string, args ScriptArgs) error {
	context.TODO()
	conn, err := db.pool.Take(context.TODO())
	if err != nil {
		return err
	}
	defer db.pool.Put(conn)

	execOptions := makeExecOptions(args)
	logger.Trace(fmt.Sprintf("Calling script %q w/ params\n%+v", script_path, execOptions))
	return sqlitex.ExecuteFS(conn, db.fs, script_path, execOptions)
}

func ToSQLTime(t time.Time) int64 {
	return t.UnixMilli()
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

func (db *DB) UpsertFileState(fileState *FileState) error {
	return db.ExecScript("upsert_filestate.sql", fileState)
}
