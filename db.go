package main

import (
	"fmt"
	"io/fs"
	"os"
	"time"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

type DB struct {
	conn *sqlite.Conn
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
func (db *DB) ExecScriptTransient(script_path string, args ScriptArgs) error {
	execOptions := makeExecOptions(args)
	logger.Debug(fmt.Sprintf("Calling script %q w/ params\n%+v", script_path, execOptions))
	return sqlitex.ExecuteTransientFS(db.conn, db.fs, script_path, execOptions)
}

func (db *DB) ExecScript(script_path string, args ScriptArgs) error {
	execOptions := makeExecOptions(args)
	logger.Debug(fmt.Sprintf("Calling script %q w/ params\n%+v", script_path, execOptions))
	return sqlitex.ExecuteFS(db.conn, db.fs, script_path, execOptions)
}

func ToSQLTime(t time.Time) int64 {
	return t.UnixMilli()
}

func initDb(sqlite_dsn string) (*DB, error) {
	// conn, err := sqlite.OpenConn(sqlite_dsn, sqlite.OpenReadWrite, sqlite.OpenCreate)
	conn, err := sqlite.OpenConn(sqlite_dsn)
	if err != nil {
		return nil, err
	}

	db := DB{
		conn: conn,
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
