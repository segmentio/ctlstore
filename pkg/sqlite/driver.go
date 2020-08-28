package sqlite

// TODO: support disabling auto checkpoint with modernc.org/sqlite
// import (
// 	"database/sql"
// 	"sync"

// 	"github.com/segmentio/go-sqlite3"
// 	_ "github.com/segmentio/go-sqlite3"
// )

// func init() {
// 	InitDriver()
// }

// var initDriverOnce sync.Once

// // InitDriver ensures that the sqlite3 driver is initialized
// func InitDriver() {
// 	initDriverOnce.Do(func() {
// 		sql.Register("sqlite3_with_autocheckpoint_off", &sqlite3.SQLiteDriver{
// 			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
// 				// This turns off automatic WAL checkpoints in the reader. Since the reader
// 				// can't do checkpoints as it's usually in read-only mode, checkpoints only
// 				// result in an error getting returned to callers in some circumstances.
// 				// As the Reflector is the only writer to the LDB, and it will continue to
// 				// run checkpoints, the WAL will stay nice and tidy.
// 				_, err := conn.Exec("PRAGMA wal_autocheckpoint = 0", nil)
// 				return err
// 			},
// 		})
// 	})
// }
