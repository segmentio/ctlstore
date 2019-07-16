package ctldb

import (
	"database/sql"
	"database/sql/driver"
	"strings"
)

const LimiterDBSchemaUp = `
CREATE TABLE max_table_sizes (
	family_name VARCHAR(30) NOT NULL, /* limit pulled from validate.go */
	table_name  VARCHAR(50) NOT NULL, /* limit pulled from validate.go */
	warn_size_bytes BIGINT NOT NULL DEFAULT 0,
	max_size_bytes BIGINT NOT NULL DEFAULT 0,
	PRIMARY KEY (family_name, table_name)
);

CREATE TABLE max_writer_rates (
	writer_name VARCHAR(50) NOT NULL, /* limit pulled from validate.go */
	max_rows_per_minute BIGINT NOT NULL ,
	PRIMARY KEY (writer_name)
);

CREATE TABLE writer_usage (
	writer_name VARCHAR(50) NOT NULL, /* limit pulled from validate.go */
	bucket BIGINT NOT NULL,
	amount BIGINT NOT NULL ,
	PRIMARY KEY (writer_name, bucket)
); `

var CtlDBSchemaByDriver = map[string]string{
	"mysql": `

ALTER DATABASE CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;

CREATE TABLE families (
	id INTEGER AUTO_INCREMENT PRIMARY KEY,
	name VARCHAR(191) NOT NULL,
	UNIQUE KEY name (name)
);

CREATE TABLE mutators (
	writer VARCHAR(191) NOT NULL PRIMARY KEY,
	secret VARCHAR(255) NOT NULL,
	cookie BLOB(1024) NOT NULL,
	clock BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE ctlstore_dml_ledger (
	seq INTEGER AUTO_INCREMENT PRIMARY KEY,
	leader_ts DATETIME DEFAULT CURRENT_TIMESTAMP,
	statement MEDIUMTEXT NOT NULL
);

CREATE TABLE locks (
	id VARCHAR(191) NOT NULL PRIMARY KEY,
	clock BIGINT NOT NULL DEFAULT 0
);

INSERT INTO locks VALUES('ledger', 0);

` + LimiterDBSchemaUp,
	"sqlite3": `

CREATE TABLE families (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	name VARCHAR(191) NOT NULL UNIQUE
);

CREATE TABLE mutators (
	writer VARCHAR(191) NOT NULL PRIMARY KEY,
	secret VARCHAR(255),
	cookie BLOB(1024) NOT NULL,
	clock INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE ctlstore_dml_ledger (
	seq INTEGER PRIMARY KEY AUTOINCREMENT,
	leader_ts DATETIME DEFAULT CURRENT_TIMESTAMP,
	statement TEXT NOT NULL
);

CREATE TABLE locks (
	id VARCHAR(191) NOT NULL PRIMARY KEY,
	clock INTEGER NOT NULL DEFAULT 0
);

INSERT INTO locks VALUES('ledger', 0);
` + LimiterDBSchemaUp,
}

func InitializeCtlDB(db *sql.DB, driverFunc func(driver driver.Driver) (name string)) error {
	driverName := driverFunc(db.Driver())
	schema := CtlDBSchemaByDriver[driverName]
	statements := strings.Split(schema, ";")

	for _, statement := range statements {
		tsql := strings.TrimSpace(statement)
		if tsql == "" {
			continue
		}
		_, err := db.Exec(tsql)
		if err != nil {
			return err
		}
	}

	return nil
}
