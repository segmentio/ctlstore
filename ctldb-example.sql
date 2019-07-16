USE ctldb;

ALTER DATABASE CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;

DROP TABLE IF EXISTS families;
CREATE TABLE families (
	id INTEGER AUTO_INCREMENT PRIMARY KEY,
	name VARCHAR(191) NOT NULL,
	UNIQUE KEY name (name)
);

DROP TABLE IF EXISTS mutators;
CREATE TABLE mutators (
	writer VARCHAR(191) NOT NULL PRIMARY KEY,
	secret VARCHAR(255) NOT NULL,
	cookie BLOB(1024) NOT NULL,
	clock BIGINT NOT NULL DEFAULT 0
);

DROP TABLE IF EXISTS ctlstore_dml_ledger;
CREATE TABLE ctlstore_dml_ledger (
	seq INTEGER AUTO_INCREMENT PRIMARY KEY,
	leader_ts DATETIME DEFAULT CURRENT_TIMESTAMP,
	statement MEDIUMTEXT NOT NULL
);

DROP TABLE IF EXISTS locks;
CREATE TABLE locks (
	id VARCHAR(191) NOT NULL PRIMARY KEY,
	clock BIGINT NOT NULL DEFAULT 0
);

INSERT INTO locks VALUES('ledger', 0);

DROP TABLE IF EXISTS max_table_sizes;
CREATE TABLE max_table_sizes (
  family_name VARCHAR(30) NOT NULL, /* limit pulled from validate.go */
  table_name  VARCHAR(50) NOT NULL, /* limit pulled from validate.go */
  warn_size_bytes BIGINT NOT NULL DEFAULT 0,
  max_size_bytes BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (family_name, table_name)
);

DROP TABLE IF EXISTS max_writer_rates;
CREATE TABLE max_writer_rates (
  writer_name VARCHAR(50) NOT NULL, /* limit pulled from validate.go */
  max_rows_per_minute BIGINT NOT NULL ,
  PRIMARY KEY (writer_name)
);

DROP TABLE IF EXISTS writer_usage;
CREATE TABLE writer_usage (
  writer_name VARCHAR(50) NOT NULL, /* limit pulled from validate.go */
  bucket BIGINT NOT NULL,
  amount BIGINT NOT NULL ,
  PRIMARY KEY (writer_name, bucket)
);

