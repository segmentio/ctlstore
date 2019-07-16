package ctldb

import "testing"

// This configuration comes from the docker-compose.yml file
const testCtlDBRawDSN = "ctldb:ctldbpw@tcp(localhost:3306)/ctldb"

func GetTestCtlDBDSN(t *testing.T) string {
	dsn, err := SetCtldbDSNParameters(testCtlDBRawDSN)
	if err != nil {
		if t == nil {
			panic(err)
		}
		t.Fatal(err)
	}
	return dsn
}
