package ctldb

import "net/url"

func SetCtldbDSNParameters(dsn string) (string, error) {
	var err error

	parameters := map[string]string{
		"collation": "utf8mb4_unicode_ci",
		"timeout":   "5s",
		"sql_mode":  "'NO_BACKSLASH_ESCAPES,ANSI_QUOTES'",
	}
	for name, value := range parameters {
		dsn, err = AddParameterToDSN(dsn, name, value)
		if err != nil {
			return "", err
		}
	}

	return dsn, nil
}

func AddParameterToDSN(dsn string, key string, value string) (string, error) {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return "", err
	}
	q := parsed.Query()
	q.Add(key, value)
	parsed.RawQuery = q.Encode()
	return parsed.String(), nil
}
