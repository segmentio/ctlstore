package schema

type Table struct {
	Family    string     `json:"name"`
	Name      string     `json:"name"`
	Fields    [][]string `json:"fields"`
	KeyFields []string   `json:"keyFields"`
}
