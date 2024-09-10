package sqlization

import "testing"

func TestSqlization(t *testing.T) {
	Convert("cve", "test.db")
}
