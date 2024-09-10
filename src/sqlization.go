package sqlization

//here be dragons

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"slices"
	"strings"

	"github.com/goaux/jsonify"
	_ "github.com/mattn/go-sqlite3"
)

const ESCAPE_CHAR = "\\"

func escape_sql_value(v string) string {
	//v = strings.ReplaceAll(v, ESCAPE_CHAR, fmt.Sprintf("%s%s", ESCAPE_CHAR, ESCAPE_CHAR)) //congratulations you are non-idempotent
	//v = strings.ReplaceAll(v, "%", "%%") //fmt.Sprintf("%s%%", ESCAPE_CHAR))
	//v = strings.ReplaceAll(v, "`", "%27")
	//v = strings.ReplaceAll(v, "\\", "%5C")
	//v = strings.ReplaceAll(v, "\n", "\\n") //"%0A")
	//v = strings.ReplaceAll(v, "\r", "%0D")
	//v = strings.ReplaceAll(v, "\t", "%09")
	v = strings.ReplaceAll(v, "\"", "\"\"")
	//v = strings.ReplaceAll(v, "'", "''")
	//v = strings.ReplaceAll(v, "_", "%5f") //fmt.Sprintf("%s_", ESCAPE_CHAR))
	//v = strings.ReplaceAll(v, "[", "[[]") //fmt.Sprintf("%s[", ESCAPE_CHAR))
	//v = strings.ReplaceAll(v, "]", )//fmt.Sprintf("%s]", ESCAPE_CHAR))
	//v = strings.ReplaceAll(v, "^") //fmt.Sprintf("%s^", ESCAPE_CHAR))

	return fmt.Sprintf("\"%s\"", v)
}

func unfold_interface(v interface{}) (value string, sql_type string) { //returns value, type
	switch v.(type) {
	case string:
		return escape_sql_value(v.(string)), "TEXT"
	default:
		//fmt.Println(value_type)
		//log.Fatal("Hit unhandled type in unfold")
		//return "", ""
		object_string, err := jsonify.String(v)
		if err != nil {
			log.Fatal("Failed stringifying strange json object (how??)")
		}
		return escape_sql_value(object_string), "TEXT" //temporary solution
	}
}
func create_db(db *sql.DB, table string, values string) (bool, error) {
	_, err := db.Query(fmt.Sprintf("select * from %s;", table))
	if err != nil {
		err = nil
		fmt.Println(fmt.Sprintf("create table %s%s;", table, values))
		_, err = db.Exec(fmt.Sprintf("create table %s%s;", table, values))
		if err != nil {
			return false, err
		}
	}
	return true, err
}

func add_missing_column(db *sql.DB, table string, values string) error {
	//global list of columns
	values = strings.TrimSuffix(values, ", ")
	//fmt.Println(fmt.Sprintf("select name from pragma_table_info('%s') order by cid;", table))
	rows, err := db.Query(fmt.Sprintf("select name from pragma_table_info('%s') order by cid;", table))
	//fmt.Sprintf("select name from pragma table_info('%s') order by cid;", table))
	if err != nil {
		return err
	}
	var columns []string
	for rows.Next() {
		var row string
		rows.Scan(&row)
		columns = append(columns, row)
	}
	for _, val := range strings.Split(values, ", ") {
		split_string := strings.Split(val, " ")
		key := split_string[0]
		//sql_string := split_string[1]
		if !slices.Contains(columns, key) {
			fmt.Println(fmt.Sprintf("alter table %s add %s;", table, val))
			_, err = db.Exec(fmt.Sprintf("alter table %s add %s;", table, val))
			if err != nil {
				return err
			}
		}
	}
	return err
}

//this function would make regular json work but its too much work to fix the edge cases
// func read_interior(file *os.File) []byte { //with jsonl we could use readline but just in case this chunks bytes
// 	var read_rune []rune
// 	// file.Read(read_byte)
// 	br := bufio.NewReader(file)
// 	for r, sz, err := br.ReadRune(); err == nil; r, sz, err = br.ReadRune() {
// 		if err == io.EOF {
// 			break
// 		} else {
// 			read_rune = append(read_rune, r)
// 			if r == '{' {
// 				//its over, edge case involves fully parsing json.
// 			}
// 		}
// 	}
// 	return []byte(string(read_rune))
// }

func Convert(table string, out string) {
	db, err := sql.Open("sqlite3", out)
	if err != nil {
		log.Fatal("Failed to open ", out, ":\n", err)
	}
	jsonFile, _ := os.Open("../tmp.json")
	defer jsonFile.Close()
	br := bufio.NewReader(jsonFile)

	db_exists := false
	for l, _, err := br.ReadLine(); err == nil; l, _, err = br.ReadLine() {
		db_key := ""
		column_key := ""
		var f interface{}
		json.Unmarshal(l, &f)
		if f != nil {
			m := f.(map[string]interface{})
			insert_str := ""
			value_str := ""
			for k, v := range m {
				//fmt.Println("heres one", reflect.TypeOf(v), " ", k)
				//fmt.Println(k, v)
				sql_val, sql_type := unfold_interface(v) //todo get the maps and objects to enter sql better
				insert_str = strings.Join([]string{insert_str, fmt.Sprintf("`%s`, ", k)}, "")
				if !db_exists { //unfortunately we cant cut this out in case the dataset throws us a curveball
					db_key = strings.Join([]string{db_key, fmt.Sprintf("`%s` %s, ", k, sql_type)}, "")
				}
				column_key = strings.Join([]string{column_key, fmt.Sprintf("%s %s, ", k, sql_type)}, "")
				value_str = strings.Join([]string{value_str, fmt.Sprintf("%s, ", sql_val)}, "")

			}
			insert_str = fmt.Sprintf("(%s)", strings.TrimSuffix(insert_str, ", "))
			value_str = fmt.Sprintf("(%s)", strings.TrimSuffix(value_str, ", "))
			if !db_exists {
				db_key = strings.TrimSuffix(db_key, ", ") //dont wrap yet
				db_exists, err = create_db(db, table, fmt.Sprintf("(%s)", db_key))
				if err != nil {
					log.Fatal("Could not detect or create table:\n", err)
				}
			}
			insert_str = fmt.Sprintf("insert into %s %s", table, insert_str)
			value_str = fmt.Sprintf("values%s", value_str)
		push:
			final_string := fmt.Sprintf("%s %s;", insert_str, value_str) // fmt.Sprintf("ESCAPE '%s'", ESCAPE_CHAR))
			//fmt.Println(final_string)
			_, err = db.Exec(final_string)
			if err != nil {
				if strings.HasPrefix(err.Error(), fmt.Sprintf("table %s has no column named ", table)) {
					err = add_missing_column(db, table, column_key) //intentional overwrite
					//fmt.Println("Have to add a column")
					if err != nil {
						log.Fatal("Failed to add a column after table initialization:\n", err)
					}
					goto push
				} else {
					log.Fatal("Failed to commit an object to sqlite db:\n", err)
				}
			}
		}
		//break
	}
}
