package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

type Oplog struct {
	Op   string       `json:"op"`
	NS   string      `json:"ns"`
	O map[string]interface{} `json:"o"`
}


func main() {
	jsonData := `
		{
			"op": "i",
			"ns": "test.student",
			"o": {
				"_id": "635b79e231d82a8ab1de863b",
				"name": "Selena Miller",
				"roll_no": 51,
				"is_graduated": false,
				"date_of_birth": "2000-01-30"
			},
			"p": "Q"
		}
	`
	convertOplogToSQL(jsonData)
} 
func convertOplogToSQL(oplog string) string {
	var oplogObject Oplog
	err := json.Unmarshal([]byte(oplog), &oplogObject)
	if err != nil {
		fmt.Printf("Could not parse oplog JSON\n%v",err)
		return ""
	}
	switch oplogObject.Op {
		case "i": 
			return parseInsertOplog(oplogObject)
		default:
			return ""
	}
}

func parseInsertOplog(oplog Oplog) string {
	fields := make([]string, 0)
	values := make([]string,0)
	for key,value := range oplog.O {
		fields = append(fields, key)
		switch v:= value.(type) {
		case int,float32,float64:
				values = append(values,fmt.Sprintf("%v",v))
			case bool:
				values = append(values,fmt.Sprintf("%t",v))
			case string:
				values = append(values,fmt.Sprintf("'%s'",v))
			default:
				fmt.Printf("%T",v)
		} 
	}
	
	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",oplog.NS,strings.Join(fields,","),strings.Join(values,","))
	return insertSQL
}