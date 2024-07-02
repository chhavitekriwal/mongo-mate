package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Oplog struct {
	Op   string
	NS   string
	O map[string]interface{}
	O2 map[string] interface{}
}


func main() {
	if err := godotenv.Load(); err != nil {
		log.Fatal("Error loading .env file")
	}

	mongoURI := os.Getenv("MONGO_URI")
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(mongoURI))
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := client.Disconnect(context.TODO()); err != nil {
			panic(err)
		}
	}()

	oplogCollection := client.Database("local").Collection("oplog.rs")
	filter := bson.M{"op": bson.M{"$ne": "c"}}
	cursor, err := oplogCollection.Find(context.TODO(), filter);
	if err != nil {
		panic(err)
	}

	var oplogs []Oplog
	if err = cursor.All(context.TODO(), &oplogs); err != nil {
		panic(err)
	}

	for _,oplog := range oplogs {
		fmt.Println(convertOplogToSQL(oplog))
	}
} 

func convertOplogToSQL(oplog Oplog) string {
	switch oplog.Op {
		case "i":
			return parseInsertOplog(oplog)
		case "u":
			return parseUpdateOplog(oplog)
		case "d":
			return parseDeleteOplog(oplog)
		default:
			return ""
	}
}

var schemaTableRelations = map[string]bool{}

func parseInsertOplog(oplog Oplog) string {
	var sql string

	if _,exists := schemaTableRelations[oplog.NS]; !exists {
		sql += createSchemaAndTableSQL(oplog)
		schemaTableRelations[oplog.NS] = true
	}
	fields := make([]string, 0)
	values := make([]string,0)
	
	for key,value := range oplog.O {
		fields = append(fields, key)
		values = append(values,getFieldValue(value))
	}
	sql += fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);",oplog.NS,strings.Join(fields,","),strings.Join(values,","))
	return sql;
}

func parseUpdateOplog(oplog Oplog) string {
	diffMap := oplog.O["diff"].(map[string]interface{})
	updateSQL := fmt.Sprintf("UPDATE %s SET",oplog.NS)
	for key := range diffMap { 
		fields := diffMap[key].(map[string]interface{})
		for field,value := range fields {
			switch key {
			case "u","i":
				updateSQL += fmt.Sprintf(" %s = %s,",field,getFieldValue(value))
			case "d":
				updateSQL += fmt.Sprintf(" %s = NULL,",field)
			}
		}		
	}
	updateSQL = updateSQL[:len(updateSQL)-1]
	updateSQL += getFilter(oplog.O2)+";"
	return updateSQL
}

func parseDeleteOplog(oplog Oplog) string {
	deleteSQL := fmt.Sprintf("DELETE FROM %s%s;", oplog.NS,getFilter(oplog.O))
	return deleteSQL
}
func getFieldValue(value interface{}) string {
	switch v:= value.(type) {
		case int,int32,float32,float64:
			return fmt.Sprintf("%v",v)
		case bool:
			return fmt.Sprintf("%t",v)
		case primitive.ObjectID:
			return fmt.Sprintf("'%s'",v.Hex())
		case string:
			return fmt.Sprintf("'%s'",v)
		case primitive.DateTime:
			return fmt.Sprintf("'%s'",v.Time().Format("2006-01-02 15:04:05-07:00"))
		default:
			return ""
	}
}

func getFilter(filterMap map[string]interface{}) string {
	documentID := filterMap["_id"].(primitive.ObjectID).Hex()
	return fmt.Sprintf(" WHERE _id = '%s'",documentID)
}

func getSQLDataType(value interface{}) string {
	switch value.(type) {
		case int,int32:
			return "integer"
		case float32,float64:
			return "float"
		case bool:
			return "boolean"
		case string,primitive.ObjectID:
			return "text"
		case primitive.DateTime:
			return "timestamptz"
		default:
			return "text"
	}
}

func createSchemaAndTableSQL(oplog Oplog) string {
	sql := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;\n", strings.Split(oplog.NS, ".")[0])
	types := make([]string,0)
	for field, value := range oplog.O {
		types = append(types, fmt.Sprintf("%s %s",field,getSQLDataType(value)))
	};
	sql += fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s);\n",oplog.NS,strings.Join(types, ","))
	return sql;
}