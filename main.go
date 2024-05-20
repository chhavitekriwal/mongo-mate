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
	var oplog Oplog
	err = oplogCollection.FindOne(context.TODO(), bson.D{{"op","u"}}).Decode(&oplog)
	fmt.Println(convertOplogToSQL(oplog))
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

func parseInsertOplog(oplog Oplog) string {
	fields := make([]string, 0)
	values := make([]string,0)
	for key,value := range oplog.O {
		fields = append(fields, key)
		values = append(values,getFieldValue(value))
	}	
	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",oplog.NS,strings.Join(fields,","),strings.Join(values,","))
	return insertSQL
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
	updateSQL += getFilter(oplog.O2)
	return updateSQL
}

func parseDeleteOplog(oplog Oplog) string {
	deleteSQL := fmt.Sprintf("DELETE FROM %s%s", oplog.NS,getFilter(oplog.O))
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