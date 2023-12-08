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
			case int,int32,float32,float64:
				values = append(values,fmt.Sprintf("%v",v))
			case bool:
				values = append(values,fmt.Sprintf("%t",v))
			case primitive.ObjectID:
				values = append(values,fmt.Sprintf("'%s'",v.Hex()))
			case string:
				values = append(values,fmt.Sprintf("'%s'",v))
			default:
				fmt.Printf("%T",v)
		} 
	}
	
	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",oplog.NS,strings.Join(fields,","),strings.Join(values,","))
	return insertSQL
}

func parseUpdateOplog(oplog Oplog) string {
	fieldsToUpdate := oplog.O["diff"].(map[string]interface{})["u"].(map[string]interface{})
	updateSQL := fmt.Sprintf("UPDATE %s SET ",oplog.NS)
	for key,value := range fieldsToUpdate { 
		updateSQL += fmt.Sprintf("%s = ",key)
		switch v:= value.(type) {
			case int,int32,float32,float64:
				updateSQL += fmt.Sprintf("%v",v)
			case bool:
				updateSQL += fmt.Sprintf("%t",v)
			case primitive.ObjectID:
				updateSQL += fmt.Sprintf("'%s'",v.Hex())
			case string:
				updateSQL += fmt.Sprintf("'%s'",v)
			default:
				fmt.Printf("%T",v)
		} 
	}
	documentID := oplog.O2["_id"].(primitive.ObjectID).Hex()
	updateSQL += fmt.Sprintf(" WHERE _id = '%s'",documentID)
	return updateSQL
}