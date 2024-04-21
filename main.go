package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"

	"database/sql"
	"worker-go/metadata"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	_ "github.com/go-sql-driver/mysql"
)

type ResourceMetadata struct {
	Id   int    `json:"id"`
	Name string `json:"name"`
}

func connectToDB() *sql.DB {
	protocol := "tcp"
	port := "3306"
	username := os.Getenv("MYSQL_DB_ADMIN_USERNAME")
	password := os.Getenv("MYSQL_DB_ADMIN_PASSWORD")
	host := os.Getenv("MYSQL_DB_HOST")
	database := os.Getenv("MYSQL_DB_DATABASE")

	dsn := fmt.Sprintf("%s:%s@%s(%s:%s)/%s", username, password, protocol, host, port, database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Error connecting to the database: %s", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		log.Fatalf("Error pinging the database: %s", err)
	}

	fmt.Println("Connected to db")

	return db
}

func main() {
	err := godotenv.Load()

	if err != nil {
		log.Fatal("Error loading .env file")
	}

	fmt.Println("Going my great job :)")

	db := connectToDB()

	defer db.Close()

	kafkaBootstrapServers := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")

	kafkaProducerConfig := &kafka.ConfigMap{
		"bootstrap.servers": kafkaBootstrapServers,
	}
	kafkaConsumerConfig := &kafka.ConfigMap{
		"bootstrap.servers": kafkaBootstrapServers,
		"group.id":          "bmykhaylivvv-group",
		"auto.offset.reset": "latest",
	}
	kafkaProducer, _ := kafka.NewProducer(kafkaProducerConfig)
	consumer, err := kafka.NewConsumer(kafkaConsumerConfig)

	if err != nil {
		panic(err)
	}

	consumerTopic := "data-source-polls"
	err = consumer.SubscribeTopics([]string{consumerTopic}, nil)

	if err != nil {
		panic(err)
	}

	defer kafkaProducer.Close()

	for {
		var dataSource metadata.DataSource

		msg, err := consumer.ReadMessage(-1)

		if err == nil {
			if err := json.Unmarshal(msg.Value, &dataSource); err != nil {
				fmt.Printf("Error parsing JSON: %v\n", err)
			} else {
				metadata.SelectResourceCredentialsById(kafkaProducer, db, dataSource.Id, dataSource.Type, dataSource.DataStores)
			}
		} else {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
}
