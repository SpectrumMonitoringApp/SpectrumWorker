package metadata

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"worker-go/credentials"
	cryptoPackage "worker-go/crypto"
	"worker-go/producer"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type DataStore struct {
	Id   int    `json:"id"`
	Name string `json:"name"`
}

type DataSource struct {
	Id         int         `json:"dataSourceId"`
	Type       string      `json:"type"`
	DataStores []DataStore `json:"dataStores"`
}

func collectMySqlMetadata(kafkaProducer *kafka.Producer, db *sql.DB, resourceId int, dataStores []DataStore) {
	credentials, err := credentials.GetMySqlCredentials(db, resourceId)

	if err != nil {
		log.Fatalf("Failed to select resource by ID: %s", err)
	}

	username, err := cryptoPackage.Decrypt(credentials.Username)

	if err != nil {
		log.Fatalf("Failed to cryptoPackage.Decrypt(credentials.Username): %s", err)
		return
	}

	password, err := cryptoPackage.Decrypt(credentials.Password)

	if err != nil {
		log.Fatalf("Failed to cryptoPackage.Decrypt(credentials.Password): %s", err)
		return
	}

	host, err := cryptoPackage.Decrypt(credentials.Host)

	if err != nil {
		log.Fatalf("Failed to connect to cryptoPackage.Decrypt(credentials.Host): %s", err)
		return
	}

	port, err := cryptoPackage.Decrypt(credentials.Port)

	if err != nil {
		log.Fatalf("Failed to cryptoPackage.Decrypt(credentials.Port): %s", err)
		return
	}

	databaseName, err := cryptoPackage.Decrypt(credentials.DatabaseName)

	if err != nil {
		log.Fatalf("Failed to cryptoPackage.Decrypt(credentials.DatabaseName): %s", err)
		return
	}

	if credentials != nil {
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", username, password, host, port, databaseName)

		metadataDB, err := sql.Open("mysql", dsn)

		fmt.Printf("err: %s", err)

		if err != nil {
			log.Fatalf("Failed to connect to MySQL database: %s", err)
			return
		}
		defer metadataDB.Close()

		if err := metadataDB.Ping(); err != nil {
			log.Printf("Failed to establish a connection to MySQL database (invalid credentials or database unreachable): %s", err)

			return
		}

		wg := sync.WaitGroup{}

		for _, dataStore := range dataStores {
			wg.Add(1)
			go func(ds DataStore) {
				defer wg.Done()
				logMySqlResourceTableStats(kafkaProducer, metadataDB, resourceId, databaseName, ds)
			}(dataStore)
		}

		wg.Wait()

	} else {
		log.Printf("No credentials found for resource ID\n")
	}
}

func collectMongoDbMetadata(kafkaProducer *kafka.Producer, db *sql.DB, resourceId int, dataStores []DataStore) {
	credentials, err := credentials.GetMongoDbCredentials(db, resourceId)

	if err != nil {
		log.Fatalf("Failed to select resource by ID: %s", err)
	}

	uri, err := cryptoPackage.Decrypt(credentials.Uri)

	if err != nil {
		log.Fatalf("Failed to cryptoPackage.Decrypt(credentials.Username): %s", err)
		return
	}

	databaseName, err := cryptoPackage.Decrypt(credentials.DatabaseName)

	if err != nil {
		log.Fatalf("Failed to cryptoPackage.Decrypt(credentials.DatabaseName): %s", err)
		return
	}

	if credentials != nil {
		serverAPI := options.ServerAPI(options.ServerAPIVersion1)
		opts := options.Client().ApplyURI(uri).SetServerAPIOptions(serverAPI)
		client, err := mongo.Connect(context.TODO(), opts)

		if err != nil {
			log.Printf("Failed to connect to MongoDB database: %s", err)

			return
		}

		defer func() {
			if err = client.Disconnect(context.TODO()); err != nil {
				log.Printf("Failed to disconnect from MongoDB database: %s", err)

				return
			}
		}()

		var result bson.M

		if err := client.Database("admin").RunCommand(context.TODO(), bson.D{{"ping", 1}}).Decode(&result); err != nil {
			log.Printf("Failed to ping to MongoDB database: %s", err)

			return
		}

		fmt.Println("Pinged your deployment. You successfully connected to MongoDB!")

		wg := sync.WaitGroup{}

		for _, dataStore := range dataStores {
			wg.Add(1)
			go func(ds DataStore) {
				defer wg.Done()
				logMongoDbResourceTableStats(kafkaProducer, client, resourceId, databaseName, ds)
			}(dataStore)
		}

		wg.Wait()

	} else {
		log.Printf("No credentials found for resource ID\n")
	}
}

func SelectResourceCredentialsById(kafkaProducer *kafka.Producer, db *sql.DB, id int, resourceType string, dataStores []DataStore) {
	if resourceType == "mySql" {
		fmt.Println("It is MySQL")
		collectMySqlMetadata(kafkaProducer, db, id, dataStores)
	}

	if resourceType == "mongoDb" {
		fmt.Println("It is MongoDB")
		collectMongoDbMetadata(kafkaProducer, db, id, dataStores)
	}
}

func logMySqlResourceTableStats(kafkaProducer *kafka.Producer, metadataDB *sql.DB, resourceId int, databaseName string, dataStore DataStore) {
	queryCount := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", dataStore.Name)
	var dataStoreRecordsCount int64
	err := metadataDB.QueryRow(queryCount).Scan(&dataStoreRecordsCount)

	if err != nil {
		log.Printf("Failed to execute query: %s", err)

		return
	}

	log.Printf("%s table contains %d rows\n", dataStore.Name, dataStoreRecordsCount)

	var dataSizeMB float64
	querySize := `
		SELECT ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) 
		FROM information_schema.TABLES
		WHERE table_schema = ? AND table_name = ?`
	err = metadataDB.QueryRow(querySize, databaseName, dataStore.Name).Scan(&dataSizeMB)

	if err != nil {
		log.Printf("Failed to get data size: %s", err)

		return
	}

	log.Printf("%s table size is %.2f MB\n", dataStore.Name, dataSizeMB)

	var indexSizeMB float64
	indexSizeQuery := `
		SELECT 
			ROUND(((index_length) / 1024 / 1024),2) AS index_size_in_MB
			FROM information_schema.TABLES 
			WHERE table_schema = ? AND table_name = ?;`
	err = metadataDB.QueryRow(indexSizeQuery, databaseName, dataStore.Name).Scan(&indexSizeMB)

	if err != nil {
		log.Printf("Failed to get index size: %s", err)

		return
	}

	log.Printf("%s table index size is %.2f MB\n", dataStore.Name, indexSizeMB)

	recordCountData := producer.DataSourceRecordsCountMessage{
		ResourceId:  resourceId,
		DataStoreId: dataStore.Id,
		DataType:    "recordCount",
		Payload:     dataStoreRecordsCount,
	}
	dataSourceVolumeData := producer.DataSourceVolumeMessage{
		ResourceId:  resourceId,
		DataStoreId: dataStore.Id,
		DataType:    "dataStoreVolume",
		Payload:     dataSizeMB,
	}
	dataSourceIndexSizeData := producer.DataSourceVolumeMessage{
		ResourceId:  resourceId,
		DataStoreId: dataStore.Id,
		DataType:    "indexSize",
		Payload:     indexSizeMB,
	}

	producer.AddDataSourceInfoToMessageQueue(kafkaProducer, dataSourceVolumeData)
	producer.AddDataSourceInfoToMessageQueue(kafkaProducer, recordCountData)
	producer.AddDataSourceInfoToMessageQueue(kafkaProducer, dataSourceIndexSizeData)
}

func logMongoDbResourceTableStats(kafkaProducer *kafka.Producer, client *mongo.Client, resourceId int, databaseName string, dataStore DataStore) {
	dataStoreCollection := client.Database(databaseName).Collection(dataStore.Name)

	dataStoreRecordsCount, err := dataStoreCollection.CountDocuments(context.TODO(), bson.D{{}})

	if err != nil {
		log.Printf("Failed to dataStoreRecordsCount: %s", err)

		return
	}

	fmt.Printf("The %s collection in the %s database has %d documents.\n", dataStore.Name, databaseName, dataStoreRecordsCount)

	var collectionStats bson.M
	if err := client.Database(databaseName).RunCommand(context.TODO(), bson.D{
		{"collStats", dataStore.Name},
	}).Decode(&collectionStats); err != nil {
		log.Printf("Failed to get collection stats: %s", err)

		return
	}

	storageSize, ok1 := collectionStats["storageSize"].(int32)       // Type assertion for storageSize
	totalIndexSize, ok2 := collectionStats["totalIndexSize"].(int32) // Type assertion for totalIndexSize

	if !ok1 || !ok2 {
		log.Println("Failed to get collection or index size.")

		return
	}

	storageSizeMB := float64(storageSize) / (1024 * 1024)
	totalIndexSizeMB := float64(totalIndexSize) / (1024 * 1024)

	recordCountData := producer.DataSourceRecordsCountMessage{
		ResourceId:  resourceId,
		DataStoreId: dataStore.Id,
		DataType:    "recordCount",
		Payload:     dataStoreRecordsCount,
	}
	dataSourceVolumeData := producer.DataSourceVolumeMessage{
		ResourceId:  resourceId,
		DataStoreId: dataStore.Id,
		DataType:    "dataStoreVolume",
		Payload:     storageSizeMB,
	}
	dataSourceIndexSizeData := producer.DataSourceVolumeMessage{
		ResourceId:  resourceId,
		DataStoreId: dataStore.Id,
		DataType:    "indexSize",
		Payload:     totalIndexSizeMB,
	}

	producer.AddDataSourceInfoToMessageQueue(kafkaProducer, dataSourceVolumeData)
	producer.AddDataSourceInfoToMessageQueue(kafkaProducer, recordCountData)
	producer.AddDataSourceInfoToMessageQueue(kafkaProducer, dataSourceIndexSizeData)
}
