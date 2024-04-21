package credentials

import (
	"database/sql"
	"log"
)

type MySqlCredentials struct {
	Host         string
	Port         string
	Username     string
	Password     string
	DatabaseName string
}

type MongoDbCredentials struct {
	Uri          string
	DatabaseName string
}

func GetMySqlCredentials(db *sql.DB, id int) (*MySqlCredentials, error) {
	var credentials MySqlCredentials

	query := "SELECT host, port, username, password, databaseName FROM MySqlCredentials WHERE resourceId = ?"
	err := db.QueryRow(query, id).Scan(&credentials.Host, &credentials.Port, &credentials.Username, &credentials.Password, &credentials.DatabaseName)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		log.Fatalf("Error querying for resource by ID: %s", err)
		return nil, err
	}

	return &credentials, nil
}

func GetMongoDbCredentials(db *sql.DB, id int) (*MongoDbCredentials, error) {
	var credentials MongoDbCredentials

	query := "SELECT uri, databaseName FROM MongoDbCredentials WHERE resourceId = ?"
	err := db.QueryRow(query, id).Scan(&credentials.Uri, &credentials.DatabaseName)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		log.Fatalf("Error querying for resource by ID: %s", err)
		return nil, err
	}

	return &credentials, nil
}
