package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

func main() {
	// Database connection string
	connStr := "user=postgres dbname=storage sslmode=disable password=1234 host=localhost port=5432"

	// Open the connection
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Error opening database: %v", err)
	}
	defer db.Close()

	// Ping the database to test the connection
	err = db.Ping()
	if err != nil {
		log.Fatalf("Error pinging database: %v", err)
	}

	fmt.Println("Successfully connected to PostgreSQL database!")
}
