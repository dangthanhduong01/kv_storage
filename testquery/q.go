package main

import (
	"fmt"
	"kvstorage/handl"
	"log"

	_ "github.com/lib/pq"
)

// InitDB kết nối đến cơ sở dữ liệu PostgreSQL và gán kết quả cho biến db
// func InitDB() {
// 	connStr := "user=postgres dbname=storage sslmode=disable password=1234 host=localhost port=5432"
// 	database, err := sql.Open("postgres", connStr)
// 	if err != nil {
// 		log.Fatalf("Error opening database: %v", err)
// 	}
// 	fmt.Println("Connected!!!!")
// 	db1 = database
// }

// func checkKeyExist() {
// 	exists, err := handl.DataStorage.CheckKeyExist("key1")
// 	if err != nil {
// 		log.Fatalf("Error checking key existence: %v", err)
// 	}
// 	fmt.Printf("Key exists: %v\n", exists)
// }

func main() {
	dbStorage, err := handl.Initdb()
	if err != nil {
		log.Fatalf("Error initializing database: %v", err)
	}
	exists, err := dbStorage.CheckKeyExist("key1")

	if err != nil {
		log.Fatalf("Error checking key existence: %v", err)
	}
	fmt.Printf("Key exists: %v\n", exists)
}
