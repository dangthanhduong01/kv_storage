package handl

import (
	"database/sql"
	"fmt"
	"kvstorage/gen-go/kv"

	"github.com/lib/pq"
)

// Database connection string
// func connects.db() (*sql.s.db, error) {
// 	connStr := "user=postgres s.dbname=storage sslmode=disable password=1234 host=localhost port=5432"

// 	// Open the connection
// 	s.db, err := sql.Open("postgres", connStr)
// 	return s.db, err
// }

// type DataStorage interface {
// 	checkKeyExist(key string) (bool, error)
// 	getDataByKey(key string) (string, error)
// 	putData(key string, data *kv.MapItem) error
// 	listAllDataByListKey(keys []string) (results []*kv.ListData, missingkeys []string, err error)
// 	removeData(key string) error
// 	putMultiData(listData []*kv.MapItem) error
// }

type DatabaseStorage struct {
	db *sql.DB
}

// // Inits.db kết nối đến cơ sở dữ liệu PostgreSQL và gán kết quả cho biến s.db
func Initdb() (*DatabaseStorage, error) {
	connStr := "user=postgres s.dbname=storage sslmode=disable password=1234 host=localhost port=5432"
	database, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("error opening database: %v", err)
	}
	if err = database.Ping(); err != nil {
		return nil, fmt.Errorf("error connecting to the database: %v", err)
	}
	fmt.Println("Connected to the database")
	return &DatabaseStorage{db: database}, nil
}

// Query to retrieve all data from the storage table:
// SELECT key, value FROM wallet

// Check Key exist
func (s *DatabaseStorage) CheckKeyExist(key string) (bool, error) {
	var exists bool
	err := s.db.QueryRow("SELECT EXISTS (SELECT 1 FROM wallet WHERE key = $1)", key).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

// Get value by key
func (s *DatabaseStorage) getDataByKey(key string) (string, error) {
	var value string
	err := s.db.QueryRow("SELECT value FROM wallet WHERE key = $1", key).Scan(&value)
	if err != nil {
		return "", err
	}
	return value, nil
}

// Put data
func (s *DatabaseStorage) putData(key string, data *kv.MapItem) error {
	_, err := s.db.Exec(`INSERT INTO wallet (key, value) 
						VALUES ($1, $2) 
						ON CONFLICT (key) 
						DO UPDATE SET value = $2`, key, data.Value)
	return err
}

// List Data By Key
func (s *DatabaseStorage) listAllDataByListKey(keys []string) (*kv.ListData, error) {
	var missingkeys []string
	query := "SELECT key, value FROM wallet WHERE key = ANY($1)"
	rows, err := s.db.Query(query, pq.Array(keys))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var data []*kv.MapItem
	for rows.Next() {
		var key string
		var value string
		err := rows.Scan(&key, &value)
		if err != nil {
			missingkeys = append(missingkeys, key)
		}
		data = append(data, &kv.MapItem{Key: key, Value: value})
	}
	return &kv.ListData{
		ErrorCode:   kv.ErrorCode_Good,
		Data:        data,
		Missingkeys: missingkeys,
	}, nil
}

func (s *DatabaseStorage) removeData(key string) error {
	_, err := s.db.Exec("DELETE FROM wallet WHERE key = $1", key)
	return err
}

// Hàm chèn hoặc cập nhật nhiều dữ liệu
func (s *DatabaseStorage) putMultiData(listData []*kv.MapItem) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT INTO wallet (key, value) 
							VALUES ($1, $2)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for key, value := range listData {
		_, err := stmt.Exec(key, value)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}
