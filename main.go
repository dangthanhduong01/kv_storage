package main

import (
	"context"
	"database/sql"
	"fmt"
	"kvstorage/gen-go/kv"
	"log"
	"net"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/lib/pq"
)

type KeyValueStorage struct {
	data map[string]string
}

func NewKeyValueStorageHandler() *KeyValueStorage {
	handler := &KeyValueStorage{
		data: make(map[string]string),
	}
	//Init data
	// handler.data["1"] = "a"
	// handler.data["2"] = "b"
	// handler.data["3"] = "c"

	return handler
}

// Declare db connect as global variable
var db *sql.DB

func InitDB() {
	connStr := "user=postgres dbname=storage sslmode=disable password=1234 host=localhost port=5432"
	database, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Error opening database: %v", err)
	}
	fmt.Println("Connected!!!!")
	db = database
}

func checkKeyExist(key string) (bool, error) {
	var exists bool
	err := db.QueryRow("SELECT EXISTS (SELECT 1 FROM wallet WHERE key = $1)", key).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

// GetData implements kv.DataService.
func (p *KeyValueStorage) GetData(ctx context.Context, key string) (*kv.DataResult_, error) {
	// value, ok := p.data[key]
	var value string
	err := db.QueryRow("SELECT value FROM wallet WHERE key = $1", key).Scan(&value)
	if err != nil {
		return &kv.DataResult_{ErrorCode: kv.ErrorCode_NotFound}, nil
	}
	return &kv.DataResult_{
		ErrorCode: kv.ErrorCode_Good,
		Data:      &kv.MapItem{Key: key, Value: value},
	}, nil
}

func (p *KeyValueStorage) PutData(ctx context.Context, key string, data *kv.MapItem) (kv.ErrorCode, error) {
	exist, err := checkKeyExist(key)
	if err != nil {
		log.Fatalf("Error opening database: %v", err)
	}
	if exist {
		return kv.ErrorCode_DataExisted, nil
	}
	_, err = db.Exec(`INSERT INTO wallet (key, value) 
			VALUES ($1, $2) 
			ON CONFLICT (key) 
			DO UPDATE SET value = $2`, key, data.Value)
	if err != nil {
		return kv.ErrorCode_IterExceed, nil
	}
	return kv.ErrorCode_Good, nil
}

func (p *KeyValueStorage) GetListData(ctx context.Context, lskeys []string) (*kv.ListData, error) {
	var missingkeys []string
	query := "SELECT key, value FROM wallet WHERE key = ANY($1)"
	rows, err := db.Query(query, pq.Array(lskeys))
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

func (p *KeyValueStorage) RemoveData(ctx context.Context, key string) (kv.ErrorCode, error) {
	_, err := db.Exec("DELETE FROM wallet WHERE key = $1", key)
	if err != nil {
		return kv.ErrorCode_NotFound, nil
	}
	return kv.ErrorCode_Good, nil
}

func (p *KeyValueStorage) PutMultiData(ctx context.Context, listData []*kv.MapItem) (kv.ErrorCode, error) {
	// for _, item := range listData {
	// 	if _, exists := p.data[item.Key]; exists {
	// 		return kv.ErrorCode_DataExisted, nil
	// 	}
	// 	p.data[item.Key] = item.Value
	// }
	for i, item := range listData {
		ex, err := checkKeyExist(item.Key)
		if err != nil {
			return kv.ErrorCode_Unknown, nil
		}
		if ex {
			listData = append(listData[:i], listData[i+1:]...)
			// return kv.ErrorCode_DataExisted, nil
		}
	}
	tx, err := db.Begin()
	if err != nil {
		return kv.ErrorCode_Unknown, nil
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT INTO wallet (key, value) 
							VALUES ($1, $2)`)
	if err != nil {
		return kv.ErrorCode_IterExceed, nil
	}
	defer stmt.Close()

	for key, value := range listData {
		_, err := stmt.Exec(key, value)
		if err != nil {
			return kv.ErrorCode_Unknown, nil
		}
	}

	tx.Commit()
	return kv.ErrorCode_Good, nil
}

func main() {
	InitDB()
	// query.InitDB()
	handler := NewKeyValueStorageHandler()
	processor := kv.NewStorageServiceProcessor(handler) // Sử dụng StorageServiceProcessor

	transport, err := thrift.NewTServerSocket(net.JoinHostPort("localhost", "9090"))
	if err != nil {
		log.Fatalf("Error creating socket: %v", err)
	}

	transportFactory := thrift.NewTBufferedTransportFactory(8192)
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()

	server := thrift.NewTSimpleServer4(processor, transport, transportFactory, protocolFactory)

	fmt.Println("Starting the server on localhost:9090...")
	if err := server.Serve(); err != nil {
		log.Fatalf("Error starting server: %v", err)
	}
}
