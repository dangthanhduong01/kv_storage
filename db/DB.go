package db

import (
	"context"
	"fmt"
	"kvstorage/gen-go/kv"
	"strings"

	// "database/sql"
	// "fmt"
	// "log"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/lib/pq"
)

const connStr = "user=postgres s.dbname=storage sslmode=disable password=1234 host=localhost port=5432"

type DataStorage struct {
	db *pgxpool.Pool
}

func ConnectDb(ctx context.Context, dsn string) (*DataStorage, error) {

	if dsn == "" {
		dsn = connStr
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	// Configure pool settings if needed
	config.MaxConns = 20
	config.MinConns = 5

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}

	// err := pool.
	// log.Printf("Connected to the database \n")
	return &DataStorage{db: pool}, nil
}

func (d *DataStorage) Ping(ctx context.Context) error {
	return d.db.Ping(ctx)
}

func (d *DataStorage) CheckKeyExist(ctx context.Context, key string) error {
	var exists bool
	existsQuery := "SELECT EXISTS (SELECT 1 FROM wallet WHERE key = $1)"
	err := d.db.QueryRow(ctx, existsQuery, key).Scan(&exists)
	if err != nil {
		return err
	}
	return nil
}

func (d *DataStorage) GetDataByKey(ctx context.Context, key string) (string, error) {
	var value string
	query := "SELECT value FROM wallet WHERE key = $1"
	err := d.db.QueryRow(ctx, query, key).Scan(&value)
	if err != nil {
		return "", err
	}
	return value, nil
}

func (d *DataStorage) PutData(ctx context.Context, key string, value string) error {
	query := `INSERT INTO wallet (key, value) 
			  VALUES ($1, $2) 
			  ON CONFLICT (key) 
			  DO UPDATE SET value = $2`
	_, err := d.db.Exec(ctx, query, key, value)
	return err
}

func (d *DataStorage) ListAllDataByListKey(ctx context.Context, keys []string) (*kv.ListData, error) {
	query := "SELECT key, value FROM wallet WHERE key = ANY($1)"
	rows, err := d.db.Query(ctx, query, keys)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	data := []*kv.MapItem{}
	missingKeys := make([]string, 0)
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			missingKeys = append(missingKeys, key)
			continue
		}
		data = append(data, &kv.MapItem{Key: key, Value: value})
	}
	defer rows.Close()

	return &kv.ListData{
		Data:        data,
		ErrorCode:   kv.ErrorCode_Good,
		Missingkeys: missingKeys,
	}, nil
}

func (d *DataStorage) RemoveData(ctx context.Context, key string) error {
	query := "DELETE FROM wallet WHERE key = $1"
	_, err := d.db.Exec(ctx, query, key)
	return err
}

func (d *DataStorage) PutMultiData(ctx context.Context, listData []*kv.MapItem) error {
	if len(listData) == 0 {
		return nil
	}

	tx, err := d.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	valueStrings := make([]string, 0, len(listData))
	valueArgs := make([]interface{}, 0, len(listData)*2)

	i := 0
	for _, item := range listData {
		valueStrings = append(valueStrings, fmt.Sprintf("($%d, $%d)", i+1, i+2))
		valueArgs = append(valueArgs, item.Key, item.Value)
		i += 2
	}

	// Bulk Upsert
	query := fmt.Sprintf(`
		INSERT INTO wallet (key, value) 
		VALUES %s 
		ON CONFLICT (key) 
		DO UPDATE SET value = EXCLUDED.value`,
		strings.Join(valueStrings, ","))

	_, err = tx.Exec(ctx, query, valueArgs...)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}
