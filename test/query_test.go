package main

import (
	"context"
	"kvstorage/handler"
	"testing"

	_ "github.com/lib/pq"
)

func Test_CheckKeyExist(t *testing.T) {
	ctx := context.Background()
	dbStorage := handler.NewHandler("user=postgres dbname=storage sslmode=disable password=1234 host=localhost port=5432")
	exists, err := dbStorage.CheckKeyExist(ctx, "key1")

	if err != nil {
		t.Fatalf("Error checking key existence: %v", err)
	}
	t.Logf("Key exists: %v\n", exists)
}
