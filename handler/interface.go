package handler

import "kvstorage/gen-go/kv"

type DataStorage interface {
	CheckKeyExist(key string) (bool, error)
	GetDataByKey(key string) (string, error)
	PutData(key string, data *kv.MapItem) error
	ListAllDataByListKey(keys []string) (results []*kv.ListData, missingkeys []string, err error)
	RemoveData(key string) error
	PutMultiData(listData []*kv.MapItem) error
}
