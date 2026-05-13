package handler

import (
	"context"
	"kvstorage/db"
	"kvstorage/gen-go/kv"
)

type Handler struct {
	db db.DataStorage
}

func NewHandler(dsn string) *Handler {
	db, err := db.ConnectDb(context.Background(), dsn)
	if err != nil {
		panic(err)
	}
	return &Handler{db: *db}
}

// Check Key exist
func (s *Handler) CheckKeyExist(ctx context.Context, key string) (bool, error) {
	var exists bool
	err := s.db.CheckKeyExist(ctx, key)
	if err != nil {
		return false, err
	}
	return exists, nil
}

// Get value by key
func (s *Handler) GetData(ctx context.Context, key string) (*kv.DataResult_, error) {
	value, err := s.db.GetDataByKey(ctx, key)
	if err != nil {
		return &kv.DataResult_{ErrorCode: kv.ErrorCode_NotFound}, nil
	}
	return &kv.DataResult_{
		ErrorCode: kv.ErrorCode_Good,
		Data:      &kv.MapItem{Key: key, Value: value},
	}, nil
}

// Put data
func (s *Handler) PutData(ctx context.Context, key string, data *kv.MapItem) (kv.ErrorCode, error) {
	err := s.db.PutData(ctx, data.Key, data.Value)
	return kv.ErrorCode_Good, err
}

// List Data By Key
func (s *Handler) GetListData(ctx context.Context, keys []string) (*kv.ListData, error) {
	results, err := s.db.ListAllDataByListKey(ctx, keys)
	if err != nil {
		return nil, err
	}
	return results, nil
}

func (s *Handler) RemoveData(ctx context.Context, key string) (kv.ErrorCode, error) {
	err := s.db.RemoveData(ctx, key)
	if err != nil {
		return kv.ErrorCode_NotFound, err
	}
	return kv.ErrorCode_Good, err
}

// Hàm chèn hoặc cập nhật nhiều dữ liệu
func (s *Handler) PutMultiData(ctx context.Context, listData []*kv.MapItem) (kv.ErrorCode, error) {
	err := s.db.PutMultiData(ctx, listData)
	return kv.ErrorCode_Good, err

}
