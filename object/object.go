package objects

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"

	"github.com/InsulaLabs/insi/ferry"
)

type Datum struct {
	Key   string
	Value []byte
}

func KeyGenerator(inProd bool, objectName string) func(uniqueID string) string {
	return func(uniqueID string) string {
		objectName = strings.ToLower(objectName)
		objectName = strings.ReplaceAll(objectName, ":", "_")
		if inProd {
			return "prod:" + objectName + ":" + uniqueID
		}
		return "dev:" + objectName + ":" + uniqueID
	}
}

func Encode[T any](key string, obj T) (Datum, error) {
	jsonBytes, err := json.Marshal(obj)
	if err != nil {
		return Datum{}, err
	}
	return Datum{
		Key:   key,
		Value: jsonBytes,
	}, nil
}

func Decode[T any](datum Datum) (T, error) {
	var obj T
	err := json.Unmarshal(datum.Value, &obj)
	return obj, err
}

type ByteStore interface {
	Get(context.Context, string) ([]byte, error)
	Set(context.Context, string, []byte) error
	Delete(context.Context, string) error
	SetNX(context.Context, string, []byte) error
	CompareAndSwap(context.Context, string, []byte, []byte) error
	IterateByPrefix(context.Context, string, int, int) ([]string, error)
}

type Store[T any] struct {
	logger     *slog.Logger
	ctrl       ByteStore
	keyGen     func(uniqueID string) string
	basePrefix string
}

func NewStore[T any](logger *slog.Logger, client *ferry.Ferry, useCache bool, inProd bool, objectName string) *Store[T] {
	if logger == nil || client == nil {
		panic("logger and client are required")
	}
	var ctrl ByteStore
	if useCache {
		ctrl = ferry.GetCacheController(client, []byte(""))
	} else {
		ctrl = ferry.GetValueController(client, []byte(""))
	}
	keyGen := KeyGenerator(inProd, objectName)
	return &Store[T]{
		logger:     logger,
		ctrl:       ctrl,
		keyGen:     keyGen,
		basePrefix: keyGen(""),
	}
}

func (x *Store[T]) StoreObject(uniqueID string, obj T) error {
	key := x.keyGen(uniqueID)
	jsonBytes, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	ctx := context.Background()
	return x.ctrl.Set(ctx, key, jsonBytes)
}

func (x *Store[T]) GetObject(uniqueID string) (T, error) {
	key := x.keyGen(uniqueID)
	ctx := context.Background()
	jsonBytes, err := x.ctrl.Get(ctx, key)
	var zero T
	if err != nil {
		return zero, err
	}
	var obj T
	if err = json.Unmarshal(jsonBytes, &obj); err != nil {
		return zero, err
	}
	return obj, nil
}

func (x *Store[T]) DeleteObject(uniqueID string) error {
	key := x.keyGen(uniqueID)
	ctx := context.Background()
	return x.ctrl.Delete(ctx, key)
}

func (x *Store[T]) SetNXObject(uniqueID string, obj T) error {
	key := x.keyGen(uniqueID)
	jsonBytes, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	ctx := context.Background()
	return x.ctrl.SetNX(ctx, key, jsonBytes)
}

func (x *Store[T]) CompareAndSwapObject(uniqueID string, oldObj T, newObj T) error {
	key := x.keyGen(uniqueID)
	oldBytes, err := json.Marshal(oldObj)
	if err != nil {
		return err
	}
	newBytes, err := json.Marshal(newObj)
	if err != nil {
		return err
	}
	ctx := context.Background()
	return x.ctrl.CompareAndSwap(ctx, key, oldBytes, newBytes)
}

func (x *Store[T]) ListUniqueIDs(uniqueIDPrefix string, offset, limit int) ([]string, error) {
	prefix := x.keyGen(uniqueIDPrefix)
	ctx := context.Background()
	keys, err := x.ctrl.IterateByPrefix(ctx, prefix, offset, limit)
	if err != nil {
		return nil, err
	}
	uniqueIDs := make([]string, len(keys))
	for i, key := range keys {
		uniqueIDs[i] = strings.TrimPrefix(key, x.basePrefix)
	}
	return uniqueIDs, nil
}
