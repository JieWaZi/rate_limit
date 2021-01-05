package rate_limit

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/patrickmn/go-cache"
	"strconv"
	"time"
)

var DataStoreKeyValueNotFoundError = errors.New("data store key value not found")

type DataStore interface {
	// use "set-and-get" ,not "get-and-set"
	Add(key string, start, value int64) (int64, error)

	Get(key string, start int64) (int64, error)
}

// Redis存储
type RedisDataStore struct {
	Host      string
	Port      int
	Password  string // default ""
	DB        int    // default 0
	client    *redis.Client
	expireTTL time.Duration
}

func NewRedisDataStore(host string, port int, password string, db int, expireTTL time.Duration) *RedisDataStore {
	redis := &RedisDataStore{
		Host:      host,
		Port:      port,
		Password:  password,
		DB:        db,
		expireTTL: expireTTL,
	}
	redis.init()

	return redis
}
func (r *RedisDataStore) init() {
	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", r.Host, r.Port),
		Password: r.Password,
		DB:       r.DB,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_, err := client.Ping(ctx).Result()
	if err != nil {
		panic(fmt.Sprintf("redis Ping err:%v", err))
	}

	r.client = client
}
func (r *RedisDataStore) Add(key string, start, value int64) (int64, error) {
	fullKey := formatKey(key, start)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	count, err := r.client.IncrBy(ctx, fullKey, value).Result()
	if err != nil {
		return 0, err
	}
	_ = r.client.Expire(ctx, fullKey, r.expireTTL)

	return count, nil
}
func (r *RedisDataStore) Get(key string, start int64) (int64, error) {
	fullKey := formatKey(key, start)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	result, err := r.client.Get(ctx, fullKey).Result()
	if err != nil {
		if err == redis.Nil {
			return 0, DataStoreKeyValueNotFoundError
		}
		return 0, err
	}
	count, _ := strconv.ParseInt(result, 10, 64)

	return count, nil
}

func (r *RedisDataStore) TxPipeline(key, value string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	fn := func(tx *redis.Tx) error {
		// 先查询下当前watch监听的key的值
		_, err := tx.Get(ctx, key).Result()
		if err != nil && err != redis.Nil {
			return err
		} else if err == nil {
			return errors.New("key is existed")
		}

		// 如果key的值没有改变的话，Pipelined函数才会调用成功
		_, err = tx.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Incr(ctx, key).Result()
			return nil
		})

		return err
	}

	// 使用Watch监听一些Key, 同时绑定一个回调函数fn, 监听Key后的逻辑写在fn这个回调函数里面
	// 如果想监听多个key，可以这么写：client.Watch(fn, "key1", "key2", "key3")
	return r.client.Watch(ctx, fn, key)
}

func (r *RedisDataStore) TxPipeline1() error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	pipe := r.client.TxPipeline()
	pipe.Set(ctx, "test1", "1", 0)
	pipe.Incr(ctx, "test1")
	pipe.Set(ctx, "test2", 1, 0)
	pipe.Set(ctx, "test3", 1, 0)
	_, err := pipe.Exec(ctx)
	return err
}

// 内存存储
type MemoryDataStore struct {
	cache     cache.Cache
	expireTTL time.Duration
}

func NewMemoryDataStore(expireTTL time.Duration) *MemoryDataStore {
	return &MemoryDataStore{
		cache:     cache.Cache{},
		expireTTL: expireTTL,
	}
}
func (m *MemoryDataStore) Add(key string, start, value int64) (int64, error) {
	fullKey := formatKey(key, start)
	m.cache.Add(fullKey, value, m.expireTTL)
	result, err := m.cache.IncrementInt64(fullKey, value)
	if err != nil {
		if err.Error() == fmt.Sprintf("Item %s not found", fullKey) {
			return 0, DataStoreKeyValueNotFoundError
		}
		return 0, err
	}

	return result, nil
}
func (m *MemoryDataStore) Get(key string, start int64) (int64, error) {
	fullKey := formatKey(key, start)
	result, found := m.cache.Get(fullKey)
	if !found {
		return 0, DataStoreKeyValueNotFoundError
	}

	return result.(int64), nil
}

func formatKey(key string, start int64) string {
	return fmt.Sprintf("%s_%d", key, start)
}
