package redis

import (
	"context"
	"log/slog"

	"github.com/m4n5ter/cache-killer/cache"
	rdb "github.com/redis/go-redis/v9"
)

type RedisCache struct {
	client          rdb.UniversalClient
	deathList       map[string]struct{}
	indestructibles map[string]struct{}
}

// TODO: Make the redis client configurable
func NewRedisCache(addrs ...string) *RedisCache {
	client := rdb.NewUniversalClient(&rdb.UniversalOptions{
		Addrs: addrs,
	})

	return &RedisCache{
		client,
		make(map[string]struct{}, 10),
		make(map[string]struct{}, 10),
	}
}

func (r *RedisCache) DeleteCache(keys ...string) error {
	_, err := r.client.Del(context.TODO(), keys...).Result()
	if err != nil {
		slog.Error("Failed to delete cache", "error", err)
		// Add the key to the death list
		for _, key := range keys {
			r.deathList[key] = struct{}{}
		}
		return err
	}
	return nil
}

func (r *RedisCache) DeathList() []string {
	keys := make([]string, 0, len(r.deathList))
	for key := range r.deathList {
		keys = append(keys, key)
	}
	return keys
}

func (r *RedisCache) EraseEvidence(keys ...string) {
	for _, key := range keys {
		delete(r.deathList, key)
	}
}

func (r *RedisCache) Indestructibles() []string {
	keys := make([]string, 0, len(r.indestructibles))
	for key := range r.indestructibles {
		keys = append(keys, key)
	}
	return keys
}

var _ cache.CacheKiller = (*RedisCache)(nil)
