package cache

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/capstone-b4/capstone-go/internal/config"
	"github.com/redis/go-redis/v9"
)

var RedisClient *redis.Client

func ConnectRedis() {
	RedisClient = redis.NewClient(&redis.Options{
		Addr:     config.AppConfig.RedisAddr,
		Password: config.AppConfig.RedisPassword,
		DB:       config.AppConfig.RedisDB,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := RedisClient.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("Gagal connect Redis: %v", err)
	}

	log.Println("Connected to Redis")
}

func CloseRedis() {
	if RedisClient != nil {
		RedisClient.Close()
		log.Println("Redis disconnected")
	}
}

func GetCached[T any](ctx context.Context, key string) (*T, bool) {
	val, err := RedisClient.Get(ctx, key).Result()
	if err == redis.Nil {
		return nil, false
	}
	if err != nil {
		log.Printf("Redis get error: %v", err)
		return nil, false
	}

	var data T
	if err := json.Unmarshal([]byte(val), &data); err != nil {
		log.Printf("Redis unmarshal error: %v", err)
		return nil, false
	}

	return &data, true
}

func SetCache(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}

	err = RedisClient.Set(ctx, key, data, ttl).Err()
	if err != nil {
		log.Printf("Redis set error: %v", err)
	} else {
		log.Printf("Redis set success: key=%s, ttl=%v", key, ttl)
	}
	return err
}

func IncrementWithExpiry(ctx context.Context, key string, expiry time.Duration) (int, error) {
	pipe := RedisClient.Pipeline()
	incrCmd := pipe.Incr(ctx, key)
	expireCmd := pipe.Expire(ctx, key, expiry)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return 0, err
	}

	count, err := incrCmd.Result()
	if err != nil {
		return 0, err
	}

	_, err = expireCmd.Result()
	if err != nil {
		log.Printf("Expire failed for key %s: %v", key, err)
	}

	return int(count), nil
}

func GetRateLimitStatus(ctx context.Context, key string) (int, time.Duration, error) {
	count, err := RedisClient.Get(ctx, key).Int()
	if err == redis.Nil {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, err
	}

	ttl, err := RedisClient.TTL(ctx, key).Result()
	if err != nil {
		return count, 0, err
	}

	return count, ttl, nil
}
