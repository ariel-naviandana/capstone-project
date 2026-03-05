package cache

import (
	"context"
	"encoding/json"
	"time"

	"github.com/capstone-b4/capstone-go/internal/config"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

var RedisClient *redis.Client

func ConnectRedis() {
	RedisClient = redis.NewClient(&redis.Options{
		Addr:     config.AppConfig.RedisAddr,
		Password: config.AppConfig.RedisPassword,
		DB:       config.AppConfig.RedisDB,
		PoolSize: 1000,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := RedisClient.Ping(ctx).Result()
	if err != nil {
		log.Fatal().
			Err(err).
			Str("addr", config.AppConfig.RedisAddr).
			Msg("Gagal connect Redis")
	}

	log.Info().
		Str("addr", config.AppConfig.RedisAddr).
		Msg("Connected to Redis")
}

func CloseRedis() {
	if RedisClient != nil {
		RedisClient.Close()
		log.Info().Msg("Redis disconnected")
	}
}

func GetCached[T any](ctx context.Context, key string) (*T, bool) {
	val, err := RedisClient.Get(ctx, key).Result()
	if err == redis.Nil {
		return nil, false
	}
	if err != nil {
		log.Warn().
			Err(err).
			Str("key", key).
			Msg("Redis get error")
		return nil, false
	}

	var data T
	if err := json.Unmarshal([]byte(val), &data); err != nil {
		log.Warn().
			Err(err).
			Str("key", key).
			Msg("Redis unmarshal error")
		return nil, false
	}

	return &data, true
}

func SetCache(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		log.Error().
			Err(err).
			Str("key", key).
			Msg("Gagal marshal value untuk Redis")
		return err
	}

	err = RedisClient.Set(ctx, key, data, ttl).Err()
	if err != nil {
		log.Warn().
			Err(err).
			Str("key", key).
			Dur("ttl", ttl).
			Msg("Redis set error")
	} else {
		log.Debug().
			Str("key", key).
			Dur("ttl", ttl).
			Msg("Redis set success")
	}
	return err
}

func InvalidateCache(ctx context.Context, key string) error {
	err := RedisClient.Del(ctx, key).Err()
	if err != nil {
		log.Warn().
			Err(err).
			Str("key", key).
			Msg("Gagal hapus Redis cache")
	} else {
		log.Debug().
			Str("key", key).
			Msg("Berhasil invalidasi Redis cache")
	}
	return err
}

func IncrementWithExpiry(ctx context.Context, key string, expiry time.Duration) (int, error) {
	pipe := RedisClient.Pipeline()
	incrCmd := pipe.Incr(ctx, key)
	expireCmd := pipe.Expire(ctx, key, expiry)

	_, err := pipe.Exec(ctx)
	if err != nil {
		log.Warn().
			Err(err).
			Str("key", key).
			Msg("Redis pipeline error in IncrementWithExpiry")
		return 0, err
	}

	count, err := incrCmd.Result()
	if err != nil {
		log.Warn().
			Err(err).
			Str("key", key).
			Msg("Gagal increment Redis key")
		return 0, err
	}

	_, err = expireCmd.Result()
	if err != nil {
		log.Warn().
			Err(err).
			Str("key", key).
			Dur("expiry", expiry).
			Msg("Expire failed for Redis key")
	}

	return int(count), nil
}

func GetRateLimitStatus(ctx context.Context, key string) (int, time.Duration, error) {
	count, err := RedisClient.Get(ctx, key).Int()
	if err == redis.Nil {
		return 0, 0, nil
	}
	if err != nil {
		log.Warn().
			Err(err).
			Str("key", key).
			Msg("Redis get error in GetRateLimitStatus")
		return 0, 0, err
	}

	ttl, err := RedisClient.TTL(ctx, key).Result()
	if err != nil {
		log.Warn().
			Err(err).
			Str("key", key).
			Msg("Redis TTL error in GetRateLimitStatus")
		return count, 0, err
	}

	return count, ttl, nil
}
