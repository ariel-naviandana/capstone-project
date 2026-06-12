package cache

import (
	"context"
	"testing"
	"time"

	"github.com/capstone-b4/capstone-go/internal/config"
	"github.com/stretchr/testify/require"
)

func TestConnectRedisWhenOfflineKeepsApplicationAlive(t *testing.T) {
	RedisClient = nil
	BalanceLayer = nil
	TxLayer = nil
	config.AppConfig.RedisAddr = "127.0.0.1:1"
	config.AppConfig.RedisPassword = ""
	config.AppConfig.RedisDB = 0

	require.NotPanics(t, ConnectRedis)
	require.Nil(t, RedisClient)
	require.NotNil(t, BalanceLayer)
	require.NotNil(t, TxLayer)
}

func TestRedisHelpersBypassWhenClientNil(t *testing.T) {
	RedisClient = nil

	cached, ok := GetCached[string](context.Background(), "missing")
	require.Nil(t, cached)
	require.False(t, ok)

	require.Error(t, SetCache(context.Background(), "key", "value", time.Minute))
	require.Error(t, InvalidateCache(context.Background(), "key"))

	count, ttl, err := IncrementWithExpiry(context.Background(), "key", time.Minute)
	require.Error(t, err)
	require.Zero(t, count)
	require.Zero(t, ttl)
}
