package cache

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupMiniRedis(t *testing.T) *miniredis.Miniredis {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)

	RedisClient = redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	t.Cleanup(func() {
		mr.Close()
	})

	return mr
}

func TestSetCache_And_GetCached(t *testing.T) {
	setupMiniRedis(t)
	ctx := context.Background()

	type testData struct {
		Name  string `json:"name"`
		Value int    `json:"value"`
	}

	t.Run("Set and Get success", func(t *testing.T) {
		data := testData{Name: "test", Value: 42}
		err := SetCache(ctx, "test:key1", data, 1*time.Minute)
		assert.NoError(t, err)

		result, found := GetCached[testData](ctx, "test:key1")
		assert.True(t, found)
		require.NotNil(t, result)
		assert.Equal(t, "test", result.Name)
		assert.Equal(t, 42, result.Value)
	})

	t.Run("Get cache miss", func(t *testing.T) {
		result, found := GetCached[testData](ctx, "nonexistent:key")
		assert.False(t, found)
		assert.Nil(t, result)
	})

	t.Run("Set with struct pointer", func(t *testing.T) {
		data := &testData{Name: "pointer", Value: 99}
		err := SetCache(ctx, "test:pointer", data, 1*time.Minute)
		assert.NoError(t, err)

		result, found := GetCached[testData](ctx, "test:pointer")
		assert.True(t, found)
		require.NotNil(t, result)
		assert.Equal(t, "pointer", result.Name)
	})
}

func TestInvalidateCache(t *testing.T) {
	setupMiniRedis(t)
	ctx := context.Background()

	// Set a value first
	err := SetCache(ctx, "to_delete", "some_value", 1*time.Minute)
	assert.NoError(t, err)

	// Verify it exists
	result, found := GetCached[string](ctx, "to_delete")
	assert.True(t, found)
	assert.NotNil(t, result)

	// Invalidate
	err = InvalidateCache(ctx, "to_delete")
	assert.NoError(t, err)

	// Verify it's gone
	result, found = GetCached[string](ctx, "to_delete")
	assert.False(t, found)
	assert.Nil(t, result)
}

func TestInvalidateCache_NonExistentKey(t *testing.T) {
	setupMiniRedis(t)
	ctx := context.Background()

	// Should not error when deleting a non-existent key
	err := InvalidateCache(ctx, "does_not_exist")
	assert.NoError(t, err)
}

func TestIncrementWithExpiry(t *testing.T) {
	mr := setupMiniRedis(t)
	ctx := context.Background()

	t.Run("First increment", func(t *testing.T) {
		mr.FlushAll()
		count, ttl, err := IncrementWithExpiry(ctx, "rate:test", 60*time.Second)
		assert.NoError(t, err)
		assert.Equal(t, 1, count)
		assert.Greater(t, ttl, 0)
	})

	t.Run("Multiple increments", func(t *testing.T) {
		mr.FlushAll()
		_, _, err := IncrementWithExpiry(ctx, "rate:multi", 60*time.Second)
		assert.NoError(t, err)

		count, _, err := IncrementWithExpiry(ctx, "rate:multi", 60*time.Second)
		assert.NoError(t, err)
		assert.Equal(t, 2, count)

		count, _, err = IncrementWithExpiry(ctx, "rate:multi", 60*time.Second)
		assert.NoError(t, err)
		assert.Equal(t, 3, count)
	})

	t.Run("Zero expiry defaults to 1 second", func(t *testing.T) {
		mr.FlushAll()
		count, _, err := IncrementWithExpiry(ctx, "rate:zero", 0)
		assert.NoError(t, err)
		assert.Equal(t, 1, count)
	})
}

func TestSetCache_MarshalError(t *testing.T) {
	setupMiniRedis(t)
	ctx := context.Background()

	// Channels cannot be marshalled to JSON
	err := SetCache(ctx, "bad_data", make(chan int), 1*time.Minute)
	assert.Error(t, err)
}

func TestGetCached_UnmarshalError(t *testing.T) {
	setupMiniRedis(t)
	ctx := context.Background()

	// Set raw invalid JSON data directly
	RedisClient.Set(ctx, "bad_json", "not-valid-json{{{", 1*time.Minute)

	type testData struct {
		Name string `json:"name"`
	}

	result, found := GetCached[testData](ctx, "bad_json")
	assert.False(t, found)
	assert.Nil(t, result)
}
