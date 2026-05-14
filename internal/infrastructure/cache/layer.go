package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/capstone-b4/capstone-go/internal/infrastructure/observability"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/singleflight"
)

// ErrNotFound signals a "miss" the fetcher recognized as a real absence
// (e.g. account does not exist). The Layer caches this for negativeTTL so
// repeated probes for the same key don't all stampede the database.
var ErrNotFound = errors.New("cache: not found")

// FetchFunc is the source-of-truth lookup invoked on cache miss. It returns
// (value, ErrNotFound) for confirmed absence or (value, nil) for a hit.
type FetchFunc[T any] func(ctx context.Context) (*T, error)

// Layer is a per-resource cache with five composed behaviors:
//
//	L1: process-local LRU (microsecond hits, no network)
//	L2: Redis (millisecond hits, shared across replicas)
//	singleflight: collapses concurrent misses on the same key into one fetch
//	negative caching: remembers "not found" to stop probe-by-miss attacks
//	stale-while-revalidate: serves slightly-old data instantly while a
//	    background goroutine refreshes the entry past its softTTL but
//	    still inside hardTTL.
type Layer[T any] struct {
	name        string
	l1          *lru.Cache[string, *entry[T]]
	sf          singleflight.Group
	softTTL     time.Duration
	hardTTL     time.Duration
	negativeTTL time.Duration
}

// entry is the wire format for both L1 and L2. Storing the marshaled response
// bytes alongside the typed value lets handlers skip a re-marshal on hot hits.
type entry[T any] struct {
	Value     *T     `json:"v,omitempty"`
	Bytes     []byte `json:"b,omitempty"`
	SoftExpMs int64  `json:"s,omitempty"`
	Negative  bool   `json:"n,omitempty"`
}

func (e *entry[T]) isStale() bool {
	if e.SoftExpMs == 0 {
		return false
	}
	return time.Now().UnixMilli() > e.SoftExpMs
}

// NewLayer wires up an L1 LRU + an L2 Redis-backed cache with the given TTLs.
//
//	softTTL: data is "fresh" for this long. Past it, hits trigger a
//	         background refresh but still serve the stale value.
//	hardTTL: max time a value (or negative entry) lives before being evicted.
//	negativeTTL: hardTTL override for ErrNotFound entries (kept short on purpose).
func NewLayer[T any](name string, l1Size int, softTTL, hardTTL, negativeTTL time.Duration) (*Layer[T], error) {
	l1, err := lru.New[string, *entry[T]](l1Size)
	if err != nil {
		return nil, fmt.Errorf("layer %s: build LRU: %w", name, err)
	}
	if hardTTL <= 0 {
		hardTTL = 5 * time.Minute
	}
	if softTTL <= 0 || softTTL > hardTTL {
		softTTL = hardTTL / 2
	}
	if negativeTTL <= 0 {
		negativeTTL = 30 * time.Second
	}
	return &Layer[T]{
		name:        name,
		l1:          l1,
		softTTL:     softTTL,
		hardTTL:     hardTTL,
		negativeTTL: negativeTTL,
	}, nil
}

// GetOrFetch returns the value for key, populating cache layers on miss. It
// guarantees only one fetcher runs per key concurrently. Returns ErrNotFound
// (cached) when the upstream fetcher reported absence.
func (l *Layer[T]) GetOrFetch(ctx context.Context, key string, fetch FetchFunc[T]) (*T, []byte, error) {
	if e := l.tryHit(ctx, key); e != nil {
		if e.isStale() {
			l.refreshAsync(key, fetch)
		}
		return l.unwrap(e)
	}
	return l.fetchOnce(ctx, key, fetch)
}

func (l *Layer[T]) tryHit(ctx context.Context, key string) *entry[T] {
	if e, ok := l.l1.Get(key); ok {
		observability.CacheHitsTotal.WithLabelValues("l1").Inc()
		return e
	}
	observability.CacheMissesTotal.WithLabelValues("l1").Inc()

	if RedisClient == nil {
		return nil
	}
	raw, err := RedisClient.Get(ctx, l.redisKey(key)).Bytes()
	if err == redis.Nil {
		observability.CacheMissesTotal.WithLabelValues("redis").Inc()
		return nil
	}
	if err != nil {
		log.Warn().Err(err).Str("layer", l.name).Str("key", key).Msg("redis L2 read failed")
		return nil
	}
	observability.CacheHitsTotal.WithLabelValues("redis").Inc()

	var e entry[T]
	if uerr := json.Unmarshal(raw, &e); uerr != nil {
		log.Warn().Err(uerr).Str("layer", l.name).Str("key", key).Msg("redis L2 entry unmarshal failed")
		return nil
	}
	l.l1.Add(key, &e)
	return &e
}

func (l *Layer[T]) fetchOnce(ctx context.Context, key string, fetch FetchFunc[T]) (*T, []byte, error) {
	type result struct {
		entry *entry[T]
	}

	v, err, _ := l.sf.Do(key, func() (any, error) {
		// Re-check after coalescing: another goroutine may have populated.
		if e := l.tryHit(ctx, key); e != nil {
			return result{entry: e}, nil
		}
		val, ferr := fetch(ctx)
		if errors.Is(ferr, ErrNotFound) {
			e := &entry[T]{Negative: true}
			l.write(ctx, key, e, l.negativeTTL)
			return result{entry: e}, nil
		}
		if ferr != nil {
			return nil, ferr
		}
		bytes, merr := json.Marshal(val)
		if merr != nil {
			log.Warn().Err(merr).Str("layer", l.name).Str("key", key).Msg("marshal value bytes")
		}
		e := &entry[T]{
			Value:     val,
			Bytes:     bytes,
			SoftExpMs: time.Now().Add(l.softTTL).UnixMilli(),
		}
		l.write(ctx, key, e, l.hardTTL)
		return result{entry: e}, nil
	})
	if err != nil {
		return nil, nil, err
	}
	return l.unwrap(v.(result).entry)
}

func (l *Layer[T]) refreshAsync(key string, fetch FetchFunc[T]) {
	// Drop the request context so a fast client disconnect does not abort
	// the refresh that other callers will benefit from.
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _, _ = l.fetchOnce(ctx, key, fetch)
	}()
}

func (l *Layer[T]) unwrap(e *entry[T]) (*T, []byte, error) {
	if e.Negative {
		return nil, nil, ErrNotFound
	}
	return e.Value, e.Bytes, nil
}

// WriteThrough populates both cache tiers with a known-good value, e.g. after
// the worker successfully commits a transaction. Skips singleflight; trusts the caller.
func (l *Layer[T]) WriteThrough(ctx context.Context, key string, value *T) {
	if value == nil {
		return
	}
	bytes, err := json.Marshal(value)
	if err != nil {
		log.Warn().Err(err).Str("layer", l.name).Str("key", key).Msg("write-through marshal failed")
		return
	}
	e := &entry[T]{
		Value:     value,
		Bytes:     bytes,
		SoftExpMs: time.Now().Add(l.softTTL).UnixMilli(),
	}
	l.write(ctx, key, e, l.hardTTL)
}

// Invalidate removes the key from both tiers. Safe to call even if the key
// was never cached.
func (l *Layer[T]) Invalidate(ctx context.Context, key string) {
	l.l1.Remove(key)
	if RedisClient != nil {
		if err := RedisClient.Del(ctx, l.redisKey(key)).Err(); err != nil && err != redis.Nil {
			log.Warn().Err(err).Str("layer", l.name).Str("key", key).Msg("redis invalidate failed")
		}
	}
}

func (l *Layer[T]) write(ctx context.Context, key string, e *entry[T], ttl time.Duration) {
	l.l1.Add(key, e)
	if RedisClient == nil {
		return
	}
	raw, err := json.Marshal(e)
	if err != nil {
		log.Warn().Err(err).Str("layer", l.name).Str("key", key).Msg("encode entry for redis")
		return
	}
	if err := RedisClient.Set(ctx, l.redisKey(key), raw, ttl).Err(); err != nil {
		log.Warn().Err(err).Str("layer", l.name).Str("key", key).Msg("redis write failed")
	}
}

func (l *Layer[T]) redisKey(key string) string {
	return l.name + ":" + key
}

// Stats are exposed for tests and ad-hoc debugging.
type Stats struct {
	Name   string
	L1Size int
}

func (l *Layer[T]) Stats() Stats {
	return Stats{
		Name:   l.name,
		L1Size: l.l1.Len(),
	}
}
