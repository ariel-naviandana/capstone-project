package database

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

func ShardIDForAccount(accountNo string) (int, error) {
	parts := strings.Split(accountNo, "-")
	suffix := parts[len(parts)-1]
	if suffix == "" {
		return 0, fmt.Errorf("invalid account_no for sharding: %s", accountNo)
	}

	n, err := strconv.Atoi(suffix)
	if err != nil {
		return 0, fmt.Errorf("invalid account_no suffix for sharding: %s", accountNo)
	}

	return n % 2, nil
}

func TransactionShardForAccount(accountNo string) (DBQueryInterface, int, error) {
	pool, shardID, err := TransactionShardPoolForAccount(accountNo)
	if err != nil {
		return nil, shardID, err
	}
	return pool, shardID, nil
}

func TransactionShardPoolForAccount(accountNo string) (*pgxpool.Pool, int, error) {
	shardID, err := ShardIDForAccount(accountNo)
	if err != nil {
		return nil, 0, err
	}
	if shardID >= len(ShardWritePools) || ShardWritePools[shardID] == nil {
		return nil, shardID, fmt.Errorf("transaction shard %d is not initialized", shardID)
	}
	return ShardWritePools[shardID], shardID, nil
}
