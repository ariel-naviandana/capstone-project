package database

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShardIDForAccount(t *testing.T) {
	shardID, err := ShardIDForAccount("123-456-000001")
	assert.NoError(t, err)
	assert.Equal(t, 1, shardID)

	shardID, err = ShardIDForAccount("123-456-000002")
	assert.NoError(t, err)
	assert.Equal(t, 0, shardID)
}

func TestShardIDForAccount_Invalid(t *testing.T) {
	_, err := ShardIDForAccount("invalid")
	assert.Error(t, err)
}
