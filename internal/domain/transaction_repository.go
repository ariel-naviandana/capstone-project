type TransactionRepository interface {
	Create(ctx context.Context, tx *TransactionCreate) (string, error)
	GetByTxID(ctx context.Context, txID string) (*TransactionDetail, error)
	GetUserBalance(ctx context.Context, userID int64) (*UserBalance, error)
	GetUserTransactions(ctx context.Context, userID int64, limit int, offset int) ([]*TransactionDetail, error)
	GetAccountBalance(ctx context.Context, accountNo string) (*AccountBalance, error)
	GetAccountTransactions(ctx context.Context, accountNo string, limit int, offset int) ([]*TransactionDetail, error)
}
