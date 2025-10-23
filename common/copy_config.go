package common

import "math/big"

var (
	//record0 = big.NewInt(2380000) // 这是一个测试
	Record0          = big.NewInt(21000000)
	Record1          = big.NewInt(22600000)
	RecordStateBlock = big.NewInt(600000) // 超过2200W的每60W存一次
)

const (
	DstPath           = "/data/ethereum/state_snapshot"
	NewLevelDBCache   = 8092
	NewLevelDBHandles = 1024
	MaxBatchSize      = 4 * 1024 * 1024 * 1024 // 4GB batch write
)

const CopyRecordPath = "/data/ethereum/record.log"
