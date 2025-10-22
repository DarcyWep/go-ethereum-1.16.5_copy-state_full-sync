package core

import (
	"bytes"
	"fmt"
	"path/filepath"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb/leveldb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/triedb"
)

const (
	dstPath = "/data/ethereum/state_snapshot"
	//batchSize         = 5000
	newLevelDBCache   = 8092
	newLevelDBHandles = 1024
	maxBatchSize      = 4 * 1024 * 1024 * 1024 // 4GB batch write
)

// tryStartCopyWorker 尝试启动一个状态复制 worker（只允许单例）
func (bc *BlockChain) tryStartCopyWorker() bool {
	block := bc.CurrentBlock() // 当前区块
	oldStateDB, err := state.New(block.Root, bc.statedb)
	if err != nil {
		log.RecordLog().Error("tryStartCopyWorker oldStateDB state.New failed", "err", err)
		return false
	}
	newStatePath := filepath.Join(dstPath, "snapshot_"+block.Number.String())
	//checkpointFile := filepath.Join(dstPath, "state_copy_checkpoint"+block.Number.String()+".hex")

	log.RecordLog().Info("Starting state copy for block %d root=%s", block.Number.Uint64(), block.Root.Hex())

	if err := copyStateBatchedFromRoot(oldStateDB, block.Root, newStatePath); err != nil {
		log.RecordLog().Info("[StateCopy] failed: %v", err)
	} else {
		log.RecordLog().Info("[StateCopy] success: block=%d root=%s", block.Number.Uint64(), block.Root.Hex())
	}

	return true
}

// ---------------------------------------------------------------
// 状态复制核心逻辑
// ---------------------------------------------------------------

func copyStateBatchedFromRoot(oldStateDB *state.StateDB, root common.Hash, newStatePath string) error {
	// 打开/创建新的链数据库
	newLevelDb, err := leveldb.New(newStatePath, newLevelDBCache, newLevelDBHandles, "state_snapshot", false)
	if err != nil {
		return fmt.Errorf("open newLevelDb: %w", err)
	}
	defer func() { _ = newLevelDb.Close() }()

	newTrieDB := triedb.NewDatabase(rawdb.NewDatabase(newLevelDb), DefaultConfig().triedbConfig(false))

	//newBCStateDB := state.NewDatabase(newTrieDB, nil)
	//newStateDB, err := state.New(types.EmptyRootHash, newBCStateDB)
	//if err != nil {
	//	return fmt.Errorf("create newStateDB: %w", err)
	//}

	// ================================= 开始复制 ===================================================
	start := time.Now()
	log.RecordLog().Info("Trie coping started", "root", root)

	tr, err := oldStateDB.Database().OpenTrie(root)

	if err != nil {
		return fmt.Errorf("oldStateDB OpenTrie: %w", err)
	}
	trieIt, err := tr.NodeIterator([]byte("")) // 从头开始
	if err != nil {
		return fmt.Errorf("trie nodeIterator: %w", err)
	}
	//it := trie.NewIterator(trieIt)
	// ===== 直接复制状态树 =====
	var accCount, codeCount, nodeCount int
	batch := newTrieDB.Disk().NewBatch()

	for trieIt.Next(true) {
		nodeCount++
		if trieIt.Error != nil {
			return fmt.Errorf("trie iterator error: %w", trieIt.Error())
		}

		// ----------- 存储节点数据 ---------------
		if trieIt.NodeBlob() == nil {
			continue
		}
		err := batch.Put(trieIt.Hash().Bytes(), trieIt.NodeBlob())
		if err != nil {
			return fmt.Errorf("state trie batch put error %w", err)
		}

		if batch.ValueSize() >= maxBatchSize {
			err := batch.Write()
			if err != nil {
				log.RecordLog().Error("state trie batch write error", "err", err)
			}
			batch.Reset()
		}

		if !trieIt.Leaf() { // 非叶子节点，继续寻找树上的其他节点
			continue
		}
		// ------------ 叶子节点, 继续往下搜索 ----------------
		// ------------ 解析账户 ---------------------
		var data types.StateAccount
		if err := rlp.DecodeBytes(trieIt.LeafBlob(), &data); err != nil {
			return fmt.Errorf("state trie decode state account error %w", err)
		}
		addrHash := trieIt.LeafKey()
		accCount++

		// -- ---------- 复制 code --------------
		if !bytes.Equal(data.CodeHash, types.EmptyCodeHash.Bytes()) { // Code不为空
			code := rawdb.ReadCode(oldStateDB.Database().TrieDB().Disk(), common.BytesToHash(data.CodeHash))
			if len(code) == 0 { // CodeHash 不为空，但是找不到Code
				return fmt.Errorf("state trie read code error %w", err)
			}
			rawdb.WriteCode(newTrieDB.Disk(), common.BytesToHash(data.CodeHash), code)
		}

		// -------- 复制 storage trie --------
		if bytes.Equal(data.Root.Bytes(), types.EmptyCodeHash.Bytes()) {
			continue
		}

		storageTr, err := oldStateDB.Database().OpenStorageTrie(root, common.Address{}, addrHash, data.Root, tr)
		if err != nil {
			return fmt.Errorf("failed to load storage trie %w", err)
		}
		storageTrieIt, err := storageTr.NodeIterator([]byte("")) // 从头开始
		if err != nil {
			return fmt.Errorf("failed to create stroage trie iterator %w", err)
		}

		// --------- 开始遍历 ----------
		for storageTrieIt.Next(true) {
			nodeCount++
			if storageTrieIt.NodeBlob() == nil {
				continue
			}

			err := batch.Put(storageTrieIt.Hash().Bytes(), storageTrieIt.NodeBlob())
			if err != nil {
				return fmt.Errorf("stroage trie batch put error %w", err)
			}
			if batch.ValueSize() >= maxBatchSize {
				err := batch.Write()
				if err != nil {
					log.RecordLog().Error("state trie batch write error", "err", err)
				}
				batch.Reset()
			}
		}
	}

	log.RecordLog().Info("✅ Done. Copied %d accounts, %d codes, %d nodes in %v",
		accCount, codeCount, nodeCount, time.Since(start))
	return nil
}
