package core

import (
	"bytes"
	"fmt"
	"path/filepath"
	"strconv"
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

	log.RecordLog().Info("Starting state copy for block " + block.Number.String() + " root=" + block.Root.Hex())

	if err := copyStateBatchedFromRoot(oldStateDB, block.Root, newStatePath); err != nil {
		log.RecordLog().Error("[StateCopy] failed: " + err.Error())
	} else {
		log.RecordLog().Info("[StateCopy] success: block=" + block.Number.String() + ", root=" + block.Root.Hex())

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
		return fmt.Errorf("open newLevelDb: " + err.Error())
	}
	defer func() { _ = newLevelDb.Close() }()

	newTrieDB := triedb.NewDatabase(rawdb.NewDatabase(newLevelDb), DefaultConfig().triedbConfig(false))

	// ================================= 开始复制 ===================================================
	start := time.Now()
	log.RecordLog().Info("Trie coping started", "root", root)
	oldDiskDB := oldStateDB.Database().TrieDB().Disk()
	tr, err := oldStateDB.Database().OpenTrie(root)

	if err != nil {
		return fmt.Errorf("oldStateDB OpenTrie: " + err.Error())
	}
	trieIt, err := tr.NodeIterator([]byte("")) // 从头开始
	log.RecordLog().Info("Trie trie iterator started", "trieIt", trieIt, "err", err)
	if err != nil {
		return fmt.Errorf("trie nodeIterator: " + err.Error())
	}
	//it := trie.NewIterator(trieIt)
	// ===== 直接复制状态树 =====
	var accCount, codeCount, nodeCount int
	batch := newTrieDB.Disk().NewBatch()

	for trieIt.Next(true) {
		if trieIt.Error() != nil {
			return fmt.Errorf("trie iterator error: " + trieIt.Error().Error())
		}
		//dbvalue, err := oldDiskDB.Get(trieIt.Hash().Bytes())
		//if err != nil {
		//	return fmt.Errorf("old disk db get fail:" + err.Error())
		//}
		//if !bytes.Equal(dbvalue, trieIt.NodeBlob()) {
		//	return fmt.Errorf("dbvalue is not equal trieIt.NodeBlob().\ndbvalue: " + common.Bytes2Hex(dbvalue) + "\n" +
		//		"trieIt.NodeBlob(): " + common.Bytes2Hex(trieIt.NodeBlob()))
		//}

		// ----------- 存储节点数据 ---------------
		if len(trieIt.NodeBlob()) > 0 {
			err = batch.Put(trieIt.Hash().Bytes(), trieIt.NodeBlob())
			if err != nil {
				return fmt.Errorf("state trie batch put error " + err.Error())
			}
			nodeCount++

			if batch.ValueSize() >= maxBatchSize {
				err := batch.Write()
				if err != nil {
					return fmt.Errorf("state trie batch write error " + err.Error())
				}
				batch.Reset()
			}
		}

		if !trieIt.Leaf() { // 非叶子节点，继续寻找树上的其他节点
			continue
		}
		//log.RecordLog().Info("trieIt.Leaf()=" + strconv.FormatBool(trieIt.Leaf()) + ", " +
		//	"trieIt.Hash()=" + trieIt.Hash().String() + ", " +
		//	"trieIt.NodeBlob()=" + string(trieIt.NodeBlob()) + ", " +
		//	"trieIt.LeafKey()=" + string(trieIt.LeafKey()) + ", " +
		//	"trieIt.LeafBlob()=" + string(trieIt.LeafBlob()) + ", " +
		//	"trieIt.Path()=" + string(trieIt.Path()))
		// ------------ 叶子节点, 继续往下搜索 ----------------
		// ------------ 解析账户 ---------------------
		var data types.StateAccount
		if err := rlp.DecodeBytes(trieIt.LeafBlob(), &data); err != nil {
			return fmt.Errorf("state trie decode state account error " + err.Error())
		}
		addrHash := trieIt.LeafKey()
		accCount++

		// -- ---------- 复制 code --------------
		if !bytes.Equal(data.CodeHash, types.EmptyCodeHash.Bytes()) { // Code不为空
			codeCount++
			code := rawdb.ReadCode(oldDiskDB, common.BytesToHash(data.CodeHash))
			if len(code) == 0 { // CodeHash 不为空，但是找不到Code
				return fmt.Errorf("state trie read code error codehash is not nil, but code is nil")
			}
			rawdb.WriteCode(newTrieDB.Disk(), common.BytesToHash(data.CodeHash), code)
		}

		// -------- 复制 storage trie --------
		if bytes.Equal(data.Root.Bytes(), types.EmptyRootHash.Bytes()) {
			continue
		}

		storageTr, err := oldStateDB.Database().OpenStorageTrie(root, common.Address{}, addrHash, data.Root, tr)
		if err != nil {
			return fmt.Errorf("failed to load storage trie " + err.Error())
		}
		storageTrieIt, err := storageTr.NodeIterator([]byte("")) // 从头开始
		if err != nil {
			return fmt.Errorf("failed to create stroage trie iterator " + err.Error())
		}

		// --------- 开始遍历 ----------
		for storageTrieIt.Next(true) {
			nodeCount++
			if storageTrieIt.Error() != nil {
				return fmt.Errorf("trie iterator error: " + trieIt.Error().Error())
			}
			if len(storageTrieIt.NodeBlob()) > 0 {
				err := batch.Put(storageTrieIt.Hash().Bytes(), storageTrieIt.NodeBlob())
				if err != nil {
					return fmt.Errorf("stroage trie batch put error " + err.Error())
				}
				if batch.ValueSize() >= maxBatchSize {
					err := batch.Write()
					if err != nil {
						return fmt.Errorf("storage trie batch write error " + err.Error())
					}
					batch.Reset()
				}
			}
		}
	}

	// 最后将所有的batch
	if batch.ValueSize() > 0 {
		err = batch.Write()
		if err != nil {
			log.RecordLog().Error("state trie batch write error", "err", err)
		}
		batch.Reset()
	}

	log.RecordLog().Info("✅ Done. Copied " + strconv.Itoa(accCount) + " accounts, " +
		"" + strconv.Itoa(codeCount) + " codes, " + strconv.Itoa(nodeCount) + " nodes in " + time.Since(start).String())

	// 可选：在这里尝试在 newTrieDB 上 OpenTrie(root) 以验证
	newBCStateDB := state.NewDatabase(newTrieDB, nil)
	//newStateDB, err := state.New(types.EmptyRootHash, newBCStateDB)
	//if err != nil {
	//	return fmt.Errorf("create newStateDB: " + err.Error())
	//}
	if _, err := newBCStateDB.OpenTrie(root); err != nil {
		log.RecordLog().Error("verify new trie open failed " + err.Error())
		return fmt.Errorf("verify new trie open failed: " + err.Error())
	} else {
		log.RecordLog().Info("verify new trie open success")
		//balance := newStateDB.GetBalance(common.HexToAddress("0xb4bfEfC30A60B87380e377F8B96CC3b2E65A8F64"))
		//log.RecordLog().Info("get account(0xb4bfEfC30A60B87380e377F8B96CC3b2E65A8F64) balance: " + balance.String())
	}
	return nil
}
