package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
    engine *engine_util.Engines
    config *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	kvPath := conf.DBPath + "/kv"
    raftPath := conf.DBPath + "/raft"
    kvEngine := engine_util.CreateDB(kvPath)
    raftEngine := engine_util.CreateDB(raftPath)
    
    store := StandAloneStorage{
        engine: engine_util.NewEngines(kvEngine, raftEngine, kvPath, raftPath),
        config: conf,
    }
    return &store
}

type StandAloneReader struct {
    kvTxn *badger.Txn
}

func (s *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error){
	value, err := engine_util.GetCFFromTxn(s.kvTxn,cf,key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return value,err
}

func (s *StandAloneReader) IterCF(cf string) engine_util.DBIterator{
	return engine_util.NewCFIterator(cf,s.kvTxn)
}

func (s *StandAloneReader) Close() {
	s.kvTxn.Discard()
	return
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	txn := s.engine.Kv.NewTransaction(false) // 只读事务
    return &StandAloneReader{kvTxn: txn}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	for _, b := range batch {
        switch data := b.Data.(type) {
        case storage.Put:
            if err := engine_util.PutCF(s.engine.Kv, data.Cf, data.Key, data.Value); err != nil {
                return err
            }
        case storage.Delete:
            if err := engine_util.DeleteCF(s.engine.Kv, data.Cf, data.Key); err != nil {
                return err
            }
        }
    }
    return nil
}
