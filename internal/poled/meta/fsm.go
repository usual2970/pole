package meta

import (
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"

	"github.com/hashicorp/raft"

	poleLog "pole/internal/util/log"
)

var (
	ErrAlreadyLocked   = errors.New("already-locked")
	ErrAlreadyUnlocked = errors.New("already-unlocked")
)

type raftLogOp int

const (
	raftLogOpAdd raftLogOp = iota
	raftLogOpDelete
	raftLogLeaderChange
	raftLogLock
	raftLogUnlock
)

type RaftLogData struct {
	Op             raftLogOp `json:"op"`
	Index          string    `json:"index"`
	Mapping        Mapping   `json:"mapping"`
	LeaderGrpcAddr string    `json:"leaderGrpcAddr"`
	LockUri        string    `json:"lockUri,omitempty"`
}

func (l *RaftLogData) String() string {
	rs, _ := json.Marshal(l)
	return string(rs)
}

func NewAddLogDataCmd(index string, mapping Mapping) ([]byte, error) {
	return json.Marshal(&RaftLogData{
		Op:      raftLogOpAdd,
		Index:   index,
		Mapping: mapping,
	})
}

func NewDeleteLogDataCmd(index string) ([]byte, error) {
	return json.Marshal(&RaftLogData{
		Op:    raftLogOpDelete,
		Index: index,
	})
}
func NewBecomeLeaderCmd(leaderGrpcAddr string) ([]byte, error) {
	return json.Marshal(&RaftLogData{
		Op:             raftLogLeaderChange,
		LeaderGrpcAddr: leaderGrpcAddr,
	})
}

func NewLockCmd(lockUri string) ([]byte, error) {
	return json.Marshal(&RaftLogData{
		Op:      raftLogLock,
		LockUri: lockUri,
	})
}

func NewUnLockCmd(lockUri string) ([]byte, error) {
	return json.Marshal(&RaftLogData{
		Op:      raftLogUnlock,
		LockUri: lockUri,
	})
}

func (m *Meta) Apply(log *raft.Log) interface{} {
	lg := poleLog.WithField("module", "raftApply")
	logData := &RaftLogData{}
	if err := json.Unmarshal(log.Data, logData); err != nil {
		return nil
	}
	lg = lg.WithField("cmd", logData.String())
	var rs interface{}
	switch logData.Op {
	case raftLogOpAdd:
		m.Add(logData.Index, logData.Mapping)
	case raftLogOpDelete:
		m.Delete(logData.Index)
	case raftLogLeaderChange:
		m.UpdateLeader(logData.LeaderGrpcAddr)
	case raftLogLock:
		rs = m.DLock(logData.LockUri)
	case raftLogUnlock:
		rs = m.DUnlock(logData.LockUri)

	}
	lg.Info("appply success")
	return rs
}

func (m *Meta) Snapshot() (raft.FSMSnapshot, error) {
	m.RLock()
	meta := m.MetaData
	m.RUnlock()
	return newSnapshot(meta), nil
}

func (m *Meta) Restore(reader io.ReadCloser) error {
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}

	meta := make(map[string]Mapping)

	if err := json.Unmarshal(data, &meta); err != nil {
		return err
	}

	m.MetaData = meta

	return nil
}

func (m *Meta) UpdateLeader(leaderGrpcAddr string) {
	m.Lock()
	defer m.Unlock()
	m.LeaderGrpcAddr = leaderGrpcAddr
}

func (m *Meta) Leader() string {
	m.RLock()
	defer m.RUnlock()
	return m.LeaderGrpcAddr
}

func (m *Meta) DLock(lockUri string) error {
	m.RLock()
	locked := m.dlocked(lockUri)
	m.RUnlock()

	if locked {
		return ErrAlreadyLocked
	}

	m.Lock()
	defer m.Unlock()
	m.DLocked[lockUri] = struct{}{}
	return nil
}

func (m *Meta) DUnlock(lockUri string) error {
	m.RLock()
	locked := m.dlocked(lockUri)
	m.RUnlock()
	if !locked {
		return ErrAlreadyUnlocked
	}
	m.Lock()
	defer m.Unlock()
	delete(m.DLocked, lockUri)
	return nil
}

func (m *Meta) dlocked(lockUri string) bool {
	_, ok := m.DLocked[lockUri]
	return ok
}
