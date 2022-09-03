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

func NewLockCmd() ([]byte, error) {
	return json.Marshal(&RaftLogData{
		Op: raftLogLock,
	})
}

func NewUnLockCmd() ([]byte, error) {
	return json.Marshal(&RaftLogData{
		Op: raftLogUnlock,
	})
}

func (m *Meta) Apply(log *raft.Log) interface{} {
	lg := poleLog.WithField("module", "raftApply")
	logData := &RaftLogData{}
	if err := json.Unmarshal(log.Data, logData); err != nil {
		return nil
	}
	lg = lg.WithField("cmd", logData.String())

	switch logData.Op {
	case raftLogOpAdd:
		m.Add(logData.Index, logData.Mapping)
	case raftLogOpDelete:
		m.Delete(logData.Index)
	case raftLogLeaderChange:
		m.UpdateLeader(logData.LeaderGrpcAddr)
	case raftLogLock:
		return m.DLock()
	case raftLogUnlock:
		return m.DUnlock()

	}
	lg.Info("appply success")
	return nil
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

func (m *Meta) DLock() error {
	m.RLock()
	locked := m.dlocked()
	m.RUnlock()

	if locked {
		return ErrAlreadyLocked
	}

	m.Lock()
	defer m.Unlock()
	m.DLocked = true
	return nil
}

func (m *Meta) DUnlock() error {
	m.RLock()
	locked := m.dlocked()
	m.RUnlock()
	if !locked {
		return ErrAlreadyUnlocked
	}
	m.Lock()
	defer m.Unlock()
	m.DLocked = false
	return nil
}

func (m *Meta) IsDlocked() bool {
	m.RLock()
	defer m.RUnlock()
	return m.dlocked()
}

func (m *Meta) dlocked() bool {
	return m.DLocked
}
