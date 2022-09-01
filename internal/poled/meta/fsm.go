package meta

import (
	"encoding/json"
	"io"
	"io/ioutil"

	"github.com/hashicorp/raft"
)

type raftLogOp int

const (
	raftLogOpAdd raftLogOp = iota
	raftLogOpDelete
	raftLogLeaderChange
)

type RaftLogData struct {
	Op             raftLogOp `json:"op"`
	Index          string    `json:"index"`
	Mapping        Mapping   `json:"mapping"`
	LeaderGrpcAddr string    `json:"leaderGrpcAddr"`
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

func (m *Meta) Apply(log *raft.Log) interface{} {
	logData := &RaftLogData{}
	if err := json.Unmarshal(log.Data, logData); err != nil {
		return nil
	}

	switch logData.Op {
	case raftLogOpAdd:
		m.Add(logData.Index, logData.Mapping)
	case raftLogOpDelete:
		m.Delete(logData.Index)
	case raftLogLeaderChange:
		m.UpdateLeader(logData.LeaderGrpcAddr)
	}

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
