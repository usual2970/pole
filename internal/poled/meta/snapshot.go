package meta

import (
	"encoding/json"

	"github.com/hashicorp/raft"
)

type snapshot struct {
	MetaData map[string]Mapping `json:"metaData"`
}

func newSnapshot(meta map[string]Mapping) *snapshot {
	return &snapshot{MetaData: meta}
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	data, err := json.Marshal(s.MetaData)
	if err != nil {
		return err
	}

	if _, err := sink.Write(data); err != nil {
		return err
	}
	return nil
}

func (s *snapshot) Release() {}
