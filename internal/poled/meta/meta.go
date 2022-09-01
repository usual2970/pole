package meta

import (
	"sync"
)

type Meta struct {
	MetaData       map[string]Mapping `json:"metaData"`
	LeaderGrpcAddr string             `json:"leaderGrpcAddr"`
	sync.RWMutex
}

func NewMeta() *Meta {
	return &Meta{
		MetaData: make(map[string]Mapping),
	}
}

func (m *Meta) Exists(index string) bool {
	m.RLock()
	defer m.RUnlock()
	_, ok := m.getIndex(index)
	return ok
}

func (m *Meta) Get(index string) (Mapping, bool) {
	m.RLock()
	defer m.RUnlock()
	return m.getIndex(index)
}

func (m *Meta) getIndex(index string) (Mapping, bool) {

	rs, ok := m.MetaData[index]
	return rs, ok
}

func (m *Meta) Delete(index string) {
	m.Lock()
	defer m.Unlock()
	delete(m.MetaData, index)
}

func (m *Meta) Add(index string, fields Mapping) {
	m.Lock()
	defer m.Unlock()
	m.MetaData[index] = fields
}

func (m *Meta) All() map[string]Mapping {
	m.RLock()
	defer m.RUnlock()
	return m.MetaData
}
