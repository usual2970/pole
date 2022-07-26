package meta

import (
	"sync"
)

type FieldType string

const (
	FieldTypeNumeric FieldType = "numeric"
	FieldTypeText    FieldType = "text"
	FieldTypeUnknown FieldType = "unknown"
)

type Meta struct {
	MetaData map[string]map[string]FiledOptions `json:"meta_data"`
	sync.RWMutex
}

type FiledOptions struct {
	Type   FieldType `json:"type"`
	Option Option    `json:"option"`
}

type Option struct {
}

func (m *Meta) Exists(index string) bool {
	m.RLock()
	defer m.RUnlock()
	_, ok := m.getIndex(index)
	return ok
}

func (m *Meta) Get(index string) (map[string]FiledOptions, bool) {
	m.RLock()
	defer m.RUnlock()
	return m.getIndex(index)
}

func (m *Meta) getIndex(index string) (map[string]FiledOptions, bool) {

	rs, ok := m.MetaData[index]
	return rs, ok
}

func (m *Meta) Delete(index string) {
	m.Lock()
	defer m.Unlock()
	delete(m.MetaData, index)
}

func (m *Meta) Add(index string, fields map[string]FiledOptions) {
	m.Lock()
	defer m.Unlock()
	m.MetaData[index] = fields
}

func (m *Meta) All() map[string]map[string]FiledOptions {
	m.RLock()
	defer m.RUnlock()
	return m.MetaData
}
