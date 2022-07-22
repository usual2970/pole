package poled

import (
	"sync"
)

type fieldType string

const (
	fieldTypeNumeric fieldType = "numeric"
	fieldTypeText    fieldType = "text"
	fieldTypeUnknown fieldType = "unknown"
)

type meta struct {
	MetaData map[string]map[string]filedOptions `json:"meta_data"`
	sync.RWMutex
}

type filedOptions struct {
	Type   fieldType `json:"type"`
	Option option    `json:"option"`
}

type option struct {
}

func (m *meta) Exists(index string) bool {
	m.RLock()
	defer m.RUnlock()
	_, ok := m.getIndex(index)
	return ok
}

func (m *meta) Get(index string) (map[string]filedOptions, bool) {
	m.RLock()
	defer m.RUnlock()
	return m.getIndex(index)
}

func (m *meta) getIndex(index string) (map[string]filedOptions, bool) {

	rs, ok := m.MetaData[index]
	return rs, ok
}

func (m *meta) Delete(index string) {
	m.Lock()
	defer m.Unlock()
	delete(m.MetaData, index)
}

func (m *meta) Add(index string, fields map[string]filedOptions) {
	m.Lock()
	defer m.Unlock()
	m.MetaData[index] = fields
}

func (m *meta) All() map[string]map[string]filedOptions {
	m.RLock()
	defer m.RUnlock()
	return m.MetaData
}
