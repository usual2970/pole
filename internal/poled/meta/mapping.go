package meta

import (
	"errors"
	"fmt"

	"github.com/blugelabs/bluge"
)

const (
	DefaultTextFieldOption        = bluge.Index | bluge.Store | bluge.SearchTermPositions | bluge.HighlightMatches | bluge.Sortable | bluge.Aggregatable
	DefaultNumericIndexingOptions = bluge.Index | bluge.Store | bluge.Sortable | bluge.Aggregatable
)

const IdentifierField = "_id"

var (
	ErrFieldNotFound         = errors.New("field not found")
	ErrNotSupportedFieldType = errors.New("no supported filed type")
	ErrFieldNotSetOption     = errors.New("field not set option")
)

type FieldType string

const (
	FieldTypeNumeric FieldType = "numeric"
	FieldTypeText    FieldType = "text"
	FieldTypeUnknown FieldType = "unknown"
)

type FiledOptions struct {
	Type   FieldType `json:"type"`
	Option Option    `json:"option"`
}

type Option struct {
	Index         bool `json:"index"`
	Store         bool `json:"store"`
	TermPositions bool `json:"term_positions"`
	Highlight     bool `json:"highlight"`
	Sortable      bool `json:"sortable"`
	Aggregatable  bool `json:"aggregatable"`
}

type Mapping struct {
	Properties map[string]FiledOptions
}

func (m *Mapping) MakeField(name string, value interface{}) (bluge.Field, error) {
	options, ok := m.Properties[name]
	if !ok {
		return nil, fmt.Errorf("%w:%s", ErrFieldNotFound, name)
	}

	switch options.Type {
	case FieldTypeNumeric:
		return m.MakeNumericField(name, value)
	case FieldTypeText:
		return m.MakeTextField(name, value)

	}

	return nil, ErrNotSupportedFieldType
}

func (m *Mapping) MakeNumericField(name string, value interface{}) (bluge.Field, error) {
	filed := bluge.NewNumericField(name, getNumericValue(value))
	fieldOptions, err := m.getFieldOptions(name)
	if err != nil {
		fieldOptions = DefaultNumericIndexingOptions
	}
	filed.FieldOptions = fieldOptions
	return filed, nil
}

func (m *Mapping) MakeTextField(name string, value interface{}) (bluge.Field, error) {
	filed := bluge.NewTextField(name, fmt.Sprintf("%v", value))
	fieldOptions, err := m.getFieldOptions(name)
	if err != nil {
		fieldOptions = DefaultTextFieldOption
	}
	filed.FieldOptions = fieldOptions
	return filed, nil
}

func (m *Mapping) getFieldOptions(name string) (bluge.FieldOptions, error) {
	options := m.Properties[name]
	var rs bluge.FieldOptions
	if options.Option.Index {
		rs |= bluge.Index
	}
	if options.Option.Store {
		rs |= bluge.Store
	}
	if options.Option.TermPositions {
		rs |= bluge.SearchTermPositions
	}
	if options.Option.Highlight {
		rs |= bluge.HighlightMatches
	}
	if options.Option.Sortable {
		rs |= bluge.Sortable
	}
	if options.Option.Aggregatable {
		rs |= bluge.Aggregatable
	}
	var err error
	if rs == 0 {
		err = ErrFieldNotSetOption
	}
	return rs, err
}

func getNumericValue(value interface{}) float64 {
	switch v := value.(type) {
	case int64:
		return float64(v)
	case float64:
		return v
	}
	return 0
}
