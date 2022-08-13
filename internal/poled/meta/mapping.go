package meta


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
}

type Mapping struct {
	Properties map[string]FiledOptions
}
