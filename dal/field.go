package dal

type Field struct {
	Name       string                 `json:"name"`
	Type       string                 `json:"type"`
	Length     int                    `json:"length,omitempty"`
	Properties map[string]interface{} `json:"properties,omitempty"`
}
