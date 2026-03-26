package queryvalue

import (
	"bytes"
	"encoding/json"
	"errors"
)

type Kind string

const (
	KindNull       Kind = "null"
	KindString     Kind = "string"
	KindNumber     Kind = "number"
	KindBoolean    Kind = "boolean"
	KindStringList Kind = "string_list"
	KindNumberList Kind = "number_list"
	KindBoolList   Kind = "bool_list"
)

type Value struct {
	kind         Kind
	stringValue  string
	numberValue  float64
	boolValue    bool
	stringValues []string
	numberValues []float64
	boolValues   []bool
}

func (v *Value) UnmarshalJSON(data []byte) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		*v = Value{kind: KindNull}
		return nil
	}

	var decoded any
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}

	switch typed := decoded.(type) {
	case string:
		*v = Value{kind: KindString, stringValue: typed}
		return nil
	case float64:
		*v = Value{kind: KindNumber, numberValue: typed}
		return nil
	case bool:
		*v = Value{kind: KindBoolean, boolValue: typed}
		return nil
	case []any:
		return v.fromSlice(typed)
	default:
		return errors.New("query param values must be scalar or homogeneous primitive arrays")
	}
}

func (v *Value) fromSlice(items []any) error {
	if len(items) == 0 {
		*v = Value{kind: KindStringList, stringValues: []string{}}
		return nil
	}

	switch items[0].(type) {
	case string:
		values := make([]string, 0, len(items))
		for _, item := range items {
			next, ok := item.(string)
			if !ok {
				return errors.New("query param arrays must contain only strings")
			}
			values = append(values, next)
		}
		*v = Value{kind: KindStringList, stringValues: values}
		return nil
	case float64:
		values := make([]float64, 0, len(items))
		for _, item := range items {
			next, ok := item.(float64)
			if !ok {
				return errors.New("query param arrays must contain only numbers")
			}
			values = append(values, next)
		}
		*v = Value{kind: KindNumberList, numberValues: values}
		return nil
	case bool:
		values := make([]bool, 0, len(items))
		for _, item := range items {
			next, ok := item.(bool)
			if !ok {
				return errors.New("query param arrays must contain only booleans")
			}
			values = append(values, next)
		}
		*v = Value{kind: KindBoolList, boolValues: values}
		return nil
	default:
		return errors.New("query param arrays must contain only primitive values")
	}
}

func (v Value) MarshalJSON() ([]byte, error) {
	switch v.kind {
	case KindNull:
		return []byte("null"), nil
	case KindString:
		return json.Marshal(v.stringValue)
	case KindNumber:
		return json.Marshal(v.numberValue)
	case KindBoolean:
		return json.Marshal(v.boolValue)
	case KindStringList:
		return json.Marshal(v.stringValues)
	case KindNumberList:
		return json.Marshal(v.numberValues)
	case KindBoolList:
		return json.Marshal(v.boolValues)
	default:
		return []byte("null"), nil
	}
}

func (v Value) Kind() Kind {
	return v.kind
}

func (v Value) String() (string, bool) {
	return v.stringValue, v.kind == KindString
}

func (v Value) Number() (float64, bool) {
	return v.numberValue, v.kind == KindNumber
}

func (v Value) Boolean() (val bool, ok bool) {
	return v.boolValue, v.kind == KindBoolean
}

func (v Value) StringList() ([]string, bool) {
	if v.kind != KindStringList {
		return nil, false
	}
	return append([]string(nil), v.stringValues...), true
}

func (v Value) NumberList() ([]float64, bool) {
	if v.kind != KindNumberList {
		return nil, false
	}
	return append([]float64(nil), v.numberValues...), true
}

func (v Value) BoolList() ([]bool, bool) {
	if v.kind != KindBoolList {
		return nil, false
	}
	return append([]bool(nil), v.boolValues...), true
}
