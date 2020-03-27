package main

import (
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	sppb "google.golang.org/genproto/googleapis/spanner/v1"
)

func DecodeRow(row *spanner.Row) ([]string, error) {
	columns := make([]string, row.Size())
	for i := 0; i < row.Size(); i++ {
		var column spanner.GenericColumnValue
		if err := row.Column(i, &column); err != nil {
			return nil, err
		}
		decoded, err := DecodeColumn(column)
		if err != nil {
			return nil, err
		}
		columns[i] = decoded
	}
	return columns, nil
}

func DecodeColumn(column spanner.GenericColumnValue) (string, error) {
	// Allowable types: https://cloud.google.com/spanner/docs/data-types#allowable-types
	switch column.Type.Code {
	case sppb.TypeCode_ARRAY:
		var decoded []string
		switch column.Type.GetArrayElementType().Code {
		case sppb.TypeCode_BOOL:
			var vs []spanner.NullBool
			if err := column.Decode(&vs); err != nil {
				return "", err
			}
			if vs == nil {
				return "NULL", nil
			}
			for _, v := range vs {
				decoded = append(decoded, nullBoolToString(v))
			}
		case sppb.TypeCode_BYTES:
			var vs [][]byte
			if err := column.Decode(&vs); err != nil {
				return "", err
			}
			if vs == nil {
				return "NULL", nil
			}
			for _, v := range vs {
				decoded = append(decoded, nullBytesToString(v))
			}
		case sppb.TypeCode_FLOAT64:
			var vs []spanner.NullFloat64
			if err := column.Decode(&vs); err != nil {
				return "", err
			}
			if vs == nil {
				return "NULL", nil
			}
			for _, v := range vs {
				decoded = append(decoded, nullFloat64ToString(v))
			}
		case sppb.TypeCode_INT64:
			var vs []spanner.NullInt64
			if err := column.Decode(&vs); err != nil {
				return "", err
			}
			if vs == nil {
				return "NULL", nil
			}
			for _, v := range vs {
				decoded = append(decoded, nullInt64ToString(v))
			}
		case sppb.TypeCode_STRING:
			var vs []spanner.NullString
			if err := column.Decode(&vs); err != nil {
				return "", err
			}
			if vs == nil {
				return "NULL", nil
			}
			for _, v := range vs {
				decoded = append(decoded, nullStringToString(v))
			}
		case sppb.TypeCode_TIMESTAMP:
			var vs []spanner.NullTime
			if err := column.Decode(&vs); err != nil {
				return "", err
			}
			if vs == nil {
				return "NULL", nil
			}
			for _, v := range vs {
				decoded = append(decoded, nullTimeToString(v))
			}
		case sppb.TypeCode_DATE:
			var vs []spanner.NullDate
			if err := column.Decode(&vs); err != nil {
				return "", err
			}
			if vs == nil {
				return "NULL", nil
			}
			for _, v := range vs {
				decoded = append(decoded, nullDateToString(v))
			}
		case sppb.TypeCode_STRUCT:
			var vs []spanner.NullRow
			if err := column.Decode(&vs); err != nil {
				return "", err
			}
			if vs == nil {
				return "NULL", nil
			}
			for _, v := range vs {
				columns, err := DecodeRow(&v.Row)
				if err != nil {
					return "", err
				}
				decoded = append(decoded, fmt.Sprintf("[%s]", strings.Join(columns, ", ")))
			}
		}
		return fmt.Sprintf("[%s]", strings.Join(decoded, ", ")), nil
	case sppb.TypeCode_BOOL:
		var v spanner.NullBool
		if err := column.Decode(&v); err != nil {
			return "", err
		}
		return nullBoolToString(v), nil
	case sppb.TypeCode_BYTES:
		var v []byte
		if err := column.Decode(&v); err != nil {
			return "", err
		}
		return nullBytesToString(v), nil
	case sppb.TypeCode_FLOAT64:
		var v spanner.NullFloat64
		if err := column.Decode(&v); err != nil {
			return "", err
		}
		return nullFloat64ToString(v), nil
	case sppb.TypeCode_INT64:
		var v spanner.NullInt64
		if err := column.Decode(&v); err != nil {
			return "", err
		}
		return nullInt64ToString(v), nil
	case sppb.TypeCode_STRING:
		var v spanner.NullString
		if err := column.Decode(&v); err != nil {
			return "", err
		}
		return nullStringToString(v), nil
	case sppb.TypeCode_TIMESTAMP:
		var v spanner.NullTime
		if err := column.Decode(&v); err != nil {
			return "", err
		}
		return nullTimeToString(v), nil
	case sppb.TypeCode_DATE:
		var v spanner.NullDate
		if err := column.Decode(&v); err != nil {
			return "", err
		}
		return nullDateToString(v), nil
	default:
		return fmt.Sprintf("%s", column.Value), nil
	}
}

func nullBoolToString(v spanner.NullBool) string {
	if v.Valid {
		return fmt.Sprintf("%t", v.Bool)
	} else {
		return "NULL"
	}
}

func nullBytesToString(v []byte) string {
	if v != nil {
		return base64.RawStdEncoding.EncodeToString(v)
	} else {
		return "NULL"
	}
}

func nullFloat64ToString(v spanner.NullFloat64) string {
	if v.Valid {
		return fmt.Sprintf("%f", v.Float64)
	} else {
		return "NULL"
	}
}

func nullInt64ToString(v spanner.NullInt64) string {
	if v.Valid {
		return fmt.Sprintf("%d", v.Int64)
	} else {
		return "NULL"
	}
}

func nullStringToString(v spanner.NullString) string {
	if v.Valid {
		return v.StringVal
	} else {
		return "NULL"
	}
}

func nullTimeToString(v spanner.NullTime) string {
	if v.Valid {
		return fmt.Sprintf("%s", v.Time.Format(time.RFC3339Nano))
	} else {
		return "NULL"
	}
}

func nullDateToString(v spanner.NullDate) string {
	if v.Valid {
		return strings.Trim(v.String(), `"`)
	} else {
		return "NULL"
	}
}
