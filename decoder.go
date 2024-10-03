//
// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package main

import (
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
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
		case sppb.TypeCode_BYTES, sppb.TypeCode_PROTO:
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
		case sppb.TypeCode_FLOAT32:
			var vs []spanner.NullFloat32
			if err := column.Decode(&vs); err != nil {
				return "", err
			}
			if vs == nil {
				return "NULL", nil
			}
			for _, v := range vs {
				decoded = append(decoded, nullFloat32ToString(v))
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
		case sppb.TypeCode_INT64, sppb.TypeCode_ENUM:
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
		case sppb.TypeCode_NUMERIC:
			var vs []spanner.NullNumeric
			if err := column.Decode(&vs); err != nil {
				return "", err
			}
			if vs == nil {
				return "NULL", nil
			}
			for _, v := range vs {
				decoded = append(decoded, nullNumericToString(v))
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
			// STRUCT is only allowed in an ARRAY
			// https://cloud.google.com/spanner/docs/structs#returning_struct_objects_in_sql_query_results
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
		case sppb.TypeCode_JSON:
			var vs []spanner.NullJSON
			if err := column.Decode(&vs); err != nil {
				return "", err
			}
			if vs == nil {
				return "NULL", nil
			}
			for _, v := range vs {
				decoded = append(decoded, nullJSONToString(v))
			}
		}
		return fmt.Sprintf("[%s]", strings.Join(decoded, ", ")), nil
	case sppb.TypeCode_BOOL:
		var v spanner.NullBool
		if err := column.Decode(&v); err != nil {
			return "", err
		}
		return nullBoolToString(v), nil
	case sppb.TypeCode_BYTES, sppb.TypeCode_PROTO:
		var v []byte
		if err := column.Decode(&v); err != nil {
			return "", err
		}
		return nullBytesToString(v), nil
	case sppb.TypeCode_FLOAT32:
		var v spanner.NullFloat32
		if err := column.Decode(&v); err != nil {
			return "", err
		}
		return nullFloat32ToString(v), nil
	case sppb.TypeCode_FLOAT64:
		var v spanner.NullFloat64
		if err := column.Decode(&v); err != nil {
			return "", err
		}
		return nullFloat64ToString(v), nil
	case sppb.TypeCode_INT64, sppb.TypeCode_ENUM:
		var v spanner.NullInt64
		if err := column.Decode(&v); err != nil {
			return "", err
		}
		return nullInt64ToString(v), nil
	case sppb.TypeCode_NUMERIC:
		var v spanner.NullNumeric
		if err := column.Decode(&v); err != nil {
			return "", err
		}
		return nullNumericToString(v), nil
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
	case sppb.TypeCode_JSON:
		var v spanner.NullJSON
		if err := column.Decode(&v); err != nil {
			return "", err
		}
		return nullJSONToString(v), nil
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
		return base64.StdEncoding.EncodeToString(v)
	} else {
		return "NULL"
	}
}

func nullFloat32ToString(v spanner.NullFloat32) string {
	if v.Valid {
		return fmt.Sprintf("%f", v.Float32)
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

func nullNumericToString(v spanner.NullNumeric) string {
	if v.Valid {
		s := v.Numeric.FloatString(spanner.NumericScaleDigits)
		s = strings.TrimRight(s, "0")     // trim trailing 0: 123.000000000 => 123.
		return strings.TrimSuffix(s, ".") // trim unused dot: 123. => 123
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

func nullJSONToString(v spanner.NullJSON) string {
	if v.Valid {
		return v.String()
	} else {
		return "NULL"
	}
}

func formatTypeSimple(typ *sppb.Type) string {
	switch code := typ.GetCode(); code {
	case sppb.TypeCode_ARRAY:
		return fmt.Sprintf("ARRAY<%v>", formatTypeSimple(typ.GetArrayElementType()))
	default:
		if name, ok := sppb.TypeCode_name[int32(code)]; ok {
			return name
		} else {
			return "UNKNOWN"
		}
	}
}

func formatTypeVerbose(typ *sppb.Type) string {
	switch code := typ.GetCode(); code {
	case sppb.TypeCode_ARRAY:
		return fmt.Sprintf("ARRAY<%v>", formatTypeVerbose(typ.GetArrayElementType()))
	case sppb.TypeCode_ENUM, sppb.TypeCode_PROTO:
		return typ.GetProtoTypeFqn()
	case sppb.TypeCode_STRUCT:
		var structTypeStrs []string
		for _, v := range typ.GetStructType().GetFields() {
			if v.GetName() != "" {
				structTypeStrs = append(structTypeStrs, fmt.Sprintf("%v %v", v.GetName(), formatTypeVerbose(v.GetType())))
			} else {
				structTypeStrs = append(structTypeStrs, fmt.Sprintf("%v", formatTypeVerbose(v.GetType())))
			}
		}
		return fmt.Sprintf("STRUCT<%v>", strings.Join(structTypeStrs, ", "))
	default:
		if name, ok := sppb.TypeCode_name[int32(code)]; ok {
			return name
		} else {
			return "UNKNOWN"
		}
	}
}
