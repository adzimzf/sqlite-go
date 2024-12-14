package db

import (
	"fmt"
	"log"
	"strings"
)

type TupleType int

const (
	TupleTypeInt64 TupleType = iota
	TupleTypeString
	TupleTypeInt8
	TupleTypeNull
)

type Tuple struct {
	typeID TupleType
	value  interface{}
}

func NewInt64Tuple(value int64) *Tuple {
	return &Tuple{value: value, typeID: TupleTypeInt64}
}

func NewStringTuple(value string) *Tuple {
	return &Tuple{value: value, typeID: TupleTypeString}
}

func NewInt8Tuple(value int8) *Tuple {
	return &Tuple{value: value, typeID: TupleTypeInt8}
}

func NewNullTuple() *Tuple {
	return &Tuple{typeID: TupleTypeNull}
}

type RecordTuple []*Tuple

type Rows []RecordTuple

func RecordsToRows(records []Record, tableInfo TableSchemaInfo) (Rows, error) {
	var rows Rows
	for _, record := range records {
		var recordTuple RecordTuple
		for i := 0; i < len(record.Header.Fields); i++ {
			data, fieldType, err := record.FieldData(i)
			if err != nil {
				return rows, nil
			}
			switch fieldType {
			case Null:
				// if it's null however the column as primary id and auto increment
				// the value equal to RowID
				if tableInfo.HasPrimaryKey() && record.Header.Fields[i].FieldIdx == tableInfo.PrimaryKey.Idx {
					recordTuple = append(recordTuple, NewInt64Tuple(record.RowID))
				} else {
					recordTuple = append(recordTuple, NewNullTuple())
				}
			case Int8:
				recordTuple = append(recordTuple, NewInt8Tuple(data.(int8)))
			case Int64:
				recordTuple = append(recordTuple, NewInt64Tuple(data.(int64)))
			case String:
				recordTuple = append(recordTuple, NewStringTuple(data.(string)))
			default:
				log.Printf("Unsupported field type %v", fieldType)
			}
		}
		rows = append(rows, recordTuple)
	}
	return rows, nil
}

func (r Rows) RowsString(selectedFields []*SelectFieldExpression) string {

	res := strings.Builder{}
	for i, field := range selectedFields {
		if field.IsAgg {
			res.WriteString(field.AggType.String())
			continue
		}
		if field.TableName != "" {
			res.WriteString(fmt.Sprintf("%s.%s", field.TableName, field.ColName))
		} else {
			res.WriteString(fmt.Sprintf("%s", field.ColName))
		}
		if i < len(selectedFields)-1 {
			res.WriteString(fmt.Sprintf(", "))
		}
	}
	res.WriteString("\n")
	for _, row := range r {
		for i, tuple := range row {
			if tuple.value == nil {
				res.WriteString("NULL")
			} else {
				res.WriteString(fmt.Sprintf("%v", tuple.value))
			}
			if i < len(row)-1 {
				res.WriteString(fmt.Sprintf(", "))
			}
		}
		res.WriteString("\n")
	}
	return res.String()
}
