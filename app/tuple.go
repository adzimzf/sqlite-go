package main

import (
	"fmt"
	"log"
	"strings"
)

type TupleType int

const (
	TupleTypeInt64 TupleType = iota
	TupleTypeString
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

type RecordTuple []*Tuple

type Rows []RecordTuple

func RecordsToRows(records []Record) (Rows, error) {
	var rows Rows
	for _, record := range records {
		var recordTuple RecordTuple
		for i := 0; i < len(record.Header.Fields); i++ {
			data, fieldType, err := record.FieldData(i)
			if err != nil {
				return rows, nil
			}
			switch fieldType {
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
	for _, field := range selectedFields {
		if field.IsAgg {
			res.WriteString(field.AggType.String())
			continue
		}
		if field.TableName != "" {
			res.WriteString(fmt.Sprintf("%s.%s", field.TableName, field.ColName))
		} else {
			res.WriteString(fmt.Sprintf("%s", field.ColName))
		}
	}
	res.WriteString("\n")
	for _, row := range r {
		for _, tuple := range row {
			res.WriteString(fmt.Sprintf("%v", tuple.value))
		}
		res.WriteString("\n")
	}
	return res.String()
}
