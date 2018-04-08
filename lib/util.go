package srm

import (
	"reflect"
	"time"
	"math/big"
	"strings"
)

type Joins struct {
	joinList []string
	onList []string
}

func (o *Joins)Size() int {
	return len(o.joinList)
}

func (o *Joins)Join(i int) string {
	return o.joinList[i]
}

func (o *Joins)On(i int) string {
	return o.onList[i]
}

func (o *Joins)Ij(on string) *Joins {
	o.init()
	o.joinList = append(o.joinList, "join")
	o.onList = append(o.onList, on)
	return o
}

func (o *Joins)Loj(on string) *Joins {
	o.init()
	o.joinList = append(o.joinList, "left outer join")
	o.onList = append(o.onList, on)
	return o
}

func (o *Joins) init() {
	if o.joinList == nil {
		o.joinList = make([]string, 0)
	}
	if o.onList == nil {
		o.onList = make([]string, 0)
	}
}


func Loj(on string) *Joins {
	j := Joins{}
	return j.Loj(on)
}

func Ij(on string) *Joins {
	j := Joins{}
	return j.Ij(on)
}

var complexTypes = []reflect.Type{reflect.TypeOf(time.Now()), reflect.TypeOf(big.Float{})}

func IsEntity(objectType reflect.Type) bool {
	kind := objectType.Kind()
	if kind == reflect.Struct {
		f, ok := objectType.FieldByName("Id")
		return ok && f.Type.Kind() == reflect.Int64
	} else {
		return false
	}
}

func FqTableName(objectType reflect.Type) string {
	name := strings.ToLower(objectType.Name())
	idField, _ := objectType.FieldByName("Id")
	schema, ok := idField.Tag.Lookup("schema")
	if ok {
		return schema + "." + name
	} else {
		return name
	}
}