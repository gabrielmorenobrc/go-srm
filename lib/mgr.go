package srm

import (
	"github.com/gabrielmorenobrc/go-tkt/lib"
	"reflect"
	"bytes"
	"time"
)

type Mgr struct {
	DatabaseConfig tkt.DatabaseConfig
}

func (o *Mgr) StartTransaction() *Trx {
	transaction := Trx{}
	db := tkt.OpenDB(o.DatabaseConfig)
	tx, err := db.Begin()
	tkt.CheckErr(err)
	sequences := tkt.NewSequences(o.DatabaseConfig)
	transaction.Init(db, tx, sequences)
	return &transaction
}

func (o *Mgr) CreateTables(templates []interface{}) {
	trx := o.StartTransaction()
	defer trx.RollbackOnPanic()
	for i := range templates {
		t := templates[i]
		o.createTable(trx, t)
	}
	trx.Commit()
}

func (o *Mgr) createTable(trx *Trx, template interface{}) {
	objectType := reflect.TypeOf(template)

	r, err := trx.db.Query("select * from " + objectType.Name() + " where 1 = 2")
	if err == nil {
		r.Close()
		tkt.Logger("srm").Printf("%s already exists", objectType.Name())
		return
	}

	buffer := bytes.Buffer{}
	buffer.WriteString("create table ")
	idField, _ := objectType.FieldByName("Id")
	schema, ok := idField.Tag.Lookup("schema")
	if ok {
		buffer.WriteString(schema)
		buffer.WriteString(".")
	}
	buffer.WriteString(objectType.Name())
	buffer.WriteString("(\r\n")
	for i := 0; i < objectType.NumField(); i++ {
		if i > 0 {
			buffer.WriteString(",\r\n")
		}
		f := objectType.Field(i)
		buffer.WriteString(f.Name)
		if f.Type == reflect.TypeOf(time.Now()) {
			temporal := f.Tag.Get("temporal")
			buffer.WriteString(" ")
			buffer.WriteString(temporal)
		} else if f.Type.Kind() == reflect.Struct {
			buffer.WriteString("_id bigint")
		} else if f.Type == reflect.TypeOf(0) {
			buffer.WriteString(" int")
		} else if f.Type == reflect.TypeOf(int64(0)) {
			buffer.WriteString(" bigint")
		} else if f.Type == reflect.TypeOf([]uint8{}) {
			precision, _ := f.Tag.Lookup("precision")
			buffer.WriteString(" numeric(")
			buffer.WriteString(precision)
			buffer.WriteString(")")
		} else if f.Type == reflect.TypeOf(float32(0.0)) {
			buffer.WriteString(" float")
		} else if f.Type == reflect.TypeOf(float64(0.0)) {
			buffer.WriteString(" double precision")
		} else if f.Type == reflect.TypeOf("") {
			len, ok := f.Tag.Lookup("len")
			if !ok {
				len = "255"
			}
			buffer.WriteString(" varchar(")
			buffer.WriteString(len)
			buffer.WriteString(")")
		} else if f.Type == reflect.TypeOf(make([]byte, 0)) {
			buffer.WriteString(" blob")
		}
		buffer.WriteString(" not null")
	}
	buffer.WriteString(",\r\nprimary key(id)")
	for i := 0; i < objectType.NumField(); i++ {
		f := objectType.Field(i)
		fieldType := f.Type
		if IsEntity(fieldType) {
			buffer.WriteString(",\r\n foreign key(")
			buffer.WriteString(f.Name)
			buffer.WriteString("_id) references ")
			buffer.WriteString(FqTableName(f.Type))
			buffer.WriteString("(id)")
		}
	}
	buffer.WriteString(");")
	sql := buffer.String()
	tkt.Logger("srm").Println(sql)
	_, err = trx.tx.Exec(sql)
	tkt.CheckErr(err)
}
