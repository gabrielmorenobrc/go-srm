package srm

import (
	"database/sql"
	"reflect"
	"fmt"
	"sync"
	"strings"
	"github.com/gabrielmorenobrc/go-tkt/lib"
)

type Trx struct {
	sequences *tkt.Sequences
	db        *sql.DB
	tx        *sql.Tx
	queryMap  map[string]string
	insertMap map[string]string
	updateMap map[string]string
	deleteMap map[string]string
	stmtMap   map[string]*sql.Stmt
	mux       sync.Mutex
	active    bool
}

func (o *Trx) Commit() {
	tkt.CheckErr(o.tx.Commit())
	o.active = false
}

func (o *Trx) Rollback() {
	if o.active {
		tkt.CheckErr(o.tx.Rollback())
	}
	o.active = false

}

func (o *Trx) Close() {
	tkt.CheckErr(o.db.Close())
}

func (o *Trx) Query(template interface{}, conditions string, args ...interface{}) interface{} {
	objectType := reflect.TypeOf(template)
	o.checkMaps()
	sql, ok := o.queryMap[objectType.Name()]
	if !ok {
		sql = o.buildQuerySql(objectType)
	}
	sql += " " + conditions
	tkt.Logger("orm").Println(sql)
	stmt, ok := o.stmtMap[sql]
	if !ok {
		stmt = o.createStmt(sql)
	}
	buffer := o.buildBuffer(objectType)
	r, err := stmt.Query(args...)
	tkt.CheckErr(err)
	arr := reflect.MakeSlice(reflect.SliceOf(objectType), 0, 0)
	for r.Next() {
		tkt.CheckErr(r.Scan(buffer...))
		object, _ := o.readBuffer(buffer, objectType, 0)
		arr = reflect.Append(arr, object)
	}
	return arr.Interface()
}

func (o *Trx) createStmt(sql string) *sql.Stmt {
	o.mux.Lock()
	defer o.mux.Unlock()
	stmt, err := o.tx.Prepare(sql)
	tkt.CheckErr(err)
	o.stmtMap[sql] = stmt
	return stmt
}

func (o *Trx) checkMaps() {
	if o.stmtMap == nil {
		o.mux.Lock()
		defer o.mux.Unlock()
		o.stmtMap = make(map[string]*sql.Stmt)
		o.queryMap = make(map[string]string)
		o.insertMap = make(map[string]string)
		o.deleteMap = make(map[string]string)
		o.updateMap = make(map[string]string)
	}
}

func (o *Trx) readBuffer(buffer []interface{}, objectType reflect.Type, offset int) (reflect.Value, int) {
	object := reflect.New(objectType).Elem()
	mtos := make([]reflect.Value, 0)
	var i int
	vi := offset
	for i = 0; i < object.NumField(); i++ {
		of := object.Field(i)
		if of.Type().Kind() == reflect.Struct {
			mtos = append(mtos, of)
		} else {
			v := buffer[vi]
			of.Set(reflect.ValueOf(v).Elem())
			vi++
		}
	}
	for j := range mtos {
		mto := mtos[j]
		var child interface{}
		child, vi = o.readBuffer(buffer, mto.Type(), vi)
		mto.Set(child.(reflect.Value))
	}
	return object, vi
}

func (o *Trx) buildBuffer(objectType reflect.Type) []interface{} {
	buffer := make([]interface{}, 0)
	mtos := make([]reflect.StructField, 0)
	for i := 0; i < objectType.NumField(); i++ {
		field := objectType.Field(i)
		if field.Type.Kind() == reflect.Struct {
			mtos = append(mtos, field)
		} else {
			buffer = append(buffer, reflect.New(field.Type).Interface())
		}
	}
	for i := range mtos {
		mto := mtos[i]
		mtoType := mto.Type
		buffer = append(buffer, o.buildBuffer(mtoType)...)
	}
	return buffer
}

func (o *Trx) buildQuerySql(objectType reflect.Type) string {
	o.mux.Lock()
	defer o.mux.Unlock()
	fields := make([]reflect.StructField, 0)
	mtos := make([]reflect.StructField, 0)
	for i := 0; i < objectType.NumField(); i++ {
		field := objectType.Field(i)
		if field.Type.Kind() == reflect.Struct {
			mtos = append(mtos, field)
		} else {
			fields = append(fields, field)
		}
	}
	sql := "select " + o.buildFieldsSelect(fields, "o")
	s := o.buildMtoSelects(mtos, "o")
	sql += s
	sql += " from " + objectType.Name() + " o"
	s = o.buildMtoJoins(mtos, "o")
	sql += s
	o.queryMap[objectType.Name()] = sql
	return sql
}

func (o *Trx) buildMtoSelects(mtos []reflect.StructField, path string) string {
	sql := ""
	for i := range mtos {
		mto := mtos[i]
		mtoType := mto.Type
		childMtos := make([]reflect.StructField, 0)
		childPath := path + "_" + mto.Name
		for j := 0; j < mtoType.NumField(); j++ {
			field := mtoType.Field(j)
			if field.Type.Kind() == reflect.Struct {
				childMtos = append(childMtos, field)
			} else {
				sql += ", "
				sql += fmt.Sprintf("%s.%s", childPath, field.Name)
			}
		}
		s := o.buildMtoSelects(childMtos, childPath)
		sql += s
	}
	return sql
}

func (o *Trx) buildMtoJoins(mtos []reflect.StructField, path string) string {
	sql := ""
	for i := range mtos {
		mto := mtos[i]
		mtoType := mto.Type
		childPath := path + "_" + mto.Name
		sql += fmt.Sprintf(" join %s %s on %s.id = %s.%s_id", mto.Name, childPath, childPath, path, mto.Name)
		childMtos := make([]reflect.StructField, 0)
		for j := 0; j < mtoType.NumField(); j++ {
			field := mtoType.Field(j)
			if field.Type.Kind() == reflect.Struct {
				childMtos = append(childMtos, field)
			}
		}
		if len(childMtos) > 0 {
			var s string
			s = o.buildMtoJoins(childMtos, childPath)
			sql += s
		}
	}
	return sql
}

func (o *Trx) buildFieldsSelect(fields []reflect.StructField, path string) string {
	s := ""
	for i := range fields {
		if i > 0 {
			s += ", "
		}
		field := fields[i]
		s += fmt.Sprintf("%s.%s", path, field.Name)
	}
	return s
}

func (o *Trx) QueryMutliple(templates []interface{}, conditions string, args ...interface{}) [][]interface{} {
	return nil
}

func (o *Trx) Find(template interface{}, id int64) interface{} {
	r := o.Query(template, "where o.Id = $1", id)
	value := reflect.ValueOf(r)
	if value.Len() == 0 {
		return nil
	} else {
		//dpv := reflect.ValueOf(result)
		//dv := reflect.Indirect(dpv)
		v := value.Index(0)
		return reflect.Indirect(v).Addr().Interface()
		/**
		dv.Set(v)
		return true
		**/
	}
}

func (o *Trx) Persist(entity interface{}) {
	o.checkMaps()
	object := reflect.Indirect(reflect.ValueOf(entity).Elem())
	objectType := object.Type()
	sql, ok := o.insertMap[objectType.Name()]
	if !ok {
		sql = o.buildInsertSql(objectType)
	}
	stmt, ok := o.stmtMap[sql]
	if !ok {
		stmt = o.createStmt(sql)
	}
	id := o.sequences.Next(strings.ToLower(objectType.Name()))
	of := object.Field(0)
	of.SetInt(id)
	buffer := make([]interface{}, object.NumField())
	for i := 0; i < object.NumField(); i++ {
		of := object.Field(i)
		if of.Type().Kind() == reflect.Struct {
			buffer[i] = of.Elem().FieldByName("Id").Interface()
		} else {
			buffer[i] = of.Interface()
		}
	}
	_, err := stmt.Exec(buffer...)
	tkt.CheckErr(err)
}

func (o *Trx) buildInsertSql(objectType reflect.Type) string {
	o.mux.Lock()
	defer o.mux.Unlock()
	sql := `insert into ` + strings.ToLower(objectType.Name()) + `(`
	for i := 0; i < objectType.NumField(); i++ {
		field := objectType.Field(i)
		if i > 0 {
			sql += ", "
		}
		if field.Type.Kind() == reflect.Struct {
			sql += field.Name + "_id"
		} else {
			sql += field.Name
		}
	}
	sql += `) values(`
	for i := 0; i < objectType.NumField(); i++ {
		if i > 0 {
			sql += ", "
		}
		sql += fmt.Sprintf("$%d", i + 1)
	}
	sql += `)`
	o.insertMap[objectType.Name()] = sql
	return sql
}

func (o *Trx) Update(entity interface{}) {

}

func (o *Trx) Delete(entity interface{}) {

}

func (o *Trx) RollbackOnPanic() {
	if r := recover(); r != nil {
		o.Rollback()
		panic(r)
	}
}

func (o *Trx) Init(db *sql.DB, tx *sql.Tx, sequences *tkt.Sequences) {
	o.db = db
	o.tx = tx
	o.active = true
	o.sequences = sequences
	o.mux = sync.Mutex{}
}

type Orm struct {
	DatabaseConfig tkt.DatabaseConfig
}

func (o *Orm) StartTransaction() *Trx {
	transaction := Trx{}
	db := tkt.OpenDB(o.DatabaseConfig)
	tx, err := db.Begin()
	tkt.CheckErr(err)
	sequences := tkt.NewSequences(o.DatabaseConfig)
	transaction.Init(db, tx, sequences)
	return &transaction
}
