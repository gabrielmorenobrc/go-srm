package srm

import (
	"database/sql"
	"reflect"
	"fmt"
	"sync"
	"github.com/gabrielmorenobrc/go-tkt/lib"
	"bytes"
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
	buffer := o.buildReadBufferForType(objectType)
	r, err := stmt.Query(args...)
	tkt.CheckErr(err)
	arr := reflect.MakeSlice(reflect.SliceOf(objectType), 0, 0)
	for r.Next() {
		tkt.CheckErr(r.Scan(buffer...))
		object, _ := o.readBufferForType(buffer, objectType, 0)
		arr = reflect.Append(arr, *object)
	}
	return arr.Interface()
}

func (o *Trx) Find(template interface{}, id int64) interface{} {
	r := o.Query(template, "where o.Id = $1", id)
	value := reflect.ValueOf(r)
	if value.Len() == 0 {
		return nil
	} else {
		v := value.Index(0)
		return reflect.Indirect(v).Addr().Interface()
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
	name := FqTableName(objectType)
	id := o.sequences.Next(name)
	of := object.Field(0)
	of.SetInt(id)
	buffer := make([]interface{}, object.NumField())
	for i := 0; i < object.NumField(); i++ {
		of := object.Field(i)
		if IsEntity(of.Type()) {
			buffer[i] = of.FieldByName("Id").Interface()
		} else {
			buffer[i] = of.Interface()
		}
	}
	_, err := stmt.Exec(buffer...)
	tkt.CheckErr(err)
}

func (o *Trx) Update(entity interface{}) {
	o.checkMaps()
	object := reflect.Indirect(reflect.ValueOf(entity).Elem())
	objectType := object.Type()
	sql, ok := o.updateMap[objectType.Name()]
	if !ok {
		sql = o.buildUpdateSql(objectType)
	}
	stmt, ok := o.stmtMap[sql]
	if !ok {
		stmt = o.createStmt(sql)
	}
	buffer := make([]interface{}, object.NumField())
	for i := 0; i < object.NumField(); i++ {
		of := object.Field(i)
		if IsEntity(of.Type()) {
			buffer[i] = of.Elem().FieldByName("Id").Interface()
		} else {
			buffer[i] = of.Interface()
		}
	}
	_, err := stmt.Exec(buffer...)
	tkt.CheckErr(err)
}

func (o *Trx) Delete(entity interface{}) {
	o.checkMaps()
	object := reflect.Indirect(reflect.ValueOf(entity).Elem())
	objectType := object.Type()
	sql, ok := o.deleteMap[objectType.Name()]
	if !ok {
		sql = o.buildDeleteSql(objectType)
	}
	stmt, ok := o.stmtMap[sql]
	if !ok {
		stmt = o.createStmt(sql)
	}
	of := object.Field(0)
	_, err := stmt.Exec(of.Interface())
	tkt.CheckErr(err)
}

func (o *Trx) buildInsertSql(objectType reflect.Type) string {
	o.mux.Lock()
	defer o.mux.Unlock()
	name := FqTableName(objectType)
	sql := `insert into ` + name + `(`
	for i := 0; i < objectType.NumField(); i++ {
		field := objectType.Field(i)
		if i > 0 {
			sql += ", "
		}
		if IsEntity(field.Type) {
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
		sql += fmt.Sprintf("$%d", i+1)
	}
	sql += `)`
	o.insertMap[objectType.Name()] = sql
	return sql
}

func (o *Trx) buildUpdateSql(objectType reflect.Type) string {
	o.mux.Lock()
	defer o.mux.Unlock()
	name := FqTableName(objectType)
	sql := `update ` + name
	for i := 1; i < objectType.NumField(); i++ {
		field := objectType.Field(i)
		if i > 0 {
			sql += ","
		}
		sql += " set"
		if IsEntity(field.Type) {
			sql += field.Name + "_id"
		} else {
			sql += field.Name
		}
		sql += fmt.Sprintf(" = $%d", i+1)
	}
	sql += ` where id == $1`
	o.updateMap[objectType.Name()] = sql
	return sql
}

func (o *Trx) buildDeleteSql(objectType reflect.Type) string {
	o.mux.Lock()
	defer o.mux.Unlock()
	name := FqTableName(objectType)
	sql := `delete from ` + name + ` where id = $1`
	o.updateMap[objectType.Name()] = sql
	return sql
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

func (o *Trx) QueryMulti(templates []interface{}, joins *Joins, conditions string, args ...interface{}) [][]interface{} {
	o.checkMaps()
	key := o.buildStmtKeyForMultiple(templates, joins, conditions)
	var stmt *sql.Stmt
	stmt, ok := o.stmtMap[key]
	if !ok {
		stmt = o.buildStmtForMultiple(key, templates, joins, conditions)
	}

	r, err := stmt.Query(args...)
	tkt.CheckErr(err)

	objectTypes := make([]reflect.Type, 0)
	for i := range templates {
		objectTypes = append(objectTypes, reflect.TypeOf(templates[i]))
	}

	buffer := make([]interface{}, 0)
	for i := range templates {
		objectType := reflect.TypeOf(templates[i])
		buffer = append(buffer, o.buildReadBufferForType(objectType)...)
	}

	arr := make([][]interface{}, 0)
	for r.Next() {
		tkt.CheckErr(r.Scan(buffer...))
		objects := make([]interface{}, len(templates))
		offset := 0
		for i := range templates {
			objectType := objectTypes[i]
			object, n := o.readBufferForType(buffer, objectType, offset)
			if object == nil {
				objects[i] = reflect.New(reflect.PtrTo(objectType)).Elem().Interface()
			} else {
				objects[i] = object.Addr().Interface()
			}
			offset = n
		}
		arr = append(arr, objects)
	}
	return arr
}

func (o *Trx) buildStmtForMultiple(key string, templates []interface{}, joins *Joins, conditions string) *sql.Stmt {
	o.mux.Lock()
	defer o.mux.Unlock()
	sql := o.buildSqlForMultiple(templates, joins, conditions)
	tkt.Logger("srm").Println(sql)
	stmt, err := o.tx.Prepare(sql)
	tkt.CheckErr(err)
	o.stmtMap[key] = stmt
	return stmt
}

func (o *Trx) buildStmtKeyForMultiple(templates []interface{}, joins *Joins, conditions string) string {
	buffer := bytes.Buffer{}
	for i := range templates {
		buffer.WriteString(".")
		name := FqTableName(reflect.TypeOf(templates[i]))
		buffer.WriteString(name)
	}
	buffer.WriteString(";")
	for i := 0; i < joins.Size(); i++ {
		buffer.WriteString(joins.Join(i))
		buffer.WriteString(" ")
		buffer.WriteString(joins.On(i))
	}
	buffer.WriteString(";")
	buffer.WriteString(conditions)
	return buffer.String()
}

func (o *Trx)buildSqlForMultiple(templates []interface{}, joins *Joins, conditions string) string {
	sql := "select "
	for i := range templates {
		template := templates[i]
		if i > 0 {
			sql += ",\r\n"
		}
		alias := fmt.Sprintf("o%d", i+1)
		sql += o.buildSelectFieldsForTemplate(template, alias)
	}
	name := FqTableName(reflect.TypeOf(templates[0]))
	sql += "\r\nfrom " + name + " o1"
	sql += "\r\n" + o.buildFromMtoSqlForTemplate(templates[0], "o1")
	sql += o.buildJoinSqlForTemplates(templates, joins)
	sql += "\r\n" + conditions
	return sql
}

func (o *Trx) buildFromMtoSqlForTemplates(templates []interface{}, offset int) string {
	sql := ""
	for i := offset; i < len(templates); i++ {
		joinSql := o.buildFromMtoSqlForTemplate(templates[i], fmt.Sprintf("o%d", i+1))
		if len(joinSql) > 0 {
			if i > 0 {
				sql += "\r\n"
			} else {
				sql += " "
			}
			sql += joinSql
		}
	}
	return sql
}

func (o *Trx) buildFromMtoSqlForTemplate(template interface{}, path string) string {
	sql := ""
	objectType := reflect.TypeOf(template)
	mtos := o.buildMtoList(objectType)
	if len(mtos) > 0 {
		sql += o.buildMtoJoins(mtos, path)
	}
	return sql
}

func (o *Trx) buildJoinSqlForTemplates(templates []interface{}, joins *Joins) string {
	sql := ""
	for i := 0; i < joins.Size(); i++ {
		template := templates[i+1]
		objectType := reflect.TypeOf(template)
		alias := fmt.Sprintf("o%d", i+2)
		sql += "\r\n" + joins.Join(i)
		mtos := o.buildMtoList(objectType)
		if len(mtos) > 0 {
			sql += " ("
		} else {
			sql += " "
		}
		name := FqTableName(objectType)
		sql += name + " " + alias
		if len(mtos) > 0 {
			sql += o.buildMtoJoins(mtos, alias) + ")"
		}
		sql += " on " + joins.On(i)
	}
	return sql
}

func (o *Trx) buildMtoList(objectType reflect.Type) []reflect.StructField {
	mtos := make([]reflect.StructField, 0)
	for i := 0; i < objectType.NumField(); i++ {
		field := objectType.Field(i)
		if IsEntity(field.Type) {
			mtos = append(mtos, field)
		}
	}
	return mtos
}

func (o *Trx) buildSelectFieldsForTemplate(template interface{}, path string) string {
	objectType := reflect.TypeOf(template)
	fields := make([]reflect.StructField, 0)
	mtos := make([]reflect.StructField, 0)
	for i := 0; i < objectType.NumField(); i++ {
		field := objectType.Field(i)
		if IsEntity(field.Type) {
			mtos = append(mtos, field)
		} else {
			fields = append(fields, field)
		}
	}
	sql := o.buildFieldsSelect(fields, path)
	sql += o.buildMtoFieldsSelect(mtos, path)
	return sql
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

func (o *Trx) readBufferForType(buffer []interface{}, objectType reflect.Type, offset int) (*reflect.Value, int) {

	ppId := buffer[offset].(**int64)
	pId := *ppId
	if pId == nil {
		t := o.countFieldsDeep(objectType)
		return nil, offset + t
	}
	objectValue := reflect.New(objectType).Elem()
	idField := objectValue.Field(0)
	idField.Set(reflect.ValueOf(*pId))
	mtos := make([]reflect.Value, 0)
	var i int
	vi := offset + 1
	for i = 1; i < objectValue.NumField(); i++ {
		of := objectValue.Field(i)
		if IsEntity(of.Type()) {
			mtos = append(mtos, of)
		} else {
			v := buffer[vi].(*interface{})
			of.Set(reflect.ValueOf(*v))
			vi++
		}
	}
	for j := range mtos {
		mto := mtos[j]
		var child interface{}
		child, vi = o.readBufferForType(buffer, mto.Type(), vi)
		mto.Set(*child.(*reflect.Value))
	}
	return &objectValue, vi
}

func (o *Trx) countFieldsDeep(objectType reflect.Type) int {
	t := 0
	for i := 0; i < objectType.NumField(); i++ {
		f := objectType.Field(i)
		if IsEntity(f.Type) {
			t += o.countFieldsDeep(f.Type)
		} else {
			t++
		}
	}
	return t
}

func (o *Trx) buildReadBufferForType(objectType reflect.Type) []interface{} {
	buffer := o.buildStaticFieldBuffer(objectType)
	mtos := o.buildMtoList(objectType)
	for i := range mtos {
		mto := mtos[i]
		mtoType := mto.Type
		buffer = append(buffer, o.buildReadBufferForType(mtoType)...)
	}
	return buffer
}

func (o *Trx) buildStaticFieldBuffer(objectType reflect.Type) []interface{} {
	buffer := make([]interface{}, 0)
	var id *int64
	buffer = append(buffer, &id)
	for i := 1; i < objectType.NumField(); i++ {
		field := objectType.Field(i)
		if !IsEntity(field.Type) {
			i := reflect.New(field.Type).Interface()
			buffer = append(buffer, &i)
		}
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
		if IsEntity(field.Type) {
			mtos = append(mtos, field)
		} else {
			fields = append(fields, field)
		}
	}
	sql := "select " + o.buildFieldsSelect(fields, "o")
	s := o.buildMtoFieldsSelect(mtos, "o")
	sql += s
	name := FqTableName(objectType)
	sql += " from " + name + " o"
	s = o.buildMtoJoins(mtos, "o")
	sql += s
	o.queryMap[objectType.Name()] = sql
	return sql
}

func (o *Trx) buildMtoFieldsSelect(mtos []reflect.StructField, path string) string {
	sql := ""
	for i := range mtos {
		mto := mtos[i]
		mtoType := mto.Type
		childMtos := make([]reflect.StructField, 0)
		childPath := path + "_" + mto.Name
		for j := 0; j < mtoType.NumField(); j++ {
			field := mtoType.Field(j)
			if IsEntity(field.Type) {
				childMtos = append(childMtos, field)
			} else {
				sql += ", "
				sql += fmt.Sprintf("%s.%s", childPath, field.Name)
			}
		}
		s := o.buildMtoFieldsSelect(childMtos, childPath)
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
		if i > 0 {
			sql += "\r\n"
		} else {
			sql += " "
		}
		name := FqTableName(mto.Type)
		sql += fmt.Sprintf("join %s %s on %s.id = %s.%s_id", name, childPath, childPath, path, mto.Name)
		childMtos := make([]reflect.StructField, 0)
		for j := 0; j < mtoType.NumField(); j++ {
			field := mtoType.Field(j)
			if IsEntity(field.Type) {
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

