package main

import (
	"flag"
	"path/filepath"
	"io/ioutil"
	"encoding/json"
	"log"
	"github.com/gabrielmorenobrc/go-srm/lib"
	"github.com/gabrielmorenobrc/go-tkt/lib"
	"fmt"
)

type Master1 struct {
	Id int64
	Name string
}

type Master2 struct {
	Id int64
	Name string
}

type Detail struct {
	Id int64
	Master1 Master1
	Master2 Master2
	Name   string
}

type YetAnother struct {
	Id int64
	Detail Detail
	Name   string
}


type Config struct {
	DatabaseConfig    tkt.DatabaseConfig `json:"databaseConfig"`
}

var Sequences *tkt.Sequences

type InitDB struct {
	tkt.TransactionalImpl
}

func (o *InitDB) Execute() {
	_, err := o.Db().Query("SELECT count(*) FROM master1")
	if err != nil {
		log.Println(err)
		o.create()
	}
}

func (o *InitDB) create() {
	abs, err := filepath.Abs("create.sql")
	tkt.CheckErr(err)
	println(abs)
	bytes, err := ioutil.ReadFile(abs)
	tkt.CheckErr(err)
	_, err = o.Tx().Exec(string(bytes))
	tkt.CheckErr(err)
}

var config Config

var conf = flag.String("conf", "conf.json", "Config")

var sequences *tkt.Sequences

func main() {

	tkt.Ping()

	flag.Parse()
	loadConfig()

	sequences = tkt.NewSequences(config.DatabaseConfig)

	initDB := InitDB{}
	tkt.ExecuteTransactional(config.DatabaseConfig, &initDB)

	mgr := srm.Mgr{DatabaseConfig:config.DatabaseConfig}
	tx := mgr.StartTransaction()
	defer tx.RollbackOnPanic()

	r1 := tx.Query(Detail{}, "where o_Master1.Id = $1 and o_Master2.Id = 2", 1).([]Detail)
	for i := range r1 {
		log.Printf("detail: %s, master: %s", r1[i].Name, r1[i].Master1.Name)
	}

	r2 := tx.Query(YetAnother{}, "where o_Detail_Master1.Id = $1", 1).([]YetAnother)
	for i := range r2 {
		log.Printf("yet: %s, master: %s", r2[i].Name, r2[i].Detail.Master1.Name)
	}


	p1 := tx.Find(Master1{}, 1).(*Master1)
	if p1 != nil {
		println(p1.Name)
	}

	m := Master1{Name: fmt.Sprintf("Persisted")}
	tx.Persist(&m)

	r3 := tx.Query(Master1{}, "").([]Master1)
	for i := range r3 {
		log.Printf("master1: %d, %s", r3[i].Id, r3[i].Name)
	}

	rows := tx.QueryMulti([]interface{}{Master1{}, Detail{}, YetAnother{}},
		srm.Loj("o2.master1_id = o1.id").Loj("o3.detail_id = o2.id"),
		"order by o1.id")
	for i := range rows {
		row := rows[i]
		m := row[0].(*Master1)
		d := row[1].(*Detail)
		ya := row[2].(*YetAnother)
		log.Printf("%s, %s, %s", m.Name, d, ya)
	}

	tx.Commit()

}

func loadConfig() {
	abs, err := filepath.Abs(*conf)
	tkt.CheckErr(err)
	bytes, err := ioutil.ReadFile(abs)
	tkt.CheckErr(err)
	config = Config{}
	err = json.Unmarshal(bytes, &config)
	tkt.CheckErr(err)
}

