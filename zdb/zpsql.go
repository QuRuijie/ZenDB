package zdb

import (
	"flag"
	"github.com/Zentertain/zenlog"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
)

var (
	gDB *gorm.DB
)

func InitPsql(conn string) {
	if flag.Lookup("test.v") != nil && conn == "" {
		conn = "host=localhost port=5432 dbname=giftu sslmode=disable"
	}

	db, err := gorm.Open("postgres", conn)
	if err != nil || db == nil {
		errInfo := ""
		if err != nil {
			errInfo = err.Error()
		}
		zenlog.Panicln("Failed when connect db, err:", errInfo)
		return
	}
	gDB = db
	zenlog.Info("Connect Pg successful")
	err = db.DB().Ping()
	if err != nil {
		zenlog.Panicln("Failed when ping db, err:", err.Error())
	}
	zenlog.Info("Ping Pg successful")
}

func DB() *gorm.DB {
	return gDB
}
