package db

import (
	"time"

	"github.com/Zentertain/zenlog"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
)

var (
	gdbIap          *gorm.DB
	gdbDataboat     *gorm.DB
	gbdDataboatUser *gorm.DB
)

func InitIapPg(addr string, maxIdleConns int, MaxOpenConns int) {
	gdbIap = newGorm("IAP", addr, maxIdleConns, MaxOpenConns)
}

func InitDataboatPg(addr string, maxIdleConns int, MaxOpenConns int) {
	gdbDataboat = newGorm("Databoat", addr, maxIdleConns, MaxOpenConns)
}

func InitDataboatUserPg(addr string, maxIdleConns int, MaxOpenConns int) {
	gbdDataboatUser = newGorm("DataboatUser", addr, maxIdleConns, MaxOpenConns)
}

func newGorm(name string, addr string, maxIdleConns int, MaxOpenConns int) *gorm.DB {
	if addr == "" {
		return nil
	}
	db, err := gorm.Open("postgres", addr)
	if err != nil {
		panic(err)
	}

	db.DB().SetConnMaxLifetime(time.Minute * 5)
	if maxIdleConns > 0 {
		db.DB().SetMaxIdleConns(maxIdleConns)
	} else {
		db.DB().SetMaxIdleConns(5)
	}
	if MaxOpenConns > 0 {
		db.DB().SetMaxOpenConns(MaxOpenConns)
	} else {
		db.DB().SetMaxOpenConns(7)
	}

	zenlog.Info("Connect PG %s successful", name)
	err = db.DB().Ping()
	if err != nil {
		panic(err)
	}
	zenlog.Info("Ping PG %s successful", name)
	return db
}

func Iap() *gorm.DB {
	return gdbIap
}

func Databoat() *gorm.DB {
	return gdbDataboat
}

func DataboatUser() *gorm.DB {
	return gbdDataboatUser
}
