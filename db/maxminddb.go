package db

import (
	"github.com/Zentertain/zenlog"
	"github.com/oschwald/maxminddb-golang"
	"os"
	"path/filepath"
)

var gMMDB *maxminddb.Reader

func GetMaxMindDB() *maxminddb.Reader {
	return gMMDB
}

func LoadMaxMindDB() error {
	exec, err := os.Executable()
	if err != nil {
		return err
	}

	workingDir := filepath.Dir(exec)
	rd, err := maxminddb.Open(workingDir + "/GeoLite2-City.mmdb")
	if err != nil {
		zenlog.Error("maxminddb.Open failed, err:%v", err)
		panic(err)
	}
	gMMDB = rd

	return nil
}
