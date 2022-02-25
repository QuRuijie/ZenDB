package conf

import (
	"fmt"
	"github.com/QuRuijie/zenDB/commgame/options"
	"github.com/Zentertain/zenlog"
	yaml "gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"path/filepath"
)

var (
	WorkingDir = ""
)

func init() {
	exec, err := os.Executable()
	if err != nil {
		panic("Get exec path fail!")
	}
	WorkingDir = filepath.Dir(exec)

	if options.Debug {
		WorkingDir, err = os.Getwd()
		if err != nil {
			panic("Get exec path fail!")
		}
	}

	// To make test work
	// workingDir = "/Users/pdeng/work/GiftU/server/bin"

	initConfig()
	configFile := fmt.Sprintf("%s/conf/server.yaml", WorkingDir)
	err = readYaml(configFile, &Server)
	if err == nil {
		zenlog.Info("conf:%+v", Server)
		return
	}
	if err = readYaml("/configVolume/server.yaml", &Server); err != nil {
		zenlog.Panicln("real yaml config error, ", err)
	}

	zenlog.Info("conf:%+v", Server)
}

func readYaml(filename string, out interface{}) error {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}

	return yaml.Unmarshal(data, out)
}
