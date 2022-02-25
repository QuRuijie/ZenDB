package conf

import (
	"fmt"
	yaml "gopkg.in/yaml.v2"
	"testing"
)

func TestYAML(t *testing.T) {
	var content = []byte(`
ProjPrivacy:
  "com.wordgame.words.connect":
    -
      MinGameVersion: "3.715.0"
      MaxGameVersion: "*"
      Url: "https://zenlifegames.com/privacy-policy"
  "com.word.wordconnect":
    -
      MinGameVersion: "3.715.0"
      MaxGameVersion: "*"
      Url: "https://zenlifegames.com/privacy-policy"
`)

	type PrivacyConf struct {
		MinGameVersion string `yaml:"MinGameVersion"`
		MaxGameVersion string `yaml:"MaxGameVersion"`
		Url            string `yaml:"Url"`
	}
	type Conf struct {
		ProjPrivacy map[string][]*PrivacyConf `yaml:"ProjPrivacy"`
	}
	out := &Conf{}
	yaml.Unmarshal(content, out)

	fmt.Printf("yaml reslut %+v \n", out)
	confs, _ := out.ProjPrivacy["com.wordgame.words.connect"]
	for _, c := range confs {
		fmt.Printf("com.wordgame.words.connect config is %+v \n", c)
	}

}
