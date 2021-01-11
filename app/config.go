package app

import (
	"flag"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"os"
	"path"
)

var Conf *Config

type Client struct {
	Uri string `yaml:"uri" json:"-"`
}

type Server struct {
	Uri string `yaml:"uri" json:"-"`
}

type Mongo struct {
	Uri      string `yaml:"uri" json:"-"`
	Database string `yaml:"database" json:"-"`
}

type Log struct {
	Filename string `yaml:"filename" json:"-"`
}

type Config struct {
	Client *Client `yaml:"server" json:"-"`
	Server *Server `yaml:"server" json:"-"`
	Mongo  *Mongo  `yaml:"mongo" json:"-"`
	Log    *Log    `yaml:"log" json:"-"`
}

func LoadConfig(configPath string) {
	configData, err := ioutil.ReadFile(configPath)
	if err != nil {
		log.Fatalln(err)
	}
	if err := yaml.Unmarshal(configData, &Conf); err != nil {
		log.Fatalln(err)
	}
}

func InitConfig() {
	configPathDefault, _ := os.Getwd()
	configPathDefault = path.Join(configPathDefault, "example.yml")
	//configPathDefault = path.Join(configPathDefault, "dev.yml")
	configPath := flag.String("c", configPathDefault, "config file")
	flag.Parse()
	LoadConfig(*configPath)
}
