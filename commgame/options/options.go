package options

import (
	"flag"
	"github.com/Zentertain/zenlog"
)

var GameMode string
var Theme string
var Env string
var Port int
var WSPort int
var RobotNum int
var RobotIndex int
var RobotRate int
var HTTPPort int
var CPUProfile string
var MemProfile string
var BlockProfile string
var BlockProfileRate int
var MemProfileRate int
var Debug bool
var BuildVersion int

func Init() {
	mode := flag.String("m", "game", "game mode")
	theme := flag.String("t", "us", "theme")
	env := flag.String("e", "local", "env")
	port := flag.Int("p", 0, "tcp port")
	wsPort := flag.Int("wp", 0, "ws port")
	httpPort := flag.Int("hp", 0, "http port")
	robotNum := flag.Int("rn", 1, "robot num")
	robotIndex := flag.Int("ri", 0, "robot index")
	robotRate := flag.Int("rrate", 10, "robot login rate, user per second")
	cpuProfile := flag.String("cpf", "", "cpu profiling file")
	memProfile := flag.String("mpf", "", "memory profiling file")
	memProfileRate := flag.Int("mpfr", 0, "memory profiling rate")
	blockProfile := flag.String("bpf", "", "block profiling file")
	blockProfileRate := flag.Int("bpfr", 0, "block profiling rate")
	debug := flag.Bool("d", false, "debug mode")
	buildVersion := flag.Int("b", 0, "build version")
	flag.Parse()

	GameMode = *mode
	Theme = *theme
	Env = *env
	Port = *port
	WSPort = *wsPort
	HTTPPort = *httpPort
	CPUProfile = *cpuProfile
	MemProfile = *memProfile
	MemProfileRate = *memProfileRate
	BlockProfile = *blockProfile
	BlockProfileRate = *blockProfileRate
	RobotNum = *robotNum
	RobotIndex = *robotIndex
	RobotRate = *robotRate
	Debug = *debug
	BuildVersion = *buildVersion

	zenlog.Info("==== start params ====: Mode: %v, Theme: %v, Env: %v, CpuProfile: %v, MemProfile: %v", GameMode, Theme, Env, CPUProfile, MemProfile)

	// modes := []string{"master", "connector", "hall", "chat", "gm", "robot", "room", "word_game", "battle"}

	// if !toolbox.HasSliceAnyElements(modes, GameMode) {
	// 	panic("wrong mode param!")
	// }

	if RobotRate < 10 {
		panic("robot rate (rrate) must >= 10")
	}
}
