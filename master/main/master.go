package main

import (
	"fmt"
	"github.com/bboy1919/crontab/master"
	"runtime"

	"flag"
)

var (
	confFile string //配置文件路径
)

func initArgs() {
	flag.StringVar(&confFile, "config", "./master.json", "指定master.json")
	flag.Parse()
}

func initEnv() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {

	var (
		err error
	)

	//初始化命令行参数
	initArgs()

	//加载配置文件
	if err = master.InitConfig(confFile); err != nil {
		goto ERR
	}

	//初始化线程数量
	initEnv()

	//启动Api HTTP服务
	if err = master.InitApiServer(); err != nil {
		goto ERR
	}

ERR:
	fmt.Println(err)
}
