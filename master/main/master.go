package main

import (
	"fmt"
	"github.com/bboy1919/crontab/master"
	"runtime"
	"flag"
	"time"
)

var (
	confFile string //保存命令行输入的配置文件路径
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

	//初始化服务发现模块
	if err = master.InitWorkerMgr(); err != nil {
		goto ERR
	}

	//日志管理器
	if err = master.InitLogMgr(); err != nil {
		goto ERR
	}

	//任务管理器
	if err = master.InitJobMgr(); err != nil {
		goto ERR
	}

	//启动Api HTTP服务
	if err = master.InitApiServer(); err != nil {
		goto ERR
	}

	for {
		time.Sleep(1000 * time.Millisecond)
	}

ERR:
	fmt.Println(err)
}
