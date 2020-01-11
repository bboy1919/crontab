package main

import (
	"fmt"
	"github.com/bboy1919/crontab/worker"
	"runtime"
	"flag"
	"time"
)

var (
	confFile string //保存命令行输入的配置文件路径
)

func initArgs() {
	flag.StringVar(&confFile, "config", "./worker.json", "指定worker.json")
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
	if err = worker.InitConfig(confFile); err != nil {
		goto ERR
	}

	//服务注册
	if err = worker.InitRegister(); err != nil {
		goto ERR
	}

	//初始化线程数量
	initEnv()

	//启动日志
	if err = worker.InitLogSink(); err != nil {
		goto ERR
	}

	//启动执行器
	if err = worker.InitExecutor(); err != nil {
		goto ERR
	}

	//启动调度器
	if err = worker.InitScheduler(); err != nil {
		goto ERR
	}

	//任务管理器
	if err = worker.InitJobMgr(); err != nil {
		goto ERR
	}



	for {
		time.Sleep(1000 * time.Millisecond)
	}

ERR:
	fmt.Println(err)
}
