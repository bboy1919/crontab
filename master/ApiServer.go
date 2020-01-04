package master

import (
	"net"
	"net/http"
	"strconv"
	"time"
)

//任务的HTTP接口
type ApiServer struct {
	httpServer *http.Server
}

//导出单例ApiServer对象
var (
	G_apiServer *ApiServer
)

//保存任务接口
func handleJobSave(http.ResponseWriter, *http.Request) {

}

//初始化服务
func InitApiServer() (err error) {
	var (
		mux        *http.ServeMux
		listener   net.Listener
		httpServer *http.Server
	)

	//配置路由
	mux = http.NewServeMux()
	mux.HandleFunc("/job/save", handleJobSave)

	//启动tcp监听
	if listener, err = net.Listen("tcp", ":"+strconv.Itoa(G_Config.ApiPort)); err != nil {
		return
	}

	//创建一个HTTP服务
	httpServer = &http.Server{
		ReadTimeout:  time.Duration(G_Config.ApiReadTimeout) * time.Millisecond,
		WriteTimeout: time.Duration(G_Config.ApiWriteTimeout) * time.Millisecond,
		Handler:      mux,
	}

	//赋值单例
	G_apiServer = &ApiServer{
		httpServer: httpServer,
	}

	//启动了服务端
	go httpServer.Serve(listener)

	return
}
