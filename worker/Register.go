package worker

import (
	"github.com/coreos/etcd/clientv3"
	"net"
	"time"
	"github.com/bboy1919/crontab/common"
	"context"
)

type Register struct {
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease

	localIP string
}

var(
	G_register *Register
)

func getLocalIP() (ipv4 string, err error) {
	var(
		addrs []net.Addr
		addr net.Addr
		ipNet *net.IPNet
		isIpNet bool
	)

	if addrs, err = net.InterfaceAddrs(); err != nil {
		return
	}

	for _, addr = range addrs {
		if ipNet, isIpNet = addr.(*net.IPNet); isIpNet && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil {
				ipv4 = ipNet.IP.String()
				return
			}
		}
	}

	err = common.ERR_NO_LOCAL_IP_FOUND

	return
}

//注册到/cron/workers/ip,并自动续租
func (register *Register) keepOnline(){

	var(
		regKey string
		leaseGrantResp *clientv3.LeaseGrantResponse
		err error
		keepAliveChan <-chan *clientv3.LeaseKeepAliveResponse
		keepAliveResp *clientv3.LeaseKeepAliveResponse

		cancelCtx context.Context
		cancelFunc context.CancelFunc
	)

	for {
		regKey = common.JOB_WORKER_DIR + register.localIP

		cancelFunc = nil
		//创建租约
		if leaseGrantResp, err = register.lease.Grant(context.TODO(), 10); err != nil {
			goto RETRY
		}

		//自动续租
		if keepAliveChan, err = register.lease.KeepAlive(context.TODO(), leaseGrantResp.ID); err !=nil {
			goto RETRY
		}

		cancelCtx, cancelFunc = context.WithCancel(context.TODO())

		//注册到etcd
		if _, err = register.kv.Put(cancelCtx, regKey, "", clientv3.WithLease(leaseGrantResp.ID)); err != nil {
			goto RETRY
		}

		//处理续租应答
		for {
			select {
			case keepAliveResp = <-keepAliveChan:
				if keepAliveResp == nil {
					//续租失败
					goto RETRY
				}
			}
		}

		RETRY:
			time.Sleep(1 * time.Second)
		if cancelFunc != nil {
			cancelFunc()
		}
	}


}

func  InitRegister() (err error) {
	var (
		config  clientv3.Config
		client  *clientv3.Client
		kv      clientv3.KV
		lease   clientv3.Lease
		localIp string
	)

	//初始化配置
	config = clientv3.Config{
		Endpoints:   G_Config.EtcdEndpoint,
		DialTimeout: time.Duration(G_Config.EtcdDialTimeout) * time.Millisecond,
	}

	if client, err = clientv3.New(config); err != nil {
		return
	}

	if localIp, err = getLocalIP(); err != nil {
		return
	}

	//得到KV和LEASE的api子集
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)


	G_register = &Register{
		client:  client,
		kv:      kv,
		lease:   lease,
		localIP: localIp,
	}

	go G_register.keepOnline()

	return
}