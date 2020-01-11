package worker

import (
	"github.com/coreos/etcd/clientv3"
	"context"
	"github.com/bboy1919/crontab/common"
)

//分布式锁（TXN事务）
type JobLock struct {
	kv clientv3.KV
	lease clientv3.Lease

	jobName string
	cancelFunc context.CancelFunc //用于终止自动续租
	leaseId clientv3.LeaseID  //租约id
	isLocked bool
}

//初始化一把锁
func InitJobLock(jobName string, kv clientv3.KV, lease clientv3.Lease) (jobLock *JobLock) {
	jobLock = &JobLock{
		kv: kv,
		lease: lease,
		jobName: jobName,
	}
	return
}

//尝试上锁
func (jobLock *JobLock) TryLock() (err error) {
	var(
		leaseGrantResp *clientv3.LeaseGrantResponse
		cancelCtx context.Context
		cancelFunc context.CancelFunc
		leaseId clientv3.LeaseID
		keepRespChan <-chan *clientv3.LeaseKeepAliveResponse
		txn clientv3.Txn
		locKey string
		txnResp *clientv3.TxnResponse
	)

	//创建租约（5秒）
	if leaseGrantResp, err = jobLock.lease.Grant(context.TODO(),5); err != nil {
		return
	}

	//context用于取消自动续租
	cancelCtx, cancelFunc = context.WithCancel(context.TODO())

	//租约ID
	leaseId = leaseGrantResp.ID

	//自动续租
	if keepRespChan, err = jobLock.lease.KeepAlive(cancelCtx, leaseId); err != nil {
		goto FAIL
	}

	//处理续租应答的协程
	go func() {
		var(
			keepResp *clientv3.LeaseKeepAliveResponse
		)

		for {
			select {
			case keepResp = <- keepRespChan:    //自动续租应答
				if keepResp == nil {
					goto END
				}
			}
		}
		END:
	}()

	//创建事务txn
	txn = jobLock.kv.Txn(context.TODO())

	//每一个任务对应锁的key
	locKey = common.JOB_LOCK_DIR + jobLock.jobName

	//事务抢锁
	txn.If(clientv3.Compare(clientv3.CreateRevision(locKey), "=", 0)).
		Then(clientv3.OpPut(locKey, "", clientv3.WithLease(leaseId))).
		Else(clientv3.OpGet(locKey))

	//提交事务
	if txnResp, err = txn.Commit(); err != nil {
		goto FAIL
	}

	//成功返回，失败释放租约
	if !txnResp.Succeeded {  //锁被占用
		err = common.ERR_LOCK_ALREADY_REQUIRED
		goto FAIL
	}

	//抢锁成功
	jobLock.leaseId = leaseId
	jobLock.cancelFunc = cancelFunc
	jobLock.isLocked = true
	return

FAIL:
	cancelFunc() //取消自动续租
	jobLock.lease.Revoke(context.TODO(), leaseId)    //释放租约
	return
}

func (jobLock *JobLock) Unlock() {
	if jobLock.isLocked {
		jobLock.cancelFunc()  //取消我们程序自动续租的协程
		jobLock.lease.Revoke(context.TODO(), jobLock.leaseId)  //释放租约
	}

}