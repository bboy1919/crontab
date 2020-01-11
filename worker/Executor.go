package worker

import (
	"github.com/bboy1919/crontab/common"
	"os/exec"
	"time"
	"math/rand"
)

type Executor struct {

}

var(
	G_executor *Executor
)

//执行一个任务
func (executor *Executor) ExecuteJob(info *common.JobExecuteInfo) {
	go func() {
		var(
			cmd *exec.Cmd
			err error
			output []byte
			result *common.JobExecuteResult
			jobLock *JobLock
		)

		result = &common.JobExecuteResult{
			ExecuteInfo: info,
			Output: make([]byte, 0),
		}

		//初始化分布式锁
		jobLock = G_jobMgr.CreateJobLock(info.Job.Name)

		result.StartTime = time.Now()

		//修正锁抢占偏向，增加毫秒级随机睡眠
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)

		err = jobLock.TryLock()
		defer jobLock.Unlock()

		if err != nil {
			result.Err = err
			result.EndTime = time.Now()
		} else {
			result.StartTime = time.Now()
			//执行shell命令
			cmd = exec.CommandContext(info.CancelCtx, "D:\\Program Files\\Git\\bin\\bash.exe", "-c", info.Job.Command)

			//执行并捕获输出
			output, err = cmd.CombinedOutput()

			result.EndTime = time.Now()
			result.Output = output
			result.Err = err

		}
		//任务执行完成后，把执行的结果返回给Scheduler，Scheduler会从executingTable中删除掉执行记录
		G_scheduler.PushJobResult(result)
	}()
}

//初始化执行器
func InitExecutor() (err error) {
	G_executor = &Executor{

	}
	return
}
