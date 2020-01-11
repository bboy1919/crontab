package worker

import (
	"fmt"
	"github.com/bboy1919/crontab/common"
	"time"
)

type Scheduler struct {
	jobEventChan chan *common.JobEvent              //etcd任务事件队列
	jobPlanTable map[string]*common.JobSchedulePlan //任务调度计划表
	jobExecutingTable map[string] *common.JobExecuteInfo   //任务执行表
	jobResultChan chan *common.JobExecuteResult
}

var (
	G_scheduler *Scheduler
)

//处理任务结果
func (scheduler *Scheduler) handleJobResult(jobResult *common.JobExecuteResult){

	var(
		jobLog *common.JobLog
	)
	//该项执行命令完成，删除执行状态表中的相关数据
	delete(scheduler.jobExecutingTable, jobResult.ExecuteInfo.Job.Name)

	//生成执行日志
	if jobResult.Err != common.ERR_LOCK_ALREADY_REQUIRED {
		jobLog = &common.JobLog{
			JobName: jobResult.ExecuteInfo.Job.Name,
			Command: jobResult.ExecuteInfo.Job.Command,
			Output: string(jobResult.Output),
			PlanTime: jobResult.ExecuteInfo.PlanTime.UnixNano() /1000 /1000,
			ScheduleTime: jobResult.ExecuteInfo.RealTime.UnixNano() /1000 /1000,
			StartTime: jobResult.StartTime.UnixNano() /1000 /1000,
			EndTime: jobResult.EndTime.UnixNano() /1000 /1000,
		}
	}

	if jobResult.Err != nil {
		jobLog.Err = jobResult.Err.Error()
	} else {
		jobLog.Err = ""
	}
	//fmt.Println("任务执行完成：", jobResult.Err, string(jobResult.Output))
	//TOTO:存储日志到mongo
	G_logSink.Append(jobLog)
}

//回传任务执行结果
func (scheduler *Scheduler) PushJobResult(jobResult *common.JobExecuteResult)  {
	scheduler.jobResultChan <- jobResult
}

//尝试执行任务
func (scheduler *Scheduler) TryStartJob(jobPlan *common.JobSchedulePlan) {
	var(
		jobExecuteInfo *common.JobExecuteInfo
		jobExecuting bool
	)

	//如果任务正在执行，跳过本次调度
	if jobExecuteInfo, jobExecuting = scheduler.jobExecutingTable[jobPlan.Job.Name]; jobExecuting {
		fmt.Println("尚未退出，跳过执行：", jobPlan.Job.Name)
		return
	}

	//如果任务没有在执行表中，则构建执行状态信息
	jobExecuteInfo = common.BuildJobExecuteInfo(jobPlan)

	//加入本次执行状态信息到执行表中
	scheduler.jobExecutingTable[jobPlan.Job.Name] = jobExecuteInfo

	//执行任务
	fmt.Println("执行任务:", jobExecuteInfo.Job.Name, jobExecuteInfo.PlanTime, jobExecuteInfo.RealTime)
	G_executor.ExecuteJob(jobExecuteInfo)
	//fmt.Println("执行任务:", jobExecuteInfo.Job.Name, jobExecuteInfo.PlanTime, jobExecuteInfo.RealTime)

}

//计算任务状态，执行任务计划中的任务
func (scheduler *Scheduler) TrySchedule() (scheduleAfter time.Duration) {
	var (
		jobPlan  *common.JobSchedulePlan
		now      time.Time
		nearTime *time.Time
	)

	if len(scheduler.jobPlanTable) == 0 {
		scheduleAfter = 1 * time.Second
		return
	}

	now = time.Now()

	//遍历所有任务
	for _, jobPlan = range scheduler.jobPlanTable {
		if jobPlan.NextTime.Before(now) || jobPlan.NextTime.Equal(now) {
			//尝试执行任务
			scheduler.TryStartJob(jobPlan)
			jobPlan.NextTime = jobPlan.Expr.Next(now) //更新下次执行时间
		}

		//统计最近一个要过期的任务时间
		if nearTime == nil || jobPlan.NextTime.Before(*nearTime) {
			nearTime = &jobPlan.NextTime
		}
	}

	//下次调度间隔（最近的需要调度的任务的间隔时间）
	scheduleAfter = (*nearTime).Sub(now)
	return
}


//推送任务变化事件
func (scheduler *Scheduler) PushJobEvent(jobEvent *common.JobEvent) {
	scheduler.jobEventChan <- jobEvent
}

//处理任务事件
func (scheduler *Scheduler) handleJobEvent(jobEvent *common.JobEvent) {
	var (
		jobSchedulePlan *common.JobSchedulePlan
		jobExisted      bool
		err             error
		jobExecuteinfo *common.JobExecuteInfo
		jobExisting bool
	)

	switch jobEvent.EventType {
	case common.JOB_EVENT_SAVE:
		if jobSchedulePlan, err = common.BuildJobSchedulePlan(jobEvent.Job); err != nil {
			return
		}
		scheduler.jobPlanTable[jobEvent.Job.Name] = jobSchedulePlan
	case common.JOB_EVENT_DELETE:
		if jobSchedulePlan, jobExisted = scheduler.jobPlanTable[jobEvent.Job.Name]; jobExisted {
			delete(scheduler.jobPlanTable, jobEvent.Job.Name)
		}
	case common.JOB_EVENT_KILL:
		if jobExecuteinfo, jobExisting = scheduler.jobExecutingTable[jobEvent.Job.Name]; jobExisting {
			jobExecuteinfo.CancelFunc()
		}
	}
}

//调度协程实现
func (scheduler *Scheduler) schedulerLoop() {

	var (
		jobEvent      *common.JobEvent
		scheduleAfter time.Duration
		scheduleTimer *time.Timer
		jobResult *common.JobExecuteResult
	)

	//初始化一次
	scheduleAfter = scheduler.TrySchedule()

	//调度的延时定时器
	scheduleTimer = time.NewTimer(scheduleAfter)

	for {
		select {
		case jobEvent = <-scheduler.jobEventChan: //监听任务变化事件
			scheduler.handleJobEvent(jobEvent)
		case <-scheduleTimer.C: //最近的任务到期了
		case jobResult =<- scheduler.jobResultChan:
			scheduler.handleJobResult(jobResult)
		}

		scheduleAfter = scheduler.TrySchedule()

		//重置定时器
		scheduleTimer.Reset(scheduleAfter)
	}
}

//初始化调度器
func InitScheduler() (err error) {
	G_scheduler = &Scheduler{
		jobEventChan: make(chan *common.JobEvent, 1000),
		jobPlanTable: make(map[string] *common.JobSchedulePlan),
		jobExecutingTable: make(map[string] *common.JobExecuteInfo),
		jobResultChan: make(chan *common.JobExecuteResult, 1000),
	}

	//启动调度协程
	go G_scheduler.schedulerLoop()
	return
}
