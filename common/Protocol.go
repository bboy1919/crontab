package common

import (
	"encoding/json"
	"github.com/gorhill/cronexpr"
	"strings"
	"time"
	"context"
)

type Job struct {
	Name      string `json:"name"`
	Command   string `json:"command"`
	CroneExpr string `json:"cronExpr"`
}

//http接口应答
type Response struct {
	ErrNo int         `json:"errno"`
	Msg   string      `json:"msg"`
	Data  interface{} `json:"data"`
}

//任务调度计划
type JobSchedulePlan struct {
	Job      *Job
	Expr     *cronexpr.Expression //解析好的cronexpr表达式
	NextTime time.Time            //下一次调度时间
}

//任务执行状态
type JobExecuteInfo struct {
	Job *Job	//任务信息
	PlanTime time.Time	//理论的调度时间
	RealTime time.Time	//实际的调度时间
	CancelCtx context.Context //任务command的context
	CancelFunc context.CancelFunc //用于取消command执行的cancel函数
}

//任务执行结果
type JobExecuteResult struct {
	ExecuteInfo *JobExecuteInfo	//执行状态数据
	Output []byte	//脚本输出
	Err error	//脚本错误原因
	StartTime time.Time	//启动时间
	EndTime time.Time	//结束时间
}

//任务执行日志
type JobLog struct {
	JobName string `json:"jobName" bson:"jobName"` //任务名字
	Command string `json:"command" bson:"command"`  //脚本命令
	Err string `json:"err" bson:"err"`	//错误原因
	Output string `json:"output" bson:"output"`	//脚本输出
	PlanTime int64 `json:"planTime" bson:"planTime"`  //计划开始时间
	ScheduleTime int64 `json:"scheduleTime" bson:"scheduleTime"`   //实际调度时间
	StartTime int64 `json:"startTime" bson:"startTime"`    //任务执行开始时间
	EndTime int64 `json:"endTime" bson:"endTime"`  //任务执行结束时间
}

//集中多条日志
type LogBatch struct {
	Logs []interface{}  //多条日志
}

//任务日志过滤条件
type JobLogFilter struct {
	JobName string `bson:"jobName"`
}

//任务日志配需规则
type SortLogByStartTime struct {
	SortOrder int `bson:"startTime"`
}

//调度事件
type JobEvent struct {
	EventType int
	Job       *Job
}

//应答方法
func BuildResponse(errno int, msg string, data interface{}) (resp []byte, err error) {
	//1、定义一个resopnse
	var (
		response Response
	)

	response.ErrNo = errno
	response.Msg = msg
	response.Data = data

	//2、序列化json
	resp, err = json.Marshal(response)
	return
}

//反序列化job
func UnpackJob(value []byte) (ret *Job, err error) {
	var (
		job *Job
	)

	job = &Job{}
	if err = json.Unmarshal(value, job); err != nil {
		return
	}
	ret = job
	return
}

//从etcd的key中提取任务名
func ExtractJobName(jobKey string) string {
	return strings.TrimPrefix(jobKey, JOB_SAVE_DIR)
}

//从etcd的killer中的key提取任务名
func ExtractKillerName(jobKey string) string {
	return strings.TrimPrefix(jobKey, JOB_KILLER_DIR)
}

//从etcd的key中提取ip
func ExtractWorkIP(regKey string) string {
	return strings.TrimPrefix(regKey, JOB_WORKER_DIR)
}

func BuildJobEvent(eventType int, job *Job) (jobEvent *JobEvent) {
	return &JobEvent{
		EventType: eventType,
		Job:       job,
	}
}

//构造任务执行计划
func BuildJobSchedulePlan(job *Job) (jobSchedulePlan *JobSchedulePlan, err error) {
	var (
		expr *cronexpr.Expression
	)

	//解析JOB的cron表达式
	if expr, err = cronexpr.Parse(job.CroneExpr); err != nil {
		return
	}

	//生成任务调度计划对象
	jobSchedulePlan = &JobSchedulePlan{
		Job:      job,
		Expr:     expr,
		NextTime: expr.Next(time.Now()),
	}
	return
}

//构造执行状态信息
func BuildJobExecuteInfo(jobSchedulePlan *JobSchedulePlan) (jobExecuteInfo *JobExecuteInfo){
	jobExecuteInfo = &JobExecuteInfo{
		Job: jobSchedulePlan.Job,
		PlanTime: jobSchedulePlan.NextTime,	//计算调度时间
		RealTime: time.Now(),	//真实调度时间
	}

	jobExecuteInfo.CancelCtx, jobExecuteInfo.CancelFunc = context.WithCancel(context.TODO())
	return
}