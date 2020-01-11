package common

const (
	//任务保存目录
	JOB_SAVE_DIR   = "/cron/job/"
	JOB_KILLER_DIR = "/cron/killer/"
	JOB_LOCK_DIR = "/cron/lock/"

	JOB_EVENT_SAVE   = 1
	JOB_EVENT_DELETE = 2
	JOB_EVENT_KILL = 3
)
