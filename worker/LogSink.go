package worker

import (
	"github.com/bboy1919/crontab/common"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"context"
	"time"
)

//mongodb存储日志
type LogSink struct {
	client         *mongo.Client
	logCollection  *mongo.Collection
	logChan        chan *common.JobLog
	autoCommitChan chan *common.LogBatch
}

var (
	G_logSink *LogSink
)

//发送日志
func (logSink *LogSink) Append(jobLog *common.JobLog) {
	select {
	case logSink.logChan <- jobLog:
	default:
		//队列满了就丢弃
	}
}

//批量写入日志
func (logSink *LogSink) saveLogs(batch *common.LogBatch) {
	logSink.logCollection.InsertMany(context.TODO(), batch.Logs)
}

//日志存储协程
func (logSink *LogSink) writeLoop() {
	var (
		log          *common.JobLog
		logBatch     *common.LogBatch
		commitTime   *time.Timer
		timeoutBatch *common.LogBatch
	)

	for {
		select {
		case log = <-logSink.logChan:
			if logBatch == nil {
				logBatch = &common.LogBatch{}

				//超时自动提交当前批次
				commitTime = time.AfterFunc(
					time.Duration(G_Config.JobLogCommitTimeout)*time.Millisecond,
					func(batch *common.LogBatch) func() {
						return func() {
							logSink.autoCommitChan <- logBatch
						}
					}(logBatch),
				)
			}

			logBatch.Logs = append(logBatch.Logs, log)

			if len(logBatch.Logs) >= G_Config.JobLogBatchSize {
				logSink.saveLogs(logBatch)
				logBatch = nil
				commitTime.Stop()
			}
		case timeoutBatch = <-logSink.autoCommitChan:

			//判断过期批次是否仍旧是当前的批次
			if timeoutBatch != logBatch {
				continue
			}
			//把批次写入到mongo中
			logSink.saveLogs(timeoutBatch)
			//清空logbatch
			logBatch = nil

		}
	}
}

//初始化
func InitLogSink() (err error) {
	var (
		client *mongo.Client
	)

	//建立mongodb连接
	if client, err = mongo.Connect(
		context.TODO(),
		options.Client().ApplyURI(G_Config.MongodbUri),
		options.Client().SetConnectTimeout(time.Duration(G_Config.MongodbConnectTimeout)*time.Millisecond),
	); err != nil {
		return
	}
	//选择db和cllection
	G_logSink = &LogSink{
		client:         client,
		logCollection:  client.Database("cron").Collection("log"),
		logChan:        make(chan *common.JobLog, 1000),
		autoCommitChan: make(chan *common.LogBatch, 1000),
	}

	go G_logSink.writeLoop()

	return
}
