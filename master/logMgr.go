package master

import (
	"go.mongodb.org/mongo-driver/mongo"
	"context"
	"time"
	"go.mongodb.org/mongo-driver/mongo/options"
	"github.com/bboy1919/crontab/common"
)

type LogMgr struct {
	client *mongo.Client
	logCollection *mongo.Collection
}

var(
	G_logMgr *LogMgr
)

func InitLogMgr() (err error) {
	var(
		client *mongo.Client
	)

	if client, err = mongo.Connect(
		context.TODO(),
		options.Client().ApplyURI(G_Config.MongodbUri),
		options.Client().SetConnectTimeout(time.Duration(G_Config.MongodbConnectTimeout) * time.Millisecond),
		); err != nil {
			return
	}

	G_logMgr = &LogMgr{
		client: client,
		logCollection: client.Database("cron").Collection("log"),
	}

	return
}


//查询任务日志
func (logMgr *LogMgr) ListLog(name string, skip int, limit int) (logArr []*common.JobLog, err error) {
	var(
		filter *common.JobLogFilter
		logSort *common.SortLogByStartTime
		cursor *mongo.Cursor
		jobLog *common.JobLog
	)

	logArr = make([]*common.JobLog, 0)

	filter = &common.JobLogFilter{JobName: name}
	logSort = &common.SortLogByStartTime{SortOrder: -1}

	if cursor, err = logMgr.logCollection.Find(
		context.TODO(),
		filter,
		options.Find().SetSort(logSort),
		options.Find().SetSkip(int64(skip)),
		options.Find().SetLimit(int64(limit)),
		); err != nil {
		return
	}

	defer cursor.Close(context.TODO())

	for cursor.Next(context.TODO()) {
		jobLog = &common.JobLog{}

		if err = cursor.Decode(jobLog); err != nil {
			continue
		}

		logArr = append(logArr, jobLog)
	}

	return
}