package worker

import (
	"encoding/json"
	"io/ioutil"
)

type Config struct {
	EtcdEndpoint    []string `json:"etcdEndpoint"`
	EtcdDialTimeout int      `json:"etcdDialTimeout"`
	MongodbUri string `json:"mongodbUri"`
	MongodbConnectTimeout int  `json:"mongodbConnectTimeout"`
	JobLogBatchSize int `json:"jobLogBatchSize"`
	JobLogCommitTimeout int `json:"jobLogCommitTimeout"`
}

var (
	G_Config *Config
)

//加载配置文件
func InitConfig(filename string) (err error) {
	var (
		content []byte
		conf    Config
	)

	//1.把配置文件读进来
	if content, err = ioutil.ReadFile(filename); err != nil {
		return
	}

	//2.json序列化配置文件数据
	if err = json.Unmarshal(content, &conf); err != nil {
		return
	}

	//3、解析成功后赋值给单例
	G_Config = &conf

	return
}
