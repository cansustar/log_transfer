package es

// 将日志数据写入到ES中

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/olivere/elastic/v7"
	"net/http"
)

var (
	esClient *ESClient
)

type ESClient struct {
	client      *elastic.Client
	index       string
	logDataChan chan interface{}
}

func Init(addr, index string, maxSize int, goroutineNum int) (err error) {
	// ElasticSearch 服务器地址
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client, err := elastic.NewClient(elastic.SetURL("https://"+addr),
		elastic.SetSniff(false),       // 避免使用节点自动发现
		elastic.SetHealthcheck(false), // 关闭健康检查
		elastic.SetBasicAuth("elastic", "hUdmIm_dIZZjI84Qo_xW"),
		elastic.SetHttpClient(&http.Client{
			Transport: tr,
		})) // 如果有的话，替换为你的用户名和密码) )
	if err != nil {
		panic(err)
	}
	esClient = &ESClient{
		client:      client,
		index:       index,
		logDataChan: make(chan interface{}, maxSize),
	}

	fmt.Println("connect to es success")
	// 从通道中取数据，写入到es
	for i := 0; i < goroutineNum; i++ {
		go SendToES()
	}
	return
}

// PutLogData 通过一个首字母大写的函数， 从包外接收msg发送到chan中
func PutLogData(msg interface{}) {
	esClient.logDataChan <- msg
}

func SendToES() {
	for m1 := range esClient.logDataChan {
		//_, err := json.Marshal(m1)
		//if err != nil {
		//	fmt.Printf("marshal failed, err:%v\n", err)
		//	continue
		//}
		put1, err := esClient.client.Index().
			Index(esClient.index). //TODO: 优化，能不能存大文本而不是键值对
			BodyJson(m1).          // 如果获取到的不是json的，则报错：unmarshal failed, err:invalid character 'T' looking for beginning of value
			Do(context.Background())
		if err != nil {
			panic(err)
		}
		fmt.Printf("Indexed user %s to index %s, type %s\n", put1.Id, put1.Index, put1.Type)
	}
}
