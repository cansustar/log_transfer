package main

import (
	"fmt"
	"github.com/go-ini/ini"
	"log_transfer/es"
	"log_transfer/kafka"
	"log_transfer/model"
)

func main() {
	var cfg = new(model.Config)
	err := ini.MapTo(cfg, "./config/logtransfer.ini")
	if err != nil {
		// log.Fatalf的作用：打印错误信息，并且退出程序
		fmt.Printf("init config failed, err:%v\n", err)
		// q: log.Fatalf和panic一样的吗
		// a: 不一样，log.Fatalf只是打印错误信息，并且退出程序，但是panic会打印错误信息，并且退出程序，但是panic会先执行defer中的代码
		//log.Fatalf("init config failed, err:%v\n", err)
		panic(err)
	}
	fmt.Println("init config success")
	// 这里es和kafka的
	// 3. 初始化ES连接
	err = es.Init(cfg.ESConf.Address, cfg.ESConf.Index, cfg.ESConf.MaxSize, cfg.ESConf.GoroutineNum)
	if err != nil {
		fmt.Printf("init es failed, err:%v\n", err)
		panic(err)
	}
	fmt.Println("init es success")
	// 2.初始化kafka连接
	err = kafka.Init([]string{cfg.KafkaConf.Address}, cfg.KafkaConf.Topic)
	if err != nil {
		fmt.Printf("init kafka failed, err:%v\n", err)
		panic(err)
	}
	fmt.Println("init kafka success")

	select {}

}
