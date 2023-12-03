package kafka

import (
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"log_transfer/es"
)

// 初始化kafka连接
// 从kafka里面取出日志数据

func Init(address []string, topic string) (err error) {
	//// 1. 消费者配置
	consumer, err := sarama.NewConsumer(address, nil)
	if err != nil {
		fmt.Printf("fail to start consumer, err:%v\n", err)
		return
	}
	partitionList, err := consumer.Partitions(topic) // 根据topic取到所有的分区
	if err != nil {
		fmt.Printf("fail to get list of partition: err:%v\n", err)
		return
	}
	// 注释：这里的partitionList是一个切片，里面存放的是分区的编号
	var pc sarama.PartitionConsumer
	for partition := range partitionList {
		// 针对每个分区创建一个对应的分区消费者，读取时更快
		pc, err = consumer.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
		if err != nil {
			fmt.Printf("failed to start consumer for partition %d, err: %v\n", partition, err)
			return
		}
		//q: defer的位置有讲究吗
		// a: defer的位置是有讲究的，defer的作用是在函数退出时执行，如果defer的位置在for循环里面，那么每次循环都会执行defer，这样会导致资源泄露，所以defer的位置要在for循环外面
		// AsyncClose() 用于异步关闭 什么是异步关闭：当调用了AsyncClose()后，不会立即关闭，而是会在消费完当前这个批次的消息后再关闭
		//defer pc.AsyncClose()
		// 异步从每个分区消费信息
		go func(sarama.PartitionConsumer) {
			// go创建的goroutine的函数相当于闭包，要访问外面的变量，需要捕获变量，捕获的变量就在go func {} ()的（）里
			for msg := range pc.Messages() {
				fmt.Printf("partition: %d Offset: %d key: %s Value: %s\n", msg.Partition, msg.Offset, msg.Key, msg.Value)
				// logDataChan <- msg // 为了实现将读日志与发送日志异步执行，将读取到的日志数据发送到一个通道中
				var m1 map[string]interface{}
				err := json.Unmarshal(msg.Value, &m1)
				if err != nil {
					fmt.Printf("unmarshal failed, err:%v\n", err)
					continue
				}
				es.PutLogData(m1)
			}
		}(pc)
	}
	return
}
