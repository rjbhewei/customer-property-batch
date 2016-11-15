package main

import (
	"flag"
	"os"
	"os/signal"
	"strings"
	"time"
	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/consumergroup"
	"github.com/wvanbergen/kazoo-go"
	"encoding/json"
	"gopkg.in/olivere/elastic.v2"
	"github.com/rjbhewei/customer-property-batch/common"
)

const (
	DefaultKafkaTopics = "bbb"
	DefaultConsumerGroup = "KafkaToES"
	DefaultZookeeper = "172.18.2.121:2181,172.18.2.40:2181,172.18.2.39:2181"
	DefaultUrls = "172.18.2.39:9200"
	DefaultESIndex = "custom-property"
	DefaultESType = "customer"
	SCRIPT_STRING = "if(ctx._source.properties.any{it.id==property.id}){i=0;ctx._source.properties.each({if(it.id==property.id){ctx._source.properties[i]=property;};++i;});}else{ctx._source.properties+=property;}"
)

var (
	mylog = common.Log()
)

var (
	consumerGroup = flag.String("group", DefaultConsumerGroup, "consumer group的名字")
	kafkaTopicsCSV = flag.String("topics", DefaultKafkaTopics, "用逗号分隔topic")
	zookeeper = flag.String("zookeeper", DefaultZookeeper, "用逗号分隔zk信息")
	urls = flag.String("urls", DefaultUrls, "用逗号分隔es url信息")
	ESIndex = flag.String("esindex", DefaultESIndex, "es的索引字段")
	ESType = flag.String("estype", DefaultESType, "es的type字段")
	zookeeperNodes []string
)

func main() {

	flag.Parse()

	if *zookeeper == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	config := consumergroup.NewConfig()

	config.Offsets.Initial = sarama.OffsetNewest

	config.Offsets.ProcessingTimeout = 10 * time.Second

	zookeeperNodes, config.Zookeeper.Chroot = kazoo.ParseConnectionString(*zookeeper)

	kafkaTopics := strings.Split(*kafkaTopicsCSV, ",")

	consumer, consumerErr := consumergroup.JoinConsumerGroup(*consumerGroup, kafkaTopics, zookeeperNodes, config)

	if consumerErr != nil {
		mylog.Error(consumerErr)
	}

	c := make(chan os.Signal, 1)

	signal.Notify(c, os.Interrupt)

	go func() {
		<-c
		if err := consumer.Close(); err != nil {
			mylog.Info("kafka客户端出现异常关闭", err)
		}
	}()

	go func() {
		for err := range consumer.Errors() {
			mylog.Error(err)
		}
	}()

	client, err := elastic.NewClient(elastic.SetURL(*urls), elastic.SetMaxRetries(3))

	if err != nil {
		mylog.Panic("创建es client error:", err)
	}

	mylog.Info("es信息:", client)

	eventCount := 0

	offsets := make(map[string]map[int32]int64)

	for message := range consumer.Messages() {

		if offsets[message.Topic] == nil {
			offsets[message.Topic] = make(map[int32]int64)
		}

		eventCount += 1

		if offsets[message.Topic][message.Partition] != 0 && offsets[message.Topic][message.Partition] != message.Offset - 1 {
			mylog.Infof("Unexpected offset on %s:%d. Expected %d, found %d, diff %d.\n", message.Topic, message.Partition, offsets[message.Topic][message.Partition] + 1, message.Offset, message.Offset - offsets[message.Topic][message.Partition] + 1)
		}

		mylog.Info("Offset:",message.Offset,"Partition:",message.Partition)

		var bean common.BatchUpdateBean

		err := json.Unmarshal(message.Value, &bean)

		if err != nil {
			mylog.Info("json 反序列化错误:", err)
		}

		mylog.Info(bean)

		s := client.Bulk()

		for index := 0; index < len(bean.Customers); index++ {

			property := &Property{
				Id:bean.PropertyId,
				Value:bean.Value,
			}

			info := &CustomerInfo{
				Customerno:bean.Customers[index],
				Platform:bean.Platform,
				TenantId:bean.TenantId,
				Properties:[]Property{*property},
			}

			mylog.Info(info)

			id := common.GenerateId(info.Customerno, info.Platform, info.TenantId)

			mylog.Info(id)

			propertyMap := map[string]string{
				"id":property.Id,
				"value":property.Value,
			}

			Request := elastic.NewBulkUpdateRequest().
				Index(*ESIndex).
				Type(*ESType).
				Id(id).
				RetryOnConflict(3).
				Script(SCRIPT_STRING).
				ScriptParams(map[string]interface{}{"property": propertyMap}).
				Upsert(info)

			mylog.Info(Request)

			s = s.Add(Request)
		}

		bulkResponse, err := s.Do()

		if err != nil {
			mylog.Info("error:", err)
		}
		if bulkResponse.Errors {
			mylog.Info("es bulk error")
		}

		for index := 0; index < len(bulkResponse.Items); index++ {
			item := bulkResponse.Items[index]
			mylog.Info(item);
		}

		offsets[message.Topic][message.Partition] = message.Offset

		consumer.CommitUpto(message)
	}

	mylog.Infof("Processed %d events.", eventCount)

	mylog.Infof("%+v", offsets)
}

type CustomerInfo struct {
	Id         string        `json:"id"`
	Customerno string        `json:"customerno"`
	Platform   string        `json:"platform"`
	TenantId   string        `json:"tenantId"`
	Properties []Property    `json:"properties"`
}

type Property struct {
	Id    string        `json:"id"`
	Value string        `json:"value"`
}