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
	"github.com/rjbhewei/customer-property-batch/common"
	"encoding/json"
	"gopkg.in/olivere/elastic.v3"
	"runtime"
	"google.golang.org/grpc"
	pb "github.com/rjbhewei/customer-property-batch/helloworld"
	"golang.org/x/net/context"
	"fmt"
)

const (
	DefaultKafkaTopics = "bbb"
	DefaultConsumerGroup = "KafkaToES"
	DefaultZookeeper = "172.18.2.121:2181,172.18.2.40:2181,172.18.2.39:2181"
	DefaultUrls = "172.18.2.179:9200"
	DefaultESIndex = "customer2"
	DefaultESType = "customer"
	DefaultEtcd = "http://172.18.21.62:2379"
	DefaultConsul = "10.117.8.138:8500"
	//DefaultServicePath = "/service/local/platform/qa/cryptserver/1.0" //etcd
	DefaultServicePath = "/cryptserver/1.0"
	SCRIPT_STRING = "if(ctx._source.properties.any{it.id==property.id}){i=0;ctx._source.properties.each({if(it.id==property.id){ctx._source.properties[i]=property;};++i;});}else{ctx._source.properties+=property;}"
)

var (
	mylog = common.Log()
)

var (
	consumerGroup = flag.String("group", DefaultConsumerGroup, "consumer group的名字")
	kafkaTopics = flag.String("topics", DefaultKafkaTopics, "用逗号分隔topic")
	zookeeper = flag.String("zookeeper", DefaultZookeeper, "用逗号分隔zk信息")
	cpuratio = flag.Int("cpuratio", 2, "GOMAXPROCS=runtime.NumCPU()*cpuratio")
	urls = flag.String("urls", DefaultUrls, "用逗号分隔es url信息")
	ESIndex = flag.String("esindex", DefaultESIndex, "es的索引字段")
	ESType = flag.String("estype", DefaultESType, "es的type字段")
	ectd = flag.String("etcd", DefaultEtcd, "访问加密微服务")
	consul = flag.String("consul", DefaultConsul, "访问加密微服务")
	servicePath = flag.String("servicePath", DefaultServicePath, "访问加密微服务")
	zookeeperNodes []string
)

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU() * *cpuratio)

	flag.Parse()

	if *zookeeper == "" || *kafkaTopics == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	config := consumergroup.NewConfig()
	config.Offsets.Initial = sarama.OffsetNewest
	config.Offsets.ProcessingTimeout = 10 * time.Second

	zookeeperNodes, config.Zookeeper.Chroot = kazoo.ParseConnectionString(*zookeeper)

	kafkaTopics := strings.Split(*kafkaTopics, ",")

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

	encryptClient := encryptClient()

	for message := range consumer.Messages() {

		if offsets[message.Topic] == nil {
			offsets[message.Topic] = make(map[int32]int64)
		}

		eventCount += 1

		if offsets[message.Topic][message.Partition] != 0 && offsets[message.Topic][message.Partition] != message.Offset - 1 {
			mylog.Infof("Unexpected offset on %s:%d. Expected %d, found %d, diff %d.\n", message.Topic, message.Partition, offsets[message.Topic][message.Partition] + 1, message.Offset, message.Offset - offsets[message.Topic][message.Partition] + 1)
		}

		mylog.Debugf("Offset:%d,Partition:%d", message.Offset, message.Partition)

		var bean1 common.BatchUpdateBean

		err := json.Unmarshal(message.Value, &bean1)

		if err != nil {
			mylog.Info("json 反序列化错误:", err)
			continue
		}

		mylog.Debug(bean1)

		bean1Len := len(bean1.Customers)

		bean, err := encryptClient.ToEncrypt(context.Background(), &pb.EncryptRequest{
			Customers: bean1.Customers,
			Platform: bean1.Platform,
			TenantId: bean1.TenantId,
			Value: bean1.Value,
			PropertyId: bean1.PropertyId,
		})

		bean2Len := len(bean.Customers)

		if err != nil {
			//encryptClient := encryptClient()
			//bean, err = encryptClient.ToEncrypt(context.Background(), &pb.EncryptRequest{
			//	Customers: bean.Customers,
			//	Platform: bean.Platform,
			//	TenantId: bean.TenantId,
			//	Value: bean.Value,
			//	PropertyId: bean.PropertyId,
			//})
		}

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

			mylog.Debug(info)

			id := common.GenerateId(info.Customerno, info.Platform, info.TenantId)

			mylog.Debug(id)

			propertyMap := map[string]string{
				"id":property.Id,
				"value":property.Value,
			}

			script := elastic.NewScriptInline(SCRIPT_STRING).Param("property", propertyMap)

			Request := elastic.NewBulkUpdateRequest().
				Index(*ESIndex).
				Type(*ESType).
				Id(id).
				RetryOnConflict(3).
				Script(script).
				Upsert(info)

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
			mylog.Debug(item);
		}

		offsets[message.Topic][message.Partition] = message.Offset

		consumer.CommitUpto(message)

		mylog.Infof("加密前数据:%d,加密后数据:%d", bean1Len, bean2Len)

	}

	mylog.Infof("Processed %d events.", eventCount)

	mylog.Infof("%+v", offsets)
}

func encryptClient() pb.EncryptClient {

	host, port := common.ConsulService(*consul, *servicePath)
	//host, port :="172.18.21.106",8888

	mylog.Infof("host:%s,port:%d", host, port)

	conn, err := grpc.Dial(fmt.Sprint(host, ":", port), grpc.WithInsecure())

	if err != nil {
		mylog.Errorf("did not connect: %v", err)
	}

	return pb.NewEncryptClient(conn)
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