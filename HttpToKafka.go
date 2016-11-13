package main

import (
	"github.com/valyala/fasthttp"
	"fmt"
	"flag"
	"log"
	"os"
	"strings"
	"github.com/Shopify/sarama"
	"encoding/json"
	"time"
	"github.com/rjbhewei/customer-property-batch-kafka/common"
)

var (
	mylog = log.New(os.Stdout, "hewei", log.LstdFlags)
)

var (
	addr = flag.String("addr", ":8080", "http请求端口")
	compress = flag.Bool("compress", false, "响应数据否是压缩")
)

var (
	topic = flag.String("topic", "bbb", "kafka的topic")
	brokers = flag.String("brokers", "172.18.2.35:9092,172.18.2.36:9092,172.18.2.38:9092", "kafka连接地址,用逗号分隔")
)

func main() {

	flag.Parse()

	if *brokers == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	brokerList := strings.Split(*brokers, ",")

	mylog.Printf("kakfa服务器列表: %s", strings.Join(brokerList, ", "))

	producer := asyncProducer(brokerList);

	server := &Server{
		producer: producer,
	}

	defer func() {
		if err := server.close(); err != nil {
			log.Println("Failed to close server", err)
		}
	}()

	mylog.Fatal(server.run())
}

func (s *Server) run() error {
	//irisConfig := config.Iris{MaxRequestBodySize: 100*1024*1024}
	//www := iris.New(irisConfig)

	h := s.handleFastHTTP

	if *compress {
		h = fasthttp.CompressHandler(h)
	}

	mylog.Printf("Listening for requests on %s...\n", *addr)

	server := &fasthttp.Server{
		Handler: h,
	}

	server.MaxRequestBodySize = 100 * 1024 * 1024

	return server.ListenAndServe(*addr)
}

func syncProducer(brokerList []string) sarama.SyncProducer {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 3
	//config.Producer.MaxMessageBytes=100*1024*1024
	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		mylog.Fatalln("启动kafka同步生产者失败:", err)
	}
	return producer
}

func asyncProducer(brokerList []string) sarama.AsyncProducer {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Flush.Frequency = 500 * time.Millisecond
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	producer, err := sarama.NewAsyncProducer(brokerList, config)
	if err != nil {
		mylog.Fatalln("启动kafka异步生产者失败:", err)
	}
	//go func() {
	//	for err := range producer.Errors() {
	//		mylog.Println("kafka异步生产者出现错误:", err)
	//	}
	//}()
	return producer
}

func (s *Server) close() error {
	if err := s.producer.Close(); err != nil {
		mylog.Println("关键kafka生产者失败", err)
	}
	return nil
}

type Server struct {
	producer sarama.AsyncProducer
}

func (s *Server) handleFastHTTP(ctx *fasthttp.RequestCtx) {

	if string(ctx.Method()) != "PUT" {
		ctx.SetBody([]byte("请求方法错误"))
		return
	}

	mylog.Println("body len:", len(ctx.PostBody()));

	var bean common.BatchUpdateBean

	err := json.Unmarshal(ctx.PostBody(), &bean)

	if err != nil {
		mylog.Println("error:", err)
	}

	mylog.Println("customer array len:", len(bean.Customers));

	tmpBean := &common.BatchUpdateBean{
		Platform :bean.Platform,
		TenantId :bean.TenantId,
		Value :bean.Value,
		PropertyId:bean.PropertyId,
	}

	sendMaxNum := 10000;

	cLen := len(bean.Customers)

	loop := 0

	for index := 0; index < cLen; {
		mylog.Println("index:", index)
		start := index
		end := index + sendMaxNum
		if (end > cLen) {
			end = cLen
		}
		tmpCustomers := bean.Customers[start:end]
		tmpBean.Customers = tmpCustomers;
		mylog.Println("分割后的tmpCustomer长度", len(tmpBean.Customers))
		j, _ := json.Marshal(tmpBean)
		tmpBean.Customers = nil
		//partition, offset, err := s.producer.SendMessage(&sarama.ProducerMessage{
		//	Topic: "aaa",
		//	Value: sarama.StringEncoder(string(j)),
		//})
		//if err != nil {
		//	mylog.Println("error:", err)
		//} else {
		//	mylog.Println(partition, offset)
		//}
		s.producer.Input() <- &sarama.ProducerMessage{
			Topic: *topic,
			Value: sarama.StringEncoder(string(j)),
		}
		index = index + sendMaxNum;
		loop++
	}

	mylog.Print("loop:", loop)

	for i := 0; i < loop; i++ {
		select {
		case msg := <-s.producer.Errors():
			mylog.Println(msg.Err)
		case msg := <-s.producer.Successes():
			mylog.Println("Offset:", msg.Offset, "Partition:", msg.Partition)
		}
	}
	mylog.Print("all over")
	fmt.Fprintf(ctx, string("{\"message\":\"成功了\"}"))
	ctx.SetContentType("application/json; charset=utf-8")
}
