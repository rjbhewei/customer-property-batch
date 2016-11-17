package main

import (
	"github.com/valyala/fasthttp"
	"flag"
	"os"
	"strings"
	"github.com/Shopify/sarama"
	"encoding/json"
	"time"
	"github.com/rjbhewei/customer-property-batch/common"
)

var (
	mylog = common.Log()
	MStatusOK = string("{\"message\":\"成功了\"}")
	MStatusMethodNotAllowed = string("{\"message\":\"请求方法错误\"}")
	MStatusBadRequest = string("{\"message\":\"请求json解析错误\"}")
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

	if *brokers == "" || *topic == "" || *addr == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	brokerList := strings.Split(*brokers, ",")

	mylog.Infof("kakfa服务器列表: %s", strings.Join(brokerList, ", "))

	producer := asyncProducer(brokerList);

	server := &Server{
		producer: producer,
	}

	defer func() {
		if err := server.close(); err != nil {
			mylog.Error("Failed to close server", err)
		}
	}()

	mylog.Error(server.run())
}

func (s *Server) run() error {

	h := s.handleFastHTTP

	if *compress {
		h = fasthttp.CompressHandler(h)
	}

	mylog.Infof("Listening for requests on %s...", *addr)

	server := &fasthttp.Server{
		Handler: h,
	}

	server.MaxRequestBodySize = 100 * 1024 * 1024

	return server.ListenAndServe(*addr)
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
		mylog.Error("启动kafka异步生产者失败:", err)
	}
	return producer
}

func (s *Server) close() error {
	if err := s.producer.Close(); err != nil {
		mylog.Error("关键kafka生产者失败", err)
	}
	return nil
}

type Server struct {
	producer sarama.AsyncProducer
}

func (s *Server) handleFastHTTP(ctx *fasthttp.RequestCtx) {

	if string(ctx.Method()) != "PUT" {
		toctx(ctx, fasthttp.StatusMethodNotAllowed, MStatusMethodNotAllowed)
		return
	}

	mylog.Info("request body len:", len(ctx.PostBody()));

	var bean common.BatchUpdateBean

	err := json.Unmarshal(ctx.PostBody(), &bean)

	if err != nil {
		mylog.Error("json error:", err)
		toctx(ctx, fasthttp.StatusBadRequest, MStatusBadRequest)
		return
	}

	mylog.Info("customer array len:", len(bean.Customers));

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
		mylog.Info("index:", index)
		start := index
		end := index + sendMaxNum
		if (end > cLen) {
			end = cLen
		}
		tmpCustomers := bean.Customers[start:end]
		tmpBean.Customers = tmpCustomers;
		mylog.Info("分割后的tmpCustomer长度:", len(tmpBean.Customers))
		j, _ := json.Marshal(tmpBean)
		tmpBean.Customers = nil
		s.producer.Input() <- &sarama.ProducerMessage{
			Topic: *topic,
			Value: sarama.StringEncoder(string(j)),
		}
		index = index + sendMaxNum;
		loop++
	}

	mylog.Info("loop:", loop)

	for i := 0; i < loop; i++ {
		select {
		case msg := <-s.producer.Errors():
			mylog.Error(msg.Err)
		case msg := <-s.producer.Successes():
			mylog.Infof("Offset:%d,Partition:%d", msg.Offset, msg.Partition)
		}
	}
	mylog.Info("one batch over")
	toctx(ctx, fasthttp.StatusOK, MStatusOK)
}

func toctx(ctx *fasthttp.RequestCtx, statusCode int, body string) {
	ctx.SetStatusCode(statusCode)
	ctx.SetBodyString(body)
	ctx.SetContentType("application/json; charset=utf-8")
	//fmt.Fprintf(ctx, MStatusOK)
}

//irisConfig := config.Iris{MaxRequestBodySize: 100*1024*1024}
//www := iris.New(irisConfig)

//partition, offset, err := s.producer.SendMessage(&sarama.ProducerMessage{
//	Topic: "aaa",
//	Value: sarama.StringEncoder(string(j)),
//})
//if err != nil {
//	mylog.Println("error:", err)
//} else {
//	mylog.Println(partition, offset)
//}

//func syncProducer(brokerList []string) sarama.SyncProducer {
//	config := sarama.NewConfig()
//	config.Producer.RequiredAcks = sarama.WaitForAll
//	config.Producer.Retry.Max = 3
//	//config.Producer.MaxMessageBytes=100*1024*1024
//	producer, err := sarama.NewSyncProducer(brokerList, config)
//	if err != nil {
//		mylog.Error("启动kafka同步生产者失败:", err)
//	}
//	return producer
//}

//go func() {
//	for err := range producer.Errors() {
//		mylog.Println("kafka异步生产者出现错误:", err)
//	}
//}()
