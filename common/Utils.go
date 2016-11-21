package common

import (
	"github.com/op/go-logging"
	"os"

	"encoding/json"
	"github.com/coreos/etcd/client"
	"golang.org/x/net/context"
	"time"
	"fmt"
	"github.com/hashicorp/consul/api"
	"strings"
	"strconv"
)

//---------------common

const SEPARATOR = "_"

func GenerateId(customerno string, platform string, tenantId string) string {
	return customerno + SEPARATOR + platform + SEPARATOR + tenantId
}

//---------------log

var (
	mylog = logging.MustGetLogger("main")
)

func init() {
	format := logging.MustStringFormatter(
		`[%{time:2006-01-02 15:04:05.000}] [%{level:.5s}] [%{shortfunc}:%{shortfile}] [%{callpath}] [-[%{message}]-]`,
	)
	leveledBackend := logging.AddModuleLevel(logging.NewBackendFormatter(logging.NewLogBackend(os.Stdout, "", 0), format))
	leveledBackend.SetLevel(logging.INFO, "")
	logging.SetBackend(leveledBackend)
}

func Log() *logging.Logger {
	return mylog
}

//---------------etcd

func EtcdService(url string, path string) (string, int) { // 加密服务只有一个进程

	cfg := client.Config{
		Endpoints:               []string{url},
		Transport:               client.DefaultTransport,
		HeaderTimeoutPerRequest: time.Second,
	}

	c, err := client.New(cfg)

	if err != nil {
		mylog.Error(err)
		os.Exit(1)
	}

	kapi := client.NewKeysAPI(c)

	ctx, _ := context.WithTimeout(context.Background(), 20*time.Second)

	resp, err := kapi.Get(ctx, path, &client.GetOptions{Sort: true, Recursive: true, Quorum: true})

	var node string

	if err != nil {
		mylog.Fatal(err)
	} else {
		mylog.Infof("%q key has %q value\n", resp.Node.Key, resp.Node.Value)
		mylog.Infof("%q key has %q value\n", resp.Node.Key, resp.Node.Nodes)
		for _, a := range resp.Node.Nodes {
			mylog.Info(a)
			node = a.Value
			break
		}
	}

	var etcdNode EtcdNode

	json.Unmarshal([]byte(node), &etcdNode)

	mylog.Info(etcdNode)

	return etcdNode.Host, etcdNode.Port

}

func ConsulService(url string, path string) (string, int) {// 加密服务只有一个进程
	config := api.DefaultConfig()
	config.Address = url
	client, err := api.NewClient(config)
	if err != nil {
		panic(err)
	}
	catalog := client.Catalog()
	service, _, err := catalog.Service(path, "", nil);

	if err != nil {
		panic(err)
	}
	fmt.Printf("KV: %v", service[0].ServiceTags)

	var port int
	for _, str := range service[0].ServiceTags {
		if strings.Contains(str, "PORT_8080") {
			mylog.Info("解析consul port:", str)
			port, _ = strconv.Atoi(strings.SplitAfter(str, "=")[1])
		}
	}
	return service[0].ServiceAddress, port

}
