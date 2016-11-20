package main

import (
	"fmt"
	"github.com/hashicorp/consul/api"
	"strings"
	"log"
	"strconv"
)

func main() {

	config:=api.DefaultConfig()
	config.Address="10.117.8.138:8500"
	client, err := api.NewClient(config)
	if err != nil {
		panic(err)
	}
	catalog :=client.Catalog()
	service, _, err :=catalog.Service("/cryptserver/1.0","",nil);

	if err != nil {
		panic(err)
	}
	fmt.Printf("KV: %v \n", service[0].ServiceTags)
	fmt.Printf("KV: %v \n", service[0].Address)
	fmt.Printf("KV: %v \n", service[0].ServiceAddress)
	var port int
	for _,str := range service[0].ServiceTags {
		if strings.Contains(str, "PORT_8080") {
			log.Println(str)
			port,_=strconv.Atoi(strings.SplitAfter(str, "=")[1])
		}
	}
	log.Print(port)
	//kv:=client.KV()
	//pair, _, err :=kv.Get("",nil)
	//
	//if err != nil {
	//	panic(err)
	//}
	//fmt.Printf("KV: %v", pair)


}
