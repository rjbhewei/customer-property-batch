package main

import (
	"compress/gzip"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
	"regexp"
	"flag"
	"time"
)

//const (
//	POSTVALUE = `{"1089806":{"selected":[2449864]}}`
//	HTTPURL = "http://panshi.qq.com/v2/vote/11018898/submit"
//	REGEXPSTR  = `"optionid":2449864,("selected":\d*),`
//	REPLACESTR  = `"optionid":2449864,`
//)

const (
	POSTVALUE = `{"1089806":{"selected":[2449768]}}`
	HTTPURL = "http://panshi.qq.com/v2/vote/11018898/submit"
	REGEXPSTR  = `"optionid":2449768,("selected":\d*),`
	REPLACESTR  = `"optionid":2449768,`
)

var (
	postValue = flag.String("postValue", POSTVALUE, "提交数据")
	httpUrl = flag.String("httpUrl", HTTPURL, "url")
	regexpstr = flag.String("regexpstr", REGEXPSTR, "龙代熊的正则数据")
	replaceStr = flag.String("replaceStr", REPLACESTR, "龙代熊")
)

var reg *regexp.Regexp

func init() {
	reg =  regexp.MustCompile(*regexpstr)
}

func main() {

	client := &http.Client{}

	var values url.Values = make(map[string][]string)

	values.Add("answer", *postValue)
	values.Add("login", "1")
	values.Add("source", "1")
	values.Add("g_tk", "569056360")
	values.Add("format", "script")
	values.Add("callback", "parent.AppPlatform.Survey.Digg.ReceiveDiggResult")

	log.Println(values)

	for ; ;  {
		reqest, err := http.NewRequest("POST", *httpUrl, strings.NewReader(values.Encode()))

		if err != nil {
			log.Panic(err)
		}

		reqest.Header.Add("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
		reqest.Header.Add("Accept-Encoding", "gzip, deflate")
		reqest.Header.Add("Accept-Language", "zh-CN,zh;q=0.8,en;q=0.6,zh-TW;q=0.4")
		reqest.Header.Add("Cache-Control", "max-age=0")
		reqest.Header.Add("Connection", "keep-alive")
		reqest.Header.Add("Content-Length", "169")
		reqest.Header.Add("Content-Type", "application/x-www-form-urlencoded")
		reqest.Header.Add("Cookie", "tvfe_boss_uuid=0809280423617fa5; pac_uid=1_41057694; ptui_loginuin=2510399159; RK=DJ8mSym3EL; pgv_pvi=3661495296; pt2gguin=o0041057694; luin=o0041057694; lskey=00010000711b6ba2168b1b1263ce6712d2212de24c16ca5779fbdee944cde2177280cbca8188c109ea090138; ptcz=263f06245a6cfd5c9d8ac9bcd056f64eabeaa821e644e24fa136420305f22ccf; pgv_info=ssid=s4681242220; pgv_pvid=7817044634; o_cookie=41057694")
		reqest.Header.Add("Host", "panshi.qq.com")
		reqest.Header.Add("Origin", "http://hn.qq.com")
		reqest.Header.Add("Referer", "http://hn.qq.com/zt2016/2016xasxxstp/index.htm?from=singlemessage&isappinstalled=0")
		reqest.Header.Add("Upgrade-Insecure-Requests", "1")
		reqest.Header.Add("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36")

		resp, err := client.Do(reqest)

		if err != nil {
			log.Panic(err)
		}

		defer resp.Body.Close()

		var reader io.ReadCloser

		if resp.Header.Get("Content-Encoding") == "gzip" {
			reader, err = gzip.NewReader(resp.Body)
			if err != nil {
				log.Panic(err)
			}
		} else {
			reader = resp.Body
		}

		b, _ := ioutil.ReadAll(reader)

		//body := string(b)

		log.Println("response Status:", resp.Status)
		//log.Println("response Headers:", resp.Header)
		//log.Println("response Body:", body)
		daixiong := string(reg.Find(b))
		log.Println("response daixiong :", daixiong)
		rep := strings.Replace(string(daixiong), *replaceStr, "龙代熊", -1)
		rep = strings.Replace(rep, `"selected"`, "票数", -1)
		log.Println(rep)

		time.Sleep(1 * time.Second)

		log.Println("暂停1秒后继续")
	}


}
