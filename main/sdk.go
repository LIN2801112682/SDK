package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	client "github.com/influxdata/influxdb1-client"
	"io"
	"log"
	"net/url"
	"os"
	"time"
)

type Log struct {
	Timestamp int64  `json:"@timestamp"`
	Request   string `json:"request"`
}

func GetLinesFromFileAndJson(filename string, index int) []Log { //
	var logdata = make([]Log, 0)
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println(err)
	}
	defer file.Close()
	buff := bufio.NewReader(file)
	i := 0
	for {
		i++
		data, _, eof := buff.ReadLine()
		if eof == io.EOF {
			break
		}
		jsonStr := data
		var logs Log
		err = json.Unmarshal(jsonStr, &logs)
		if err != nil {
			//fmt.Println(i)
			panic("解析失败")
		}
		logdata = append(logdata, Log{
			Timestamp: logs.Timestamp,
			Request:   logs.Request,
		})
		if i == index {
			break
		}
	}
	return logdata
}

func main() {
	logdata := GetLinesFromFileAndJson("../resource/result_log.txt", 5000000) //result_log
	host, err := url.Parse(fmt.Sprintf("http://%s:%d", "127.0.0.1", 8086))    // /write?db=clv&u=admin&p=At1314comi!
	if err != nil {
		log.Fatal(err)
	}
	con, err := client.NewClient(client.Config{URL: *host, Username: "admin", Password: "At1314comi!"})
	if err != nil {
		log.Fatal(err)
	}
	for k := 0; k < 50000; k++ {
		pts := make([]client.Point, 0)
		var index int64
		for i := 100 * k; i < 100*k+100; i++ {
			pt := client.Point{
				Measurement: "clvTable",
				//Tags: map[string]string{
				// "host": "A" + string(k),
				//},
				Fields: map[string]interface{}{
					"logs": logdata[i].Request,
				},
				Time:      time.Unix(0, logdata[i].Timestamp), //  int64(i)
				Precision: "ns",
			}
			pts = append(pts, pt)
			index++
		}
		bps := client.BatchPoints{
			Points:   pts,
			Database: "clv",
		}
		if _, err := con.Write(bps); err != nil {
			panic(err)
		}
		//con.Write(bps)
	}
}
