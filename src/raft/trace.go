package raft

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

type name struct {
	Name      string `json:"name"`
	Cat       string `json:"cat"`
	Phase     string `json:"ph"`
	TimeStamp string `json:"ts"`
	Pid       string `json:"pid"`
	Tid       string `json:"tid"`
}

var gMutex sync.Mutex
var gFile *os.File = nil
var gCounter = 1
var gStart int64

func InitNewTrace() {
	gMutex.Lock()
	defer gMutex.Unlock()

	if gFile != nil {
		CloseTraceFileLocked()
	}
	fid, err := os.Create(fmt.Sprintf("trace-%d.json", gCounter))
	gCounter += 1
	if err != nil {
		panic("Failed to create trace gFile")
	}
	gFile = fid
	gFile.WriteString("[")
	gStart = time.Now().UnixMicro()
}

func CloseTraceFileLocked() {
	gFile.WriteString(fmt.Sprintf("{\"args\":null,\"name\":\"End\",\"ph\":\"i\",\"pid\":0,\"tid\":0,\"ts\":%d}]", time.Now().UnixMicro()-gStart+1000*100))
	gFile.Close()
	gFile = nil
}

func TraceEventBeginEndImpl(name string, server int, timestamp int64, args map[string]any, etype string) {
	gMutex.Lock()
	defer gMutex.Unlock()
	if gFile == nil {
		return
	}
	logitem := map[string]any{
		"name": name,
		"ph":   etype,
		"pid":  0,
		"tid":  server,
		"ts":   timestamp - gStart,
		"args": args,
	}
	data, _ := json.Marshal(logitem)
	gFile.Write(data)
	gFile.WriteString(",\n")
}

func TraceInstant(name string, server int, timestamp int64, args map[string]any) {
	gMutex.Lock()
	defer gMutex.Unlock()
	if gFile == nil {
		return
	}
	logitem := map[string]any{
		"name": name,
		"ph":   "i",
		"pid":  0,
		"tid":  server,
		"ts":   timestamp - gStart,
		"args": args,
	}
	data, _ := json.Marshal(logitem)
	gFile.Write(data)
	gFile.WriteString(",\n")
}

func TraceCounter(name string, server int, timestamp int64, args map[string]any) {
	gMutex.Lock()
	defer gMutex.Unlock()
	if gFile == nil {
		return
	}
	logitem := map[string]any{
		"name": name,
		"ph":   "C",
		"pid":  0,
		"tid":  server,
		"ts":   timestamp - gStart,
		"args": args,
	}
	data, _ := json.Marshal(logitem)
	gFile.Write(data)
	gFile.WriteString(",\n")
}

func TraceEventBegin(flag bool, name string, server int, timestamp int64, args map[string]any) {
	if flag {
		TraceEventBeginEndImpl(name, server, timestamp, args, "B")
	}
}

func TraceEventEnd(flag bool, name string, server int, timestamp int64, args map[string]any) {
	if flag {
		TraceEventBeginEndImpl(name, server, timestamp, args, "E")
	}
}
