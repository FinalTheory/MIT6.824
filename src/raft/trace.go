package raft

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

const Trace = true

var gMutex sync.Mutex
var gFile *os.File = nil
var gCounter = 1
var gStart int64

func InitNewTrace() {
	if !Trace {
		return
	}
	gMutex.Lock()
	defer gMutex.Unlock()

	fileName := os.Getenv("TRACE_FILE")
	if len(fileName) != 0 {
		if gFile != nil {
			return
		}
	} else {
		fileName = fmt.Sprintf("%strace-%d.json", os.Getenv("TRACE_PREFIX"), gCounter)
		gCounter += 1
	}

	if gFile != nil {
		CloseTraceFileLocked()
	}
	fid, err := os.Create(fileName)
	if err != nil {
		panic("Failed to create trace gFile")
	}
	gFile = fid
	gFile.WriteString("[")
	gStart = time.Now().UnixMicro()
}

func CloseTraceFileLocked() {
	if !Trace {
		return
	}
	gFile.WriteString(fmt.Sprintf("{\"args\":null,\"name\":\"End\",\"ph\":\"i\",\"pid\":0,\"tid\":0,\"ts\":%d}]", time.Now().UnixMicro()-gStart+1000*100))
	gFile.Close()
	gFile = nil
}

func TraceEventBeginEndImpl(name string, server int, group int, timestamp int64, args map[string]any, etype string) {
	if !Trace {
		return
	}
	gMutex.Lock()
	defer gMutex.Unlock()
	if gFile == nil {
		return
	}
	logitem := map[string]any{
		"name": name,
		"ph":   etype,
		"pid":  group,
		"tid":  server,
		"ts":   timestamp - gStart,
		"args": args,
	}
	data, _ := json.Marshal(logitem)
	gFile.Write(data)
	gFile.WriteString(",\n")
}

func TraceInstant(name string, server int, group int, timestamp int64, args map[string]any) {
	if !Trace {
		return
	}
	gMutex.Lock()
	defer gMutex.Unlock()
	if gFile == nil {
		return
	}
	logitem := map[string]any{
		"name": name,
		"ph":   "i",
		"pid":  group,
		"tid":  server,
		"ts":   timestamp - gStart,
		"args": args,
	}
	data, _ := json.Marshal(logitem)
	gFile.Write(data)
	gFile.WriteString(",\n")
}

func TraceEventBegin(flag bool, name string, server int, group int, timestamp int64, args map[string]any) {
	if flag {
		TraceEventBeginEndImpl(name, server, group, timestamp, args, "B")
	}
}

func TraceEventEnd(flag bool, name string, server int, group int, timestamp int64, args map[string]any) {
	if flag {
		TraceEventBeginEndImpl(name, server, group, timestamp, args, "E")
	}
}
