package raft

import (
	"log"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func CheckKillFinish(timeout int64, isKillComplete func() bool, obj interface{}) {
	go func(start int64) {
		for !isKillComplete() {
			time.Sleep(time.Millisecond * 100)
			if time.Now().UnixMilli()-start > timeout*1000 {
				log.Printf("Spent more than %ds to kill %p", timeout, obj)
			}
		}
	}(time.Now().UnixMilli())
}
