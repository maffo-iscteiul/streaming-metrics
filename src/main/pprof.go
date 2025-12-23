package main

import (
	"fmt"
	"os"
	"runtime/pprof"
	"time"

	"github.com/sirupsen/logrus"
)

func activate_profiling(pprofdir string, duration time.Duration) {
	if _, err := os.Stat(pprofdir); os.IsNotExist(err) {
		os.MkdirAll(pprofdir, 0700)
	}

	f, err := os.Create(fmt.Sprintf("%s/%s.pprof", pprofdir, time.Now().Format("2006-01-02_15:04:05")))
	if err != nil {
		logrus.Debugf("Failed to open file for profiling: %+v", err)
		return
	}

	time.Sleep(time.Second * 30)

	logrus.Infof("Profiling start!")

	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	time.Sleep(duration)

	logrus.Infof("Profiling done!")
}
