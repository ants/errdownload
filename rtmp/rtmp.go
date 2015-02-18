package rtmp

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os/exec"
	"time"
)

var rtmpdumpBin *string = flag.String("rtmpdump", "rtmpdump", "Path to rtmpdump executable")

type Stream struct {
	Source   string
	Stream   string
	File     string
	Filename string
}

func (r *Stream) Download() error {
	start := time.Now()
	log.Printf("Starting download of stream from %s as %s", r.Source, r.Filename)

	rtmpCmd := exec.Command(*rtmpdumpBin, "-R", "-r", "rtmp://"+r.Stream,
		"-y", r.File, "-o", r.Filename, "-q")
	err := rtmpCmd.Run()
	if err != nil {
		return errors.New(fmt.Sprintf("Rtmp download of rtmp://%s%s failed for %s: %s",
			r.Stream, r.File, r.Source, err))
	}
	end := time.Now()
	log.Printf("Download of stream from %s took %s", r.Source, end.Sub(start))
	return nil
}

func CheckBinary() (err error) {
	_, err = exec.Command(*rtmpdumpBin, "--help").CombinedOutput()
	return
}
