package main

import (
	"errors"
	"regexp"
	"html"
	"net/http"
	"net/url"
	"io/ioutil"
	"log"
	"os/exec"
	"time"
	"fmt"
	"path"
	"flag"
	"github.com/ants/errdownload/download"
)


var mediaframeRe *regexp.Regexp = regexp.MustCompile(`<iframe id="mediaframe[^"]*"[^>]+src="([^"]*)"`)

func FindPlayerUrl(page []byte) string {
	match := mediaframeRe.FindSubmatch(page)
	if match == nil {
		return ""
	}
	return html.UnescapeString(string(match[1]))
}

var rtmpdumpBin *string = flag.String("rtmpdump", "rtmpdump", "Path to rtmpdump executable")

type RtmpStream struct {
	Source string
	Stream string
	File string
	Filename string
}

func (r *RtmpStream) Download() error {
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



func CheckForRtmp() (err error) {
	_, err = exec.Command(*rtmpdumpBin, "--help").CombinedOutput()
	return
}

func ParsePlayerParams(rawurl string, rtmp *RtmpStream) error {
	playerUrl, err := url.Parse(rawurl)
	if err != nil {
		return err
	}
	query := playerUrl.Query()

	rtmp.Stream = query.Get("stream")
	rtmp.File = query.Get("file")

	if rtmp.Stream == "" || rtmp.File == "" {
		return errors.New("Not a valid player url "+rawurl)
	}

	return nil
}

func urlMustParse(rawurl string) *url.URL {
	result, err := url.Parse(rawurl)
	if err != nil {
		panic(err)
	}
	return result
}

var showpageRe *regexp.Regexp = regexp.MustCompile(`<h2><a href="(/vaata/[^"]*)"`)

func FindShows(page []byte, baseUrl string) ([]download.Downloadable, error) {
	base := urlMustParse(baseUrl)
	
	matches := showpageRe.FindAllSubmatch(page, -1)
	if matches == nil {
		return nil, errors.New("No shows found")
	}
	results := make([]download.Downloadable, 0, len(matches))
	for _, match := range matches {
		rel := urlMustParse(string(match[1]))
		absUrl := base.ResolveReference(rel).String()
		results = append(results, &NamedShow{ShowUrl:absUrl})
	}
	return results, nil
}

func DownloadPage(url string) ([]byte, error) {
	start := time.Now()
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	end := time.Now()
	log.Printf("Download of %s took %s", url, end.Sub(start))
	return body, nil
}

func FetchSeries(seriesUrl string, dm *download.Manager) {
	seriesPage, err := DownloadPage(seriesUrl)
	if err != nil {
		log.Fatal(err)
	}

	shows, err := FindShows(seriesPage, seriesUrl)
	if err != nil {
		log.Fatal(err)
	}
	
	for _, show := range shows {
		dm.Download(show)
	}
}

type NamedShow struct {
	ShowUrl string
}

func (n *NamedShow) Url() string {
	return n.ShowUrl
}

func (n *NamedShow) Download() (string, error) {
	showPage, err := DownloadPage(n.ShowUrl)
	if err != nil {
		return "", errors.New(fmt.Sprintf("%s failed to download: %s", n.ShowUrl, err))
	}

	playerUrl := FindPlayerUrl(showPage)
	if playerUrl == "" {
		return "", errors.New(fmt.Sprintf("%s does not contain a mediaframe", n.ShowUrl))
	}
	
	rtmp := &RtmpStream{Source: n.ShowUrl}
	err = ParsePlayerParams(playerUrl, rtmp)
	if err != nil {
		return "", errors.New(fmt.Sprintf("Player parameter parsing from %s failed on %s: %s", playerUrl, n.ShowUrl, err))
	}

	showName := path.Base(urlMustParse(n.ShowUrl).Path)
	showExt := path.Ext(rtmp.File)
	rtmp.Filename = showName + showExt

	err = rtmp.Download()
	if err != nil {
		return "", err
	}
	return rtmp.Filename, nil
}

func main() {
	var parallel int
	flag.IntVar(&parallel, "parallel", 1, "Number of parallel fetches to run")
	
	var series string
	flag.StringVar(&series, "series", "", "Download series URL")
	
	var downloadRegistry string
	flag.StringVar(&downloadRegistry, "downloads", "downloaded.csv", "Store data about downloaded shows in this file")

	flag.Parse()
	
	if err := CheckForRtmp(); err != nil {
		log.Fatal("rtmpdump execution failed: ", err)
	}
	
	manager, err := download.NewManager(downloadRegistry)
	if err != nil {
		log.Fatal(err)
	}
	manager.Start(parallel)
	defer manager.Close()
	
	switch {
	case series != "":
		FetchSeries(series, manager)
	default:
		log.Fatal("Must specify the -series flag")
	}
}