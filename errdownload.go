package main

import (
	"errors"
	"regexp"
	"html"
	"net/http"
	"net/url"
	"io/ioutil"
	"log"
	"sync"
	"os"
	"os/exec"
	"time"
	"fmt"
	"path"
	"encoding/csv"
	"flag"
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
	Source RtmpSource
	Stream string
	File string
	Filename string
}

func (r *RtmpStream) Download() error {
	start := time.Now()
	log.Printf("Starting download of stream from %s as %s", r.Source.Url(), r.Filename)
	
	rtmpCmd := exec.Command(*rtmpdumpBin, "-R", "-r", "rtmp://"+r.Stream, 
						 "-y", r.File, "-o", r.Filename, "-q")
	err := rtmpCmd.Run()
	if err != nil {
		return err
	}
	end := time.Now()
	log.Printf("Download of stream from %s took %s", r.Source.Url(), end.Sub(start))
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

func FindShows(page []byte, baseUrl string) ([]RtmpSource, error) {
	base := urlMustParse(baseUrl)
	
	matches := showpageRe.FindAllSubmatch(page, -1)
	if matches == nil {
		return nil, errors.New("No shows found")
	}
	results := make([]RtmpSource, 0, len(matches))
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

type DownloadResult struct {
	ShowUrl string
	Error error
	Filename string
}

func (dr DownloadResult) IsSuccessful() bool {
	return dr.Error == nil
}

type DownloadRegistry interface {
	Exists(showUrl string) bool
	Add(showUrl, filename string)
	Close()
}

type CsvRegistry struct {
	records map[string]string
	outfile *os.File
	writer *csv.Writer
}

func CsvRegistryOpen(registryfile string) (DownloadRegistry, error) {
	registry := make(map[string]string)
	if file, err := os.Open(registryfile); err == nil {
		records, err := csv.NewReader(file).ReadAll()
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Parsing csv registry from %s failed with: %s", registryfile, err))
		}
		for _, row := range records {
			if len(row) != 2 {
				log.Printf("Invalid row in %s: %s", registryfile, row)
				continue
			}
			registry[row[0]] = row[1]
		}
	}
	outfile, err := os.OpenFile(registryfile, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0660)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Could not open registry %s for writing: %s", registryfile, err))
	}
	return &CsvRegistry{
		records: registry,
		outfile: outfile,
		writer: csv.NewWriter(outfile),
	}, nil
}

func (c *CsvRegistry) Exists(showUrl string) bool {
	_, found := c.records[showUrl]
	return found
}

func (c *CsvRegistry) Add(showUrl, filename string) {
	c.records[showUrl] = filename
	c.writer.Write([]string{showUrl, filename})
	c.writer.Flush()
}

func (c *CsvRegistry) Close() {
	c.records = nil
	if c.outfile != nil {
		c.outfile.Close()
	}
	c.writer = nil
}

func FetchSeries(seriesUrl string, dm *DownloadManager) {
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

type RtmpSource interface {
	Url() string
	FetchStream() (*RtmpStream, error)
}

type NamedShow struct {
	ShowUrl string
}

func (n *NamedShow) Url() string {
	return n.ShowUrl
}

func (n *NamedShow) FetchStream() (*RtmpStream, error) {
	showPage, err := DownloadPage(n.ShowUrl)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("%s failed to download: %s", n.ShowUrl, err))
	}

	playerUrl := FindPlayerUrl(showPage)
	if playerUrl == "" {
		return nil, errors.New(fmt.Sprintf("%s does not contain a mediaframe", n.ShowUrl))
	}
	
	rtmp := &RtmpStream{Source: n}
	err = ParsePlayerParams(playerUrl, rtmp)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Player parameter parsing from %s failed on %s: %s", playerUrl, n.ShowUrl, err))
	}

	showName := path.Base(urlMustParse(n.ShowUrl).Path)
	showExt := path.Ext(rtmp.File)
	rtmp.Filename = showName + showExt

	return rtmp, nil
}

type DownloadManager struct {
	ToDownload chan RtmpSource
	Results chan DownloadResult
	registry DownloadRegistry
	workerWait *sync.WaitGroup
}

func NewDownloadManager(registry DownloadRegistry) *DownloadManager {
	dm := &DownloadManager{}
	dm.workerWait = new(sync.WaitGroup)
	dm.registry = registry
	dm.ToDownload = make(chan RtmpSource)
	dm.Results = make(chan DownloadResult, 1)
	
	go func() {
		for result := range dm.Results {
			if result.IsSuccessful() {
				registry.Add(result.ShowUrl, result.Filename)
			} else {
				log.Print("Downloading of %s failed: %s", result.ShowUrl, result.Error)
			}
		}
	}()

	return dm
}

func (dm *DownloadManager) Start(parallel int) {
	for i := 0; i < parallel; i++ {
		dm.workerWait.Add(1)
		go dm.processDownloads()
	}
}

func (dm *DownloadManager) Download(src RtmpSource) {
	if !dm.registry.Exists(src.Url()) {
		dm.ToDownload <- src
	}
}

func (dm *DownloadManager) Close() {
	close(dm.ToDownload)
	dm.workerWait.Wait()
	close(dm.Results)
	dm.registry.Close()
}

func (dm *DownloadManager) processDownloads() {
	defer dm.workerWait.Done()
	for show := range dm.ToDownload {
		result := DownloadResult{
			ShowUrl: show.Url(),
		}
		result.Error = func()error {
			rtmp, err := show.FetchStream()
			if err != nil {
				return err
			}
			err = rtmp.Download()
			if err != nil {
				return errors.New(fmt.Sprintf("Rtmp download of %s failed for %s: %s", rtmp, show.Url(), err))
			}
			result.Filename = rtmp.Filename
			return nil
		}()

		dm.Results <- result
	}
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
	
	registry, err := CsvRegistryOpen(downloadRegistry)
	if err != nil {
		log.Fatal(err)
	}
	manager := NewDownloadManager(registry)
	manager.Start(parallel)
	defer manager.Close()
	
	switch {
	case series != "":
		FetchSeries(series, manager)
	default:
		log.Fatal("Must specify the -series flag")
	}
}