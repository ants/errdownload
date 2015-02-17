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
	ShowUrl string
	Stream string
	File string
}

func (r *RtmpStream) Download() error {
	start := time.Now()
	log.Printf("Starting download of stream from %s as %s", r.ShowUrl, r.OutName())
	
	rtmpCmd := exec.Command(*rtmpdumpBin, "-R", "-r", "rtmp://"+r.Stream, 
						 "-y", r.File, "-o", r.OutName(), "-q")
	err := rtmpCmd.Run()
	if err != nil {
		return err
	}
	end := time.Now()
	log.Printf("Download of stream from %s took %s", r.ShowUrl, end.Sub(start))
	return nil
}

func (r *RtmpStream) OutName() string {
	showName := path.Base(urlMustParse(r.ShowUrl).Path)
	showExt := path.Ext(r.File)
	return showName + showExt
}

func CheckForRtmp() (err error) {
	_, err = exec.Command(*rtmpdumpBin, "--help").CombinedOutput()
	return
}

func ParsePlayerParams(showUrl, rawurl string) (*RtmpStream, error) {
	playerUrl, err := url.Parse(rawurl)
	if err != nil {
		return nil, err
	}
	query := playerUrl.Query()

	stream := query.Get("stream")
	file := query.Get("file")
	if stream == "" || file == "" {
		return nil, errors.New("Not a valid player url "+rawurl)
	}
	
	return &RtmpStream{showUrl, stream, file}, nil
}

func urlMustParse(rawurl string) *url.URL {
	result, err := url.Parse(rawurl)
	if err != nil {
		panic(err)
	}
	return result
}

var showpageRe *regexp.Regexp = regexp.MustCompile(`<h2><a href="(/vaata/[^"]*)"`)

func FindShowUrls(page []byte, baseUrl string) ([]string, error) {
	base := urlMustParse(baseUrl)
	
	matches := showpageRe.FindAllSubmatch(page, -1)
	if matches == nil {
		return nil, errors.New("No shows found")
	}
	results := make([]string, 0, len(matches))
	for _, match := range matches {
		rel := urlMustParse(string(match[1]))
		absUrl := base.ResolveReference(rel).String()
		results = append(results, absUrl)
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
	Code DownloadResultCode
	Error error
	Filename string
}

type DownloadResultCode int
const (
	DownloadOk DownloadResultCode = iota
	DownloadFailed
)

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

func FetchSeries(registry DownloadRegistry, seriesUrl string, parallel int) {
	seriesPage, err := DownloadPage(seriesUrl)
	if err != nil {
		log.Fatal(err)
	}
	
	showUrls, err := FindShowUrls(seriesPage, seriesUrl)
	if err != nil {
		log.Fatal(err)
	}
	
	wg := new(sync.WaitGroup)
	shows := make(chan string)
	results := make(chan DownloadResult, parallel)
	
	for i := 0; i < parallel; i++ {
		wg.Add(1)
		go RtmpDownloader(shows, results, wg)
	}
	
	go func() {
		for result := range results {
			if result.Code == DownloadOk {
				registry.Add(result.ShowUrl, result.Filename)
			} else {
				log.Print("Downloading of %s failed: %s", result.ShowUrl, result.Error)
			}
		}
	}()
	
	for _, showUrl := range showUrls {
		if registry.Exists(showUrl) {
			continue
		}
		shows <- showUrl
	}
	close(shows)
	wg.Wait()
	close(results)
}

func downloadShow(showUrl string) (result DownloadResult) {
	result.ShowUrl, result.Code = showUrl, DownloadFailed

	showPage, err := DownloadPage(showUrl)
	if err != nil {
		result.Error = errors.New(fmt.Sprintf("%s failed to download: %s", showUrl, err))
		return
	}

	playerUrl := FindPlayerUrl(showPage)
	if playerUrl == "" {
		result.Error = errors.New(fmt.Sprintf("%s does not contain a mediaframe", showUrl))
		return
	}
	rtmp, err := ParsePlayerParams(showUrl, playerUrl)
	if err != nil {
		result.Error = errors.New(fmt.Sprintf("Player parameter parsing from %s failed on %s: %s", playerUrl, showUrl, err))
		return
	}
	err = rtmp.Download()
	if err != nil {
		result.Error = errors.New(fmt.Sprintf("Rtmp download of %s failed for %s: %s", rtmp, showUrl, err))
		return
	}
	result.Filename = rtmp.OutName()
	result.Code = DownloadOk
	return
}

func RtmpDownloader(showUrls chan string, results chan DownloadResult, wg *sync.WaitGroup) {
	defer wg.Done()
	for showUrl := range showUrls {
		results <- downloadShow(showUrl)
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
	defer registry.Close()
	
	switch {
	case series != "":
		FetchSeries(registry, series, parallel)
	default:
		log.Fatal("Must specify the -series flag")
	}
}