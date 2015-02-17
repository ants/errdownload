package main

import (
	"errors"
	"regexp"
	"html"
	"net/http"
	"net/url"
	"io/ioutil"
	"log"
	"time"
	"fmt"
	"path"
	"flag"
	"github.com/ants/errdownload/download"
	"github.com/ants/errdownload/rtmp"
)


var mediaframeRe *regexp.Regexp = regexp.MustCompile(`<iframe id="mediaframe[^"]*"[^>]+src="([^"]*)"`)

func FindPlayerUrl(page []byte) string {
	match := mediaframeRe.FindSubmatch(page)
	if match == nil {
		return ""
	}
	return html.UnescapeString(string(match[1]))
}

func ParsePlayerParams(rawurl string, stream *rtmp.Stream) error {
	playerUrl, err := url.Parse(rawurl)
	if err != nil {
		return err
	}
	query := playerUrl.Query()

	stream.Stream = query.Get("stream")
	stream.File = query.Get("file")

	if stream.Stream == "" || stream.File == "" {
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
	
	stream := &rtmp.Stream{Source: n.ShowUrl}
	err = ParsePlayerParams(playerUrl, stream)
	if err != nil {
		return "", errors.New(fmt.Sprintf("Player parameter parsing from %s failed on %s: %s", playerUrl, n.ShowUrl, err))
	}

	showName := path.Base(urlMustParse(n.ShowUrl).Path)
	showExt := path.Ext(stream.File)
	stream.Filename = showName + showExt

	err = stream.Download()
	if err != nil {
		return "", err
	}
	return stream.Filename, nil
}

func main() {
	var parallel int
	flag.IntVar(&parallel, "parallel", 1, "Number of parallel fetches to run")
	
	var series string
	flag.StringVar(&series, "series", "", "Download series URL")
	
	var downloadRegistry string
	flag.StringVar(&downloadRegistry, "downloads", "downloaded.csv", "Store data about downloaded shows in this file")

	flag.Parse()
	
	if err := rtmp.CheckBinary(); err != nil {
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