package main

import (
	"errors"
	"net/url"
	"log"
	"fmt"
	"path"
	"flag"
	"github.com/ants/errdownload/download"
	"github.com/ants/errdownload/rtmp"
	
	"github.com/PuerkitoBio/goquery"
)


func FindPlayerUrl(showPage string) (string, error) {
	doc, err := goquery.NewDocument(showPage)
	if err != nil {
		return "", err
	}
	src, exists := doc.Find(`iframe[id^="mediaframe"]`).First().Attr("src")
	if !exists {
		return "", errors.New(fmt.Sprintf("mediaframe not found in %s", showPage))
	}
	return src, nil
	
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

func FetchSeries(seriesUrl string, dm *download.Manager) {
	doc, err := goquery.NewDocument(seriesUrl)
	if err != nil {
		log.Fatal(err)
	}

	base := urlMustParse(seriesUrl)
	seenUrls := map[string]bool{}

	doc.Find(`a[href^="/vaata/"]`).Each(func(i int, s *goquery.Selection) {
		href, _ := s.Attr("href")
		absUrl := base.ResolveReference(urlMustParse(href)).String()
		if !seenUrls[absUrl] {
			dm.Download(&NamedShow{ShowUrl:absUrl})
			seenUrls[absUrl] = true
		}
	})
}

type NamedShow struct {
	ShowUrl string
}

func (n *NamedShow) Url() string {
	return n.ShowUrl
}

func (n *NamedShow) Download() (string, error) {
	playerUrl, err := FindPlayerUrl(n.ShowUrl)
	if err != nil {
		return "", err
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