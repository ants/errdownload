package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/ants/errdownload/download"
	"github.com/ants/errdownload/rtmp"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"time"

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
		return errors.New("Not a valid player url " + rawurl)
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
			dm.Download(&NamedShow{ShowUrl: absUrl})
			seenUrls[absUrl] = true
		}
	})
}

type fetchable interface {
	Url() string
	Filename(file string) string
}

func downloadShow(show fetchable) (string, error) {
	playerUrl, err := FindPlayerUrl(show.Url())
	if err != nil {
		return "", err
	}

	stream := &rtmp.Stream{Source: show.Url()}
	err = ParsePlayerParams(playerUrl, stream)
	if err != nil {
		return "", errors.New(fmt.Sprintf("Player parameter parsing from %s failed on %s: %s", playerUrl, show.Url(), err))
	}

	stream.Filename = show.Filename(stream.File)

	err = stream.Download()
	if err != nil {
		return "", err
	}
	return stream.Filename, nil
}

type NamedShow struct {
	ShowUrl string
}

func (n *NamedShow) Url() string {
	return n.ShowUrl
}

func (n *NamedShow) Filename(file string) string {
	showName := path.Base(urlMustParse(n.Url()).Path)
	showExt := path.Ext(file)
	return showName + showExt
}

func (n *NamedShow) Download() (string, error) {
	return downloadShow(n)
}

type ApiResult struct {
	TotalCount          int
	ElapsedMilliseconds int
	Results             []*SearchResult
}

type SearchResult struct {
	PublicId string
	Updated  string
	Header   string
	Lead     string
	ShowUrl  string `json:"Url"`
}

func (sr SearchResult) Url() string {
	return "http://" + sr.ShowUrl
}

func (sr SearchResult) Filename(file string) string {
	return sr.Header + "-" + path.Base(file)
}

func (sr SearchResult) Download() (string, error) {
	return downloadShow(sr)
}

func apiFetch(apiCall string) (*ApiResult, error) {
	resp, err := http.Get(apiCall)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var result *ApiResult
	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func FetchSearch(searchUrl, include string, dm *download.Manager) {
	su, err := url.Parse(searchUrl)
	if su.Host != "etv.err.ee" || su.Path != "/search3" {
		log.Fatal("Unknown URL for a search query")
	}
	apiCall := "http://etv.err.ee/api/search/programs?size=200&page=0&" + su.Fragment[1:] + "&onlyreviewable=true"
	apiResult, err := apiFetch(apiCall)
	if err != nil {
		log.Fatal(err)
	}

	earliest := time.Now().Add(-time.Hour * 24 * 7)
	showMatcher := regexp.MustCompile(include)

	for _, searchresult := range apiResult.Results {
		updated, err := time.Parse("2006-01-02T15:04:05", searchresult.Updated)
		if err != nil || earliest.After(updated) {
			continue
		}
		if !showMatcher.MatchString(searchresult.Header) {
			log.Printf(`Skipping "%s", does not match %s`, searchresult.Header, include)
			continue
		}
		dm.Download(searchresult)
	}
	return
}

func main() {
	var parallel int
	flag.IntVar(&parallel, "parallel", 1, "Number of parallel fetches to run")

	var series string
	flag.StringVar(&series, "series", "", "Download series URL")

	var search string
	flag.StringVar(&search, "search", "", "Download ETV search URL")

	var include string
	flag.StringVar(&include, "include", "", "Only include shows matching this regexp")

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
	case search != "":
		FetchSearch(search, include, manager)
	default:
		log.Fatal("Must specify the -series flag")
	}
}
