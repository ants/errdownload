package main

import (
	"testing"
	"io/ioutil"
)

func TestRtmpStreamExtraction(t *testing.T) {
	samplePage, err := ioutil.ReadFile("sampleshow.html")
	if err != nil { t.Fatal(err) }
	
	url := FindPlayerUrl(samplePage)

	params, err := ParsePlayerParams("test", url)
	if err != nil { t.Fatal(err) }

	if params.Stream != "media.err.ee:80/arhiiv/" || params.File != "/AUDIO/a_8378_RMARHIIV.m4a" {
		t.Log(params)
		t.Fail()
	}
}


func TestShowListExtraction(t *testing.T) {
	samplePage, err := ioutil.ReadFile("sampleseries.html")
	if err != nil { t.Fatal(err) }

	urls, err := FindShowUrls(samplePage, "https://arhiiv.err.ee/seeria/kirjutamata-memuaare/elu/69/default/koik")
	if err != nil {
		t.Fatal(err)
	}
	
	sampleUrl := urls[0]
	if sampleUrl != "https://arhiiv.err.ee/vaata/kirjutamata-memuaare" {
		t.Log(urls[0:5])
		t.Fail()
	}
}