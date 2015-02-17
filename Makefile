all: errdownload errdownload.exe

test: sampleshow.html sampleseries.html
	go test

errdownload: errdownload.go
	go build errdownload.go

errdownload.exe: errdownload.go
	GOOS=windows GOARCH=386 go build -o errdownload.exe errdownload.go

sampleshow.html:
	curl https://arhiiv.err.ee/vaata/kirjutamata-memuaare-kirjutamata-memuaare-malestusi-ii-maailmasoja-paevilt -Osampleshow.html

sampleseries.html:
	curl https://arhiiv.err.ee/seeria/kirjutamata-memuaare/elu/69/default/koik -Osampleseries.html
