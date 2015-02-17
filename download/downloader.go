package download

import (
	"errors"
	"log"
	"sync"
	"path"
)

type Downloadable interface {
	Url() string
	Download() (string, error)
}

type Result struct {
	Url string
	Error error
	Filename string
}

func (dr Result) IsSuccessful() bool {
	return dr.Error == nil
}

type Registry interface {
	Exists(showUrl string) bool
	Add(showUrl, filename string)
	Close()
}

type Manager struct {
	ToDownload chan Downloadable
	Results chan Result
	registry Registry
	workerWait *sync.WaitGroup
}

func NewManager(registryPath string) (dm *Manager, err error) {
	dm = &Manager{}

	switch ext := path.Ext(registryPath); ext {
		case ".csv":
			dm.registry, err = OpenCsvRegistry(registryPath)
		default:
			err = errors.New("Unknow download registry extension "+ext)
	}
	if err != nil {
		return
	}
	
	dm.workerWait = new(sync.WaitGroup)
	dm.ToDownload = make(chan Downloadable)
	dm.Results = make(chan Result, 1)
	
	go func() {
		for result := range dm.Results {
			if result.IsSuccessful() {
				dm.registry.Add(result.Url, result.Filename)
			} else {
				log.Print("Downloading of %s failed: %s", result.Url, result.Error)
			}
		}
	}()

	return
}

func (dm *Manager) Start(parallel int) {
	for i := 0; i < parallel; i++ {
		dm.workerWait.Add(1)
		go dm.processDownloads()
	}
}

func (dm *Manager) Download(src Downloadable) {
	if !dm.registry.Exists(src.Url()) {
		dm.ToDownload <- src
	}
}

func (dm *Manager) Close() {
	close(dm.ToDownload)
	dm.workerWait.Wait()
	close(dm.Results)
	dm.registry.Close()
}

func (dm *Manager) processDownloads() {
	defer dm.workerWait.Done()
	for dl := range dm.ToDownload {
		result := Result{Url: dl.Url()}
		result.Filename, result.Error = dl.Download()
		dm.Results <- result
	}
}