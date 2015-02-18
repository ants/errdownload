package download

import (
	"encoding/csv"
	"errors"
	"fmt"
	"log"
	"os"
)

type CsvRegistry struct {
	records map[string]string
	outfile *os.File
	writer  *csv.Writer
}

func OpenCsvRegistry(registryfile string) (Registry, error) {
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
		writer:  csv.NewWriter(outfile),
	}, nil
}

func (c *CsvRegistry) Exists(url string) bool {
	_, found := c.records[url]
	return found
}

func (c *CsvRegistry) Add(url, filename string) {
	c.records[url] = filename
	c.writer.Write([]string{url, filename})
	c.writer.Flush()
}

func (c *CsvRegistry) Close() {
	c.records = nil
	if c.outfile != nil {
		c.outfile.Close()
	}
	c.writer = nil
}
