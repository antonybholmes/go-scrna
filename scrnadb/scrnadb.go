package scrnadb

import (
	"sync"

	"github.com/antonybholmes/go-scrna"
)

var (
	instance *scrna.ScrnaDB
	once     sync.Once
)

func InitScrnaDB(dir string) *scrna.ScrnaDB {
	once.Do(func() {
		instance = scrna.NewScrnaDB(dir)
	})

	return instance
}

func Instance() *scrna.ScrnaDB {
	return instance
}
