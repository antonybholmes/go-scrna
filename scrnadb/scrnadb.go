package scrnadb

import (
	"sync"

	"github.com/antonybholmes/go-scrna"
	"github.com/antonybholmes/go-scrna/dat"
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

func GetInstance() *scrna.ScrnaDB {
	return instance
}

func Dir() string {
	return instance.Dir()
}

func Species() ([]string, error) {
	return instance.Species()
}

func Assemblies(species string) ([]string, error) {
	return instance.Assemblies(species)
}

func Datasets(species string, assembly string, permissions []string) ([]*scrna.Dataset, error) {
	return instance.Datasets(species, assembly, permissions)
}

func Gex(datasetId string, geneIds []string) (*dat.GexResults, error) {
	return instance.Gex(datasetId, geneIds)
}

// func Clusters(id string) (*scrna.DatasetClusters, error) {
// 	return instance.Clusters(id)
// }

func Metadata(id string) (*scrna.DatasetMetadata, error) {
	return instance.Metadata(id)
}

func Genes(id string) ([]*scrna.Gene, error) {
	return instance.Genes(id)
}

func SearchGenes(id string, query string, limit int16) ([]*scrna.Gene, error) {
	return instance.SearchGenes(id, query, limit)
}

func HasPermissionToViewDataset(datasetId string, permissions []string) error {
	return instance.HasPermissionToViewDataset(datasetId, permissions)
}

//
