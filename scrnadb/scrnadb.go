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

func Datasets(species string, assembly string, isAdmin bool, permissions []string) ([]*scrna.Dataset, error) {
	return instance.Datasets(species, assembly, isAdmin, permissions)
}

func Gex(datasetId string, geneIds []string, isAdmin bool, permissions []string) (*dat.GexResults, error) {
	return instance.Gex(datasetId, geneIds, isAdmin, permissions)
}

// func Clusters(id string) (*scrna.DatasetClusters, error) {
// 	return instance.Clusters(id)
// }

func Metadata(datasetId string, isAdmin bool, permissions []string) (*scrna.DatasetMetadata, error) {
	return instance.Metadata(datasetId, isAdmin, permissions)
}

func Genes(datasetId string, isAdmin bool, permissions []string) ([]*scrna.Gene, error) {
	return instance.Genes(datasetId, isAdmin, permissions)
}

func SearchGenes(id string, query string, limit int, isAdmin bool, permissions []string) ([]*scrna.Gene, error) {
	return instance.SearchGenes(id, query, limit, isAdmin, permissions)
}

// func HasPermissionToViewDataset(datasetId string, permissions []string) error {
// 	return instance.HasPermissionToViewDataset(datasetId, permissions)
// }
