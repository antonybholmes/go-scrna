package cache

import (
	"sync"

	"github.com/antonybholmes/go-scrna"
)

var (
	instance *scrna.DatasetsCache
	once     sync.Once
)

func InitCache(dir string) (*scrna.DatasetsCache, error) {
	once.Do(func() {
		instance = scrna.NewDatasetsCache(dir)
	})

	return instance, nil
}

func GetInstance() *scrna.DatasetsCache {
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

func Datasets(species string, assembly string) ([]*scrna.Dataset, error) {
	return instance.Datasets(species, assembly)
}

func Gex(id string, geneIds []string) (*scrna.GexResults, error) {
	return instance.Gex(id, geneIds)
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

//
