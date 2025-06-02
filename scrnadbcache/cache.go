package scrnadbcache

import (
	"sync"

	"github.com/antonybholmes/go-scrna"
)

var instance *scrna.DatasetsCache
var once sync.Once

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

func Datasets(species string, technology string) ([]*scrna.Dataset, error) {
	return instance.Datasets(species, technology)
}

func Gex(datasetIds []string,
	geneIds []string) ([]*scrna.SearchResults, error) {
	return instance.Gex(datasetIds, geneIds)
}

func Metadata(datasetIds []string) ([]*scrna.DatasetMetadata, error) {
	return instance.Metadata(datasetIds)
}
