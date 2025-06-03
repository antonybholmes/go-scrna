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

func Datasets(species string, assembly string) ([]*scrna.Dataset, error) {
	return instance.Datasets(species, assembly)
}

func Gex(datasetId string,
	geneIds []string) (*scrna.SearchResults, error) {
	return instance.Gex(datasetId, geneIds)
}

func Metadata(publicId string) (*scrna.DatasetMetadata, error) {
	return instance.Metadata(publicId)
}

func Genes(publicId string) ([]*scrna.Gene, error) {
	return instance.Genes(publicId)
}

func SearchGenes(publicId string, query string, limit uint16) ([]*scrna.Gene, error) {
	return instance.SearchGenes(publicId, query, limit)
}

//
