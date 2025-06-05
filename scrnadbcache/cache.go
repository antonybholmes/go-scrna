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

func Gex(publicId string, geneIds []string) (*scrna.GexResults, error) {
	return instance.Gex(publicId, geneIds)
}

func Clusters(publicId string) (*scrna.DatasetClusters, error) {
	return instance.Clusters(publicId)
}

func Cells(publicId string) (*scrna.DatasetCells, error) {
	return instance.Cells(publicId)
}

func Genes(publicId string) ([]*scrna.Gene, error) {
	return instance.Genes(publicId)
}

func SearchGenes(publicId string, query string, limit uint16) ([]*scrna.Gene, error) {
	return instance.SearchGenes(publicId, query, limit)
}

//
