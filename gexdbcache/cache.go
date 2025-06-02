package scrnacache

import (
	"sync"

	"github.com/antonybholmes/go-scrna"
)

var instance *scrna.DatasetsCache
var once sync.Once
var technologies []scrna.Technology

func Technologies() []scrna.Technology {
	return technologies
}

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

// func Platforms(species string) ([]string, error) {
// 	return instance.Plaforms(species)
// }

func Datasets(species string, technology string) ([]*scrna.Dataset, error) {
	return instance.Datasets(species, technology)
}

func AllTechnologies() (map[string]map[string][]string, error) {
	return instance.AllTechnologies()
}

func FindRNASeqValues(datasetIds []string,
	scrnaType string,
	geneIds []string) ([]*scrna.SearchResults, error) {
	return instance.FindRNASeqValues(datasetIds, scrnaType, geneIds)
}

func FindMicroarrayValues(datasetIds []string,
	geneIds []string) ([]*scrna.SearchResults, error) {
	return instance.FindMicroarrayValues(datasetIds, geneIds)
}

// func GetDataset(uuid string) (*scrna.Dataset, error) {
// 	return instance.GetDataset(uuid)
// }

// func Search(location *dna.Location, uuids []string) (*scrna.SearchResults, error) {
// 	return instance.Search(location, uuids)
// }
