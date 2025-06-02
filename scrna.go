package scrna

import (
	"database/sql"
	"path/filepath"
)

// approx size of dataset
const DATASET_SIZE = 500

const GENES_SQL = `SELECT 
	genome.id, 
	genome.gene_id, 
	genome.gene_symbol 
	FROM genes 
	ORDER BY genome.gene_symbol`

const SPECIES_SQL = `SELECT DISTINCT
	species,
	FROM datasets
	ORDER BY species`

const ASSEMBLIES_SQL = `SELECT
	datasets.assembly
	FROM datasets
	WHERE datasets.species = ?1 
	ORDER BY datasets.assembly`

const ALL_TECHNOLOGIES_SQL = `SELECT DISTINCT 
	species, technology, platform 
	FROM datasets 
	ORDER BY species, technology, platform`

// const ALL_VALUE_TYPES_SQL = `SELECT
// 	gex_value_types.id,
// 	gex_value_types.name
// 	FROM gex_value_types
// 	ORDER BY gex_value_types.platform_id, gex_value_types.id`

// const VALUE_TYPES_SQL = `SELECT
// 	gex_value_types.id,
// 	gex_value_types.name
// 	FROM gex_value_types
// 	WHERE gex_value_types.platform_id = ?1
// 	ORDER BY gex_value_types.id`

const DATASETS_SQL = `SELECT 
	datasets.id,
	datasets.public_id,
	datasets.name,
	datasets.institution,
	datasets.species,
	datasets.assembly,
	dataset.cells,
	datasets.url,
	datasets.description
	FROM datasets 
	WHERE datasets.species = ?1 AND datasets.assembly = ?2
	ORDER BY datasets.name`

const DATASET_SQL = `SELECT 
	datasets.id,
	datasets.public_id,
	datasets.name,
	datasets.institution,
	datasets.species,
	datasets.assembly,
	dataset.cells,
	datasets.url,
	datasets.description
	FROM datasets 
	WHERE datasets.public_id = ?1`

// const DATASETS_SQL = `SELECT
// 	name
// 	FROM datasets
// 	ORDER BY datasets.name`

// type GexValue string

type Idtype struct {
	Name string `json:"name"`
	Id   int    `json:"id"`
}

type NameValueType struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type Species = Idtype
type GexValue = Idtype

// type GexType string

// const (
// 	GEX_TYPE_RNA_SEQ        GexType = "RNA-seq"
// 	GEX_TYPE_RNA_MICROARRAY GexType = "Microarray"
//)

type Sample struct {
	PublicId string          `json:"publicId"`
	Name     string          `json:"name"`
	AltNames []string        `json:"altNames"`
	Id       int             `json:"-"`
	Metadata []NameValueType `json:"metadata"`
}

type Dataset struct {
	PublicId    string `json:"publicId"`
	Name        string `json:"name"`
	Species     string `json:"species"`
	Assembly    string `json:"assembly"`
	Url         string `json:"-"`
	Institution string `json:"institution"`
	Cells       uint   `json:"cells"`
	Id          int    `json:"-"`
	Description string `json:"description"`
}

// type RNASeqGex struct {
// 	Dataset int     `json:"dataset"`
// 	Sample  int     `json:"sample"`
// 	Gene    int     `json:"gene"`
// 	Counts  int     `json:"counts"`
// 	TPM     float32 `json:"tpm"`
// 	VST     float32 `json:"vst"`
// }

// type MicroarrayGex struct {
// 	Dataset int     `json:"dataset"`
// 	Sample  int     `json:"sample"`
// 	Gene    int     `json:"gene"`
// 	RMA     float32 `json:"vst"`
// }

type ResultSample struct {
	//Dataset int     `json:"dataset"`
	Id int `json:"id"`
	//Gene    int     `json:"gene"`
	//Counts int     `json:"counts"`
	////TPM    float32 `json:"tpm"`
	//VST    float32 `json:"vst"`
	Value float32 `json:"value"`
}

type ResultDataset struct {
	Values   []float32 `json:"values"`
	PublicId string    `json:"publicId"`
}

type SearchResults struct {
	// we use the simpler value type for platform in search
	// results so that the value types are not repeated in
	// each search. The useful info in a search is just
	// the platform name and id

	//Dataset *Dataset      `json:"dataset"`
	Dataset  string           `json:"dataset"`
	Features []*ResultFeature `json:"features"`
}

type DatasetsCache struct {
	dir  string
	path string
}

func NewDatasetsCache(dir string) *DatasetsCache {

	path := filepath.Join(dir, "scrna.db")

	// db, err := sql.Open("sqlite3", path)

	// if err != nil {
	// 	log.Fatal().Msgf("%s", err)
	// }

	// defer db.Close()

	return &DatasetsCache{dir: dir, path: path}
}

func (cache *DatasetsCache) Dir() string {
	return cache.dir
}

// func (cache *DatasetsCache) GetGenes(genes []string) ([]*GexGene, error) {
// 	db, err := sql.Open("sqlite3", cache.dir)

// 	if err != nil {
// 		return nil, err
// 	}

// 	defer db.Close()

// 	ret := make([]*GexGene, 0, len(genes))

// 	for _, gene := range genes {
// 		var gexGene GexGene

// 		err := db.QueryRow(GENE_SQL, fmt.Sprintf("%%%s%%", gene)).Scan(&gexGene.Id, &gexGene.GeneId, &gexGene.GeneSymbol)

// 		if err == nil {
// 			ret = append(ret, &gexGene)
// 		}
// 	}

// 	return ret, nil
// }

func (cache *DatasetsCache) Species() ([]string, error) {
	db, err := sql.Open("sqlite3", cache.path)

	if err != nil {
		return nil, err
	}

	defer db.Close()

	species := make([]string, 0, 10)

	rows, err := db.Query(SPECIES_SQL)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var name string

		err := rows.Scan(
			&name)

		if err != nil {
			return nil, err
		}

		species = append(species, name)
	}

	return species, nil
}

func (cache *DatasetsCache) Assemblies(species string) ([]string, error) {
	db, err := sql.Open("sqlite3", cache.path)

	if err != nil {
		return nil, err
	}

	defer db.Close()

	assemblies := make([]string, 0, 10)

	rows, err := db.Query(ASSEMBLIES_SQL, species)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var name string

		err := rows.Scan(
			&name)

		if err != nil {
			return nil, err
		}

		assemblies = append(assemblies, name)
	}

	return assemblies, nil
}

func (cache *DatasetsCache) Datasets(species string, assembly string) ([]*Dataset, error) {

	db, err := sql.Open("sqlite3", cache.path)

	if err != nil {
		return nil, err
	}

	defer db.Close()

	datasets := make([]*Dataset, 0, 10)

	datasetRows, err := db.Query(DATASETS_SQL, species, assembly)

	if err != nil {
		return nil, err
	}

	defer datasetRows.Close()

	for datasetRows.Next() {
		var dataset Dataset

		err := datasetRows.Scan(
			&dataset.Id,
			&dataset.PublicId,
			&dataset.Name,
			&dataset.Institution,
			&dataset.Species,
			&dataset.Assembly,
			&dataset.Cells,
			&dataset.Url,
			&dataset.Description)

		if err != nil {
			return nil, err
		}

		// log.Debug().Msgf("db %s", filepath.Join(cache.dir, dataset.Url))

		// db2, err := sql.Open("sqlite3", filepath.Join(cache.dir, dataset.Url))

		// if err != nil {
		// 	return nil, err
		// }

		// defer db2.Close()

		// err = db2.QueryRow(SAMPLE_COUNT_SQL, dataset.Id).Scan(&dataset.Cells)

		// if err != nil {
		// 	return nil, err
		// }

		datasets = append(datasets, &dataset)
	}

	return datasets, nil
}

func (cache *DatasetsCache) dataset(datasetId string) (*Dataset, error) {
	db, err := sql.Open("sqlite3", cache.path)

	if err != nil {
		return nil, err
	}

	defer db.Close()

	var dataset Dataset

	err = db.QueryRow(DATASET_SQL, datasetId).Scan(
		&dataset.Id,
		&dataset.PublicId,
		&dataset.Name,
		&dataset.Institution,
		&dataset.Species,
		&dataset.Assembly,
		&dataset.Cells,
		&dataset.Url,
		&dataset.Description)

	if err != nil {
		return nil, err
	}

	return &dataset, nil
}

func (cache *DatasetsCache) Gex(datasetIds []string,
	geneIds []string) ([]*SearchResults, error) {

	ret := make([]*SearchResults, 0, len(datasetIds))

	for _, datasetId := range datasetIds {
		dataset, err := cache.dataset(datasetId)

		if err != nil {
			return nil, err
		}

		datasetCache := NewDatasetCache(dataset)

		res, err := datasetCache.Gex(geneIds)

		if err != nil {
			return nil, err
		}

		ret = append(ret, res)
	}

	return ret, nil
}

func (cache *DatasetsCache) Metadata(datasetIds []string) ([]*DatasetMetadata, error) {

	ret := make([]*DatasetMetadata, 0, len(datasetIds))

	for _, datasetId := range datasetIds {
		dataset, err := cache.dataset(datasetId)

		if err != nil {
			return nil, err
		}

		datasetCache := NewDatasetCache(dataset)

		res, err := datasetCache.Metadata()

		if err != nil {
			return nil, err
		}

		ret = append(ret, res)
	}

	return ret, nil
}
