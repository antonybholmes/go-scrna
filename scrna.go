package scrna

import (
	"database/sql"
	"path/filepath"

	"github.com/rs/zerolog/log"
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

const TECHNOLOGIES_SQL = `SELECT
	datasets.platform
	FROM datasets
	WHERE datasets.species = ?1 
	ORDER BY datasets.platform`

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
	datasets.species,
	datasets.technology,
	datasets.platform,
	datasets.institution,
	datasets.name,
	datasets.path,
	datasets.description
	FROM datasets 
	WHERE datasets.species = ?1 AND datasets.technology = ?2
	ORDER BY datasets.name`

const DATASET_SQL = `SELECT 
	datasets.id,
	datasets.public_id,
	datasets.species,
	datasets.technology,
	datasets.platform,
	datasets.institution,
	datasets.name,
	datasets.path,
	datasets.description
	FROM datasets 
	WHERE datasets.public_id = ?1`

// const DATASETS_SQL = `SELECT
// 	name
// 	FROM datasets
// 	ORDER BY datasets.name`

// type GexValue string

const (
	RNA_SEQ_TECHNOLOGY    string = "RNA-seq"
	MICROARRAY_TECHNOLOGY string = "Microarray"
)

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

type GexGene struct {
	Ensembl    string `json:"ensembl"`
	Refseq     string `json:"refseq"`
	Hugo       string `json:"hugo"`
	Mgi        string `json:"mgi"`
	GeneSymbol string `json:"geneSymbol"`
	Id         int    `json:"-"`
}

type Technology struct {
	Name     string   `json:"name"`
	PublicId string   `json:"publicId"`
	GexTypes []string `json:"gexTypes"`
}

type Sample struct {
	PublicId string          `json:"publicId"`
	Name     string          `json:"name"`
	AltNames []string        `json:"altNames"`
	Id       int             `json:"-"`
	Metadata []NameValueType `json:"metadata"`
}

type Dataset struct {
	PublicId    string    `json:"publicId"`
	Name        string    `json:"name"`
	Species     string    `json:"species"`
	Technology  string    `json:"technology"`
	Platform    string    `json:"platform"`
	Path        string    `json:"-"`
	Institution string    `json:"institution"`
	Samples     []*Sample `json:"samples"`
	Id          int       `json:"id"`
	Description string    `json:"description"`
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

// Either a probe or gene
type ResultFeature struct {
	ProbeId string   `json:"probeId,omitempty"`
	Gene    *GexGene `json:"gene"`
	//Platform     *ValueType       `json:"platform"`
	//GexValue *GexValue    `json:"gexType"`
	Expression []float32 `json:"expression"`
}

type SearchResults struct {
	// we use the simpler value type for platform in search
	// results so that the value types are not repeated in
	// each search. The useful info in a search is just
	// the platform name and id

	//Dataset *Dataset      `json:"dataset"`
	Dataset  string           `json:"dataset"`
	GexType  string           `json:"gexType"`
	Features []*ResultFeature `json:"features"`
}

type DatasetsCache struct {
	dir  string
	path string
}

func NewDatasetsCache(dir string) *DatasetsCache {

	path := filepath.Join(dir, "gex.db")

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

func (cache *DatasetsCache) Technologies(species string) ([]string, error) {
	db, err := sql.Open("sqlite3", cache.path)

	if err != nil {

		return nil, err
	}

	defer db.Close()

	platforms := make([]string, 0, 10)

	rows, err := db.Query(TECHNOLOGIES_SQL, species)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var platform string

		err := rows.Scan(
			&platform)

		if err != nil {
			return nil, err
		}

		platforms = append(platforms, platform)
	}

	return platforms, nil
}

func (cache *DatasetsCache) AllTechnologies() (map[string]map[string][]string, error) {
	db, err := sql.Open("sqlite3", cache.path)

	if err != nil {

		return nil, err
	}

	defer db.Close()

	technologies := make(map[string]map[string][]string)

	rows, err := db.Query(ALL_TECHNOLOGIES_SQL)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var species string
	var technology string
	var platform string
	for rows.Next() {

		err := rows.Scan(&species,
			&technology,
			&platform)

		if err != nil {
			return nil, err
		}

		if technologies[species] == nil {
			technologies[species] = make(map[string][]string)
		}

		if technologies[species][technology] == nil {
			technologies[species][technology] = make([]string, 0, 10)
		}

		technologies[species][technology] = append(technologies[species][technology], platform)

	}

	return technologies, nil
}

// func (cache *DatasetsCache) GexValues(platform int) ([]*GexValue, error) {

// 	db, err := sql.Open("sqlite3", cache.dir)

// 	if err != nil {
// 		return nil, err
// 	}

// 	defer db.Close()

// 	valueTypes := make([]*GexValue, 0, 10)

// 	rows, err := db.Query(VALUE_TYPES_SQL, platform)

// 	if err != nil {
// 		return nil, err
// 	}

// 	defer rows.Close()

// 	for rows.Next() {
// 		var valueType GexValue

// 		err := rows.Scan(
// 			&valueType.Id,
// 			&valueType.Name)

// 		if err != nil {
// 			return nil, err
// 		}

// 		valueTypes = append(valueTypes, &valueType)
// 	}

// 	return valueTypes, nil
// }

func (cache *DatasetsCache) Datasets(species string, technology string) ([]*Dataset, error) {
	var name string
	var value string

	db, err := sql.Open("sqlite3", cache.path)

	if err != nil {
		return nil, err
	}

	defer db.Close()

	datasets := make([]*Dataset, 0, 10)

	datasetRows, err := db.Query(DATASETS_SQL, species, technology)

	if err != nil {
		return nil, err
	}

	defer datasetRows.Close()

	for datasetRows.Next() {
		var dataset Dataset

		err := datasetRows.Scan(
			&dataset.Id,
			&dataset.PublicId,
			&dataset.Species,
			&dataset.Technology,
			&dataset.Platform,
			&dataset.Institution,
			&dataset.Name,
			&dataset.Path,
			&dataset.Description)

		if err != nil {
			return nil, err
		}

		// the largest dataset is around 500 samples
		// so use that as an estimate
		dataset.Samples = make([]*Sample, 0, DATASET_SIZE)

		log.Debug().Msgf("db %s", filepath.Join(cache.dir, dataset.Path))

		db2, err := sql.Open("sqlite3", filepath.Join(cache.dir, dataset.Path))

		if err != nil {
			return nil, err
		}

		defer db2.Close()

		geneRows, err := db2.Query(SAMPLES_SQL, dataset.Id)

		if err != nil {
			return nil, err
		}

		defer geneRows.Close()

		for geneRows.Next() {
			var sample Sample

			err := geneRows.Scan(
				&sample.Id,
				&sample.PublicId,
				&sample.Name)

			if err != nil {
				return nil, err
			}

			//
			// See if sample has alternative names
			//

			sample.AltNames = make([]string, 0, 10)

			dataRows, err := db2.Query(SAMPLE_ALT_NAMES_SQL, sample.Id)

			if err != nil {
				return nil, err
			}

			defer dataRows.Close()

			for dataRows.Next() {

				err := dataRows.Scan(&name, &value)

				if err != nil {
					return nil, err
				}

				sample.AltNames = append(sample.AltNames, value)
			}

			//
			// Attach sample meta data
			//

			sample.Metadata = make([]NameValueType, 0, 50)

			//sample.Metadata =) make(map[string]string)

			dataRows, err = db2.Query(SAMPLE_METADATA_SQL, sample.Id)

			if err != nil {
				return nil, err
			}

			defer dataRows.Close()

			for dataRows.Next() {
				err := dataRows.Scan(&name, &value)

				if err != nil {
					return nil, err
				}

				sample.Metadata = append(sample.Metadata, NameValueType{Name: name, Value: value})
			}

			dataset.Samples = append(dataset.Samples, &sample)
		}

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
		&dataset.Species,
		&dataset.Technology,
		&dataset.Platform,
		&dataset.Institution,
		&dataset.Name,
		&dataset.Path,
		&dataset.Description)

	if err != nil {
		return nil, err
	}

	return &dataset, nil
}

func (cache *DatasetsCache) FindRNASeqValues(datasetIds []string,
	gexType string,
	geneIds []string) ([]*SearchResults, error) {

	ret := make([]*SearchResults, 0, len(datasetIds))

	for _, datasetId := range datasetIds {
		dataset, err := cache.dataset(datasetId)

		if err != nil {
			return nil, err
		}

		datasetCache := NewDatasetCache(cache.dir, dataset)

		res, err := datasetCache.FindRNASeqValues(gexType, geneIds)

		if err != nil {
			return nil, err
		}

		ret = append(ret, res)
	}

	return ret, nil
}

func (cache *DatasetsCache) FindMicroarrayValues(datasetIds []string,
	geneIds []string) ([]*SearchResults, error) {

	ret := make([]*SearchResults, 0, len(datasetIds))

	for _, datasetId := range datasetIds {
		dataset, err := cache.dataset(datasetId)

		if err != nil {
			return nil, err
		}

		datasetCache := NewDatasetCache(cache.dir, dataset)

		res, err := datasetCache.FindMicroarrayValues(geneIds)

		if err != nil {
			return nil, err
		}

		ret = append(ret, res)
	}

	return ret, nil
}
