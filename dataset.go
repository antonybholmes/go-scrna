package scrna

import (
	"compress/gzip"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/rs/zerolog/log"
)

// keep them in the entered order so we can preserve
// groupings such as N/GC/M which are not alphabetical
const CELL_COUNT_SQL = `SELECT COUNT(cells.id) FROM cells`

const METADATA_SQL = `SELECT 
	cells.id,
	cells.barcode,
	cells.umap_x,
	cells.umap_y,
	cells.cluster,
	cells.sc_class,
	cells.sample
	FROM cells`

const GENE_SQL = `SELECT 
	genes.id, 
	genes.ensembl_id,
	genes.gene_symbol 
	FROM genes
	WHERE genes.gene_symbol LIKE ?1 OR genes.ensembl_id LIKE ?1
	LIMIT 1`

type Gene struct {
	Ensembl    string `json:"ensembl"`
	GeneSymbol string `json:"geneSymbol"`
	Id         int    `json:"-"`
	File       string `json:"-"`
}

type GexGene struct {
	Id   string      `json:"id"`
	Sym  string      `json:"sym"`
	Data [][]float32 `json:"data"`
}

type Metadata struct {
	Id      string  `json:"-"`
	Barcode string  `json:"barcode"`
	UmapX   float32 `json:"umapX"`
	UmapY   float32 `json:"umapY"`
	Cluster uint    `json:"cluster"`
	ScClass string  `json:"scClass"`
	Sample  string  `json:"sample"`
}

type DatasetMetadata struct {
	PublicId string      `json:"publicId"`
	Metadata []*Metadata `json:"metadata"`
}

// Either a probe or gene
type ResultFeature struct {
	ProbeId string `json:"probeId,omitempty"`
	Gene    *Gene  `json:"gene"`
	//Platform     *ValueType       `json:"platform"`
	//GexValue *GexValue    `json:"gexType"`
	Gex [][]float32 `json:"gex"`
}

type DatasetCache struct {
	dataset *Dataset
}

func NewDatasetCache(dataset *Dataset) *DatasetCache {
	return &DatasetCache{dataset: dataset}
}

func (cache *DatasetCache) FindGenes(genes []string) ([]*Gene, error) {

	db, err := sql.Open("sqlite3", cache.dataset.Url)

	if err != nil {
		return nil, err
	}

	defer db.Close()

	ret := make([]*Gene, 0, len(genes))

	for _, g := range genes {
		var gene Gene
		err := db.QueryRow(GENE_SQL, g).Scan(
			&gene.Id,
			&gene.Ensembl,
			&gene.GeneSymbol,
			&gene.File)

		if err == nil {
			// add as many genes as possible
			ret = append(ret, &gene)
		} else {
			// log that we couldn't find a gene, but continue
			// anyway
			log.Debug().Msgf("gene not found: %s", g)
			//return nil, err
		}
	}

	return ret, nil
}

func (cache *DatasetCache) Gex(
	geneIds []string) (*SearchResults, error) {

	genes, err := cache.FindGenes(geneIds)

	if err != nil {
		return nil, err
	}

	//log.Debug().Msgf("cripes %v", filepath.Join(cache.dir, cache.dataset.Path))

	db, err := sql.Open("sqlite3", cache.dataset.Url)

	datasetUrl := filepath.Dir(cache.dataset.Url)

	//cellCount := cache.dataset.Cells

	if err != nil {
		return nil, err
	}

	defer db.Close()

	ret := SearchResults{
		Dataset:  cache.dataset.PublicId,
		Features: make([]*ResultFeature, 0, len(genes)),
	}

	var gexCache = make(map[string][]GexGene)

	for _, gene := range genes {
		gexFile := filepath.Join(datasetUrl, gene.File)

		gexData, ok := gexCache[gexFile]

		if !ok {

			f, err := os.Open(gexFile)
			if err != nil {
				return nil, err
			}
			defer f.Close()

			// Create gzip reader
			gz, err := gzip.NewReader(f)
			if err != nil {
				return nil, err
			}
			defer gz.Close()

			// Example 1: decode into a map (for JSON object)
			var data []GexGene

			if err := json.NewDecoder(gz).Decode(&data); err != nil {
				return nil, err
			}

			gexCache[gexFile] = data
			gexData = data
		}

		// find the index of our gene

		geneIndex := -1

		for i, g := range gexData {
			if g.Id == gene.Ensembl {
				geneIndex = i
				break
			}
		}

		if geneIndex == -1 {
			return nil, err
		}

		gexGeneData := gexData[geneIndex]

		// values := make([][]float32, 0, cellCount)

		// for _, gex := range gexGeneData.Data {
		// 	// data is sparse consisting of index, value pairs
		// 	// which we use to fill in the array
		// 	//i := uint32(gex[0])
		// 	//values[i] = gex[1]
		// 	//values[i] = gex
		// 	values = append(values, gex)
		// }

		//log.Debug().Msgf("hmm %s %f %f", gexType, sample.Value, tpm)

		//datasetResults.Samples = append(datasetResults.Samples, &sample)
		ret.Features = append(ret.Features, &ResultFeature{Gene: gene, Gex: gexGeneData.Data})

	}

	return &ret, nil
}

func (cache *DatasetCache) Metadata() (*DatasetMetadata, error) {

	//log.Debug().Msgf("cripes %v", filepath.Join(cache.dir, cache.dataset.Path))

	db, err := sql.Open("sqlite3", cache.dataset.Url)

	if err != nil {
		return nil, err
	}

	defer db.Close()

	var cellCount uint

	err = db.QueryRow(CELL_COUNT_SQL).Scan(&cellCount)

	if err != nil {
		return nil, err
	}

	ret := make([]*Metadata, 0, cellCount)

	rows, err := db.Query(METADATA_SQL)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var metadata Metadata

		err := rows.Scan(
			&metadata.Id,
			&metadata.Barcode,
			&metadata.UmapX,
			&metadata.UmapY,
			&metadata.Cluster,
			&metadata.ScClass,
			&metadata.Sample)

		if err != nil {
			return nil, err
		}

		ret = append(ret, &metadata)
	}

	return &DatasetMetadata{
		PublicId: cache.dataset.PublicId,
		Metadata: ret,
	}, nil
}
