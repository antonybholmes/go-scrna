package scrna

import (
	"compress/gzip"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/antonybholmes/go-web"
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

const GENES_SQL = `SELECT 
	gex.id, 
	gex.ensembl_id,
	gex.gene_symbol 
	FROM gex
	ORDER BY gex.gene_symbol`

const FIND_GENE_SQL = `SELECT 
	gex.id, 
	gex.ensembl_id,
	gex.gene_symbol,
	gex.file
	FROM gex
	WHERE gex.gene_symbol LIKE ?1 OR gex.ensembl_id LIKE ?1
	LIMIT 1`

const SEARCH_GENE_SQL = `SELECT id, ensembl_id, gene_symbol FROM gex WHERE `

type Gene struct {
	Ensembl    string `json:"ens"`
	GeneSymbol string `json:"sym"`
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
		err := db.QueryRow(FIND_GENE_SQL, g).Scan(
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

func (cache *DatasetCache) Genes() ([]*Gene, error) {

	//log.Debug().Msgf("cripes %v", filepath.Join(cache.dir, cache.dataset.Path))

	db, err := sql.Open("sqlite3", cache.dataset.Url)

	if err != nil {
		return nil, err
	}

	defer db.Close()

	ret := make([]*Gene, 0, 40000)

	rows, err := db.Query(GENES_SQL)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var gene Gene

		err := rows.Scan(
			&gene.Id,
			&gene.Ensembl,
			&gene.GeneSymbol)

		if err != nil {
			return nil, err
		}

		ret = append(ret, &gene)
	}

	return ret, nil
}

func (cache *DatasetCache) SearchGenes(query string, limit uint16) ([]*Gene, error) {

	whereSql, args := web.BoolQuery(query, func(placeholder string) string {
		return fmt.Sprintf("(gex.gene_symbol LIKE %s OR gex.ensembl_id LIKE %s)", placeholder, placeholder)
	})

	// _, andTags := web.ParseQuery(query)

	// andClauses := make([]string, 0, len(andTags))

	// // required so that we can use it with sqlite params
	// args := make([]interface{}, 0, len(andTags))

	// for _, group := range andTags {
	// 	tagClauses := make([]string, 0, len(group))
	// 	for _, tag := range group {
	// 		args = append(args, "%"+tag+"%")
	// 		placeholder := fmt.Sprintf("?%d", len(args))
	// 		tagClauses = append(tagClauses, fmt.Sprintf("(gex.gene_symbol LIKE %s OR gex.ensembl_id LIKE %s)", placeholder, placeholder))
	// 	}
	// 	andClauses = append(andClauses, "("+strings.Join(tagClauses, " AND ")+")")
	// }

	finalSQL := SEARCH_GENE_SQL + whereSql + fmt.Sprintf(" ORDER BY gex.gene_symbol LIMIT %d", limit)

	//log.Debug().Msgf("query %s", query)
	//log.Debug().Msgf("sql %s", finalSQL)
	//log.Debug().Msgf("args %v", args)

	db, err := sql.Open("sqlite3", cache.dataset.Url)

	if err != nil {
		return nil, err
	}

	defer db.Close()

	ret := make([]*Gene, 0, limit)

	rows, err := db.Query(finalSQL, args...)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var gene Gene

		err := rows.Scan(
			&gene.Id,
			&gene.Ensembl,
			&gene.GeneSymbol)

		if err != nil {
			return nil, err
		}

		ret = append(ret, &gene)
	}

	return ret, nil
}
