package scrna

import (
	"database/sql"
	"fmt"
	"path/filepath"

	"github.com/antonybholmes/go-sys"
	"github.com/rs/zerolog/log"
)

// keep them in the entered order so we can preserve
// groupings such as N/GC/M which are not alphabetical
const CELL_COUNT_SQL = `SELECT COUNT(cells.id) FROM cells`

const CLUSTERS_SQL = `SELECT 
	clusters.id,
	clusters.cluster_id,
	clusters.sc_group,
	clusters.sc_class,
	clusters.cell_count,
	clusters.color
	FROM clusters`

const CELLS_SQL = `SELECT 
	cells.id,
	cells.barcode,
	cells.umap_x,
	cells.umap_y,
	cells.cluster_id,
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
	gex.file,
	gex.offset
	FROM gex
	WHERE gex.gene_symbol LIKE ?1 OR gex.ensembl_id LIKE ?1
	LIMIT 1`

const SEARCH_GENE_SQL = `SELECT id, ensembl_id, gene_symbol FROM gex WHERE `

type Dataset struct {
	PublicId    string `json:"publicId"`
	Name        string `json:"name"`
	Species     string `json:"species"`
	Assembly    string `json:"assembly"`
	Url         string `json:"-"`
	Institution string `json:"institution"`
	Description string `json:"description"`
	Cells       uint   `json:"cells"`
	Id          int    `json:"-"`
}

type Gene struct {
	Ensembl    string `json:"id"`
	GeneSymbol string `json:"sym"`
	File       string `json:"-"`
	Id         int    `json:"-"`
	Offset     int64  `json:"-"`
}

// Used only for reading data
type GexDataGene struct {
	Ensembl    string      `json:"id" msgpack:"id"`
	GeneSymbol string      `json:"sym" msgpack:"sym"`
	Data       [][]float64 `json:"gex" msgpack:"gex"`
}

// More human readable for output
// type GexResultGene struct {
// 	Ensembl    string      `json:"geneId"`
// 	GeneSymbol string      `json:"geneSymbol"`
// 	Gex        [][]float64 `json:"gex"`
// }

type Cluster struct {
	Id        string `json:"-"`
	Group     string `json:"group"`
	ScClass   string `json:"scClass"`
	Color     string `json:"color"`
	ClusterId uint   `json:"clusterId"`
	CellCount uint   `json:"cells"`
}

type DatasetClusters struct {
	PublicId string     `json:"publicId"`
	Clusters []*Cluster `json:"clusters"`
}

type Pos struct {
	X float32 `json:"x"`
	Y float32 `json:"y"`
}

type SingleCell struct {
	Id      string `json:"-"`
	Barcode string `json:"barcode"`
	Sample  string `json:"sample"`
	Cluster uint   `json:"clusterId"`
	//UmapX   float32 `json:"umapX"`
	//UmapY   float32 `json:"umapY"`
	Pos Pos `json:"pos"`
}

type DatasetMetadata struct {
	PublicId string        `json:"publicId"`
	Clusters []*Cluster    `json:"clusters"`
	Cells    []*SingleCell `json:"cells"`
}

// Either a probe or gene
// type ResultFeature struct {
// 	//ProbeId string `json:"probeId,omitempty"`
// 	Gene *Gene `json:"gene"`
// 	//Platform     *ValueType       `json:"platform"`
// 	//GexValue *GexValue    `json:"gexType"`
// 	Gex [][]float32 `json:"gex"`
// }

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
			&gene.File,
			&gene.Offset)

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
	geneIds []string) (*GexResults, error) {

	genes, err := cache.FindGenes(geneIds)

	if err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite3", cache.dataset.Url)

	datasetUrl := filepath.Dir(cache.dataset.Url)

	// where the gex data is located
	gexUrl := filepath.Join(datasetUrl, "gex")

	//cellCount := cache.dataset.Cells

	if err != nil {
		return nil, err
	}

	defer db.Close()

	ret := GexResults{
		Dataset: cache.dataset.PublicId,
		Genes:   make([]*GexDataGene, 0, len(genes)),
	}

	var gexCache = make(map[string]*GexDataGene)

	for _, gene := range genes {
		gexFile := filepath.Join(gexUrl, gene.File)

		gexData, ok := gexCache[gexFile]

		if !ok {

			// f, err := os.Open(gexFile)
			// if err != nil {
			// 	return nil, err
			// }
			// defer f.Close()

			data, err := SeekRecordFromDat(gexFile, gene.Offset)

			if err != nil {
				return nil, err
			}

			// Create gzip reader
			// gz, err := gzip.NewReader(f)
			// if err != nil {
			// 	return nil, err
			// }
			// defer gz.Close()

			// // Example 1: decode into a map (for JSON object)
			// var data []GexFileDataGene

			// if err := json.NewDecoder(gz).Decode(&data); err != nil {
			// 	return nil, err
			// }

			gexCache[gexFile] = data
			gexData = data
		}

		// find the index of our gene

		// geneIndex := -1

		// for i, g := range gexData {
		// 	if g.Ensembl == gene.Ensembl {
		// 		geneIndex = i
		// 		break
		// 	}
		// }

		// if geneIndex == -1 {
		// 	return nil, fmt.Errorf("%s not found", gene.GeneSymbol)
		// }

		//gexGeneData := gexData[geneIndex]

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
		ret.Genes = append(ret.Genes, gexData)

	}

	return &ret, nil
}

// func (dataset *DatasetCache) Clusters() (*DatasetClusters, error) {

// 	//log.Debug().Msgf("cripes %v", filepath.Join(cache.dir, cache.dataset.Path))

// 	db, err := sql.Open("sqlite3", dataset.dataset.Url)

// 	if err != nil {
// 		return nil, err
// 	}

// 	defer db.Close()

// 	ret := make([]*Cluster, 0, 30)

// 	rows, err := db.Query(CLUSTERS_SQL)

// 	if err != nil {
// 		return nil, err
// 	}

// 	defer rows.Close()

// 	for rows.Next() {
// 		var cluster Cluster

// 		err := rows.Scan(
// 			&cluster.Id,
// 			&cluster.ClusterId,
// 			&cluster.Group,
// 			&cluster.ScClass,
// 			&cluster.Color)

// 		if err != nil {
// 			return nil, err
// 		}

// 		ret = append(ret, &cluster)
// 	}

// 	return &DatasetClusters{
// 		PublicId: dataset.dataset.PublicId,
// 		Clusters: ret,
// 	}, nil
// }

func (dataset *DatasetCache) Metadata() (*DatasetMetadata, error) {

	//log.Debug().Msgf("cripes %v", filepath.Join(cache.dir, cache.dataset.Path))

	db, err := sql.Open("sqlite3", dataset.dataset.Url)

	if err != nil {
		return nil, err
	}

	defer db.Close()

	clusters := make([]*Cluster, 0, 30)

	rows, err := db.Query(CLUSTERS_SQL)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var cluster Cluster

		err := rows.Scan(
			&cluster.Id,
			&cluster.ClusterId,
			&cluster.Group,
			&cluster.ScClass,
			&cluster.CellCount,
			&cluster.Color)

		if err != nil {
			return nil, err
		}

		clusters = append(clusters, &cluster)
	}

	var cellCount uint

	err = db.QueryRow(CELL_COUNT_SQL).Scan(&cellCount)

	if err != nil {
		return nil, err
	}

	cells := make([]*SingleCell, 0, cellCount)

	rows, err = db.Query(CELLS_SQL)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var cell SingleCell

		err := rows.Scan(
			&cell.Id,
			&cell.Barcode,
			&cell.Pos.X,
			&cell.Pos.Y,
			&cell.Cluster,
			&cell.Sample)

		if err != nil {
			return nil, err
		}

		cells = append(cells, &cell)
	}

	return &DatasetMetadata{
		PublicId: dataset.dataset.PublicId,
		Clusters: clusters,
		Cells:    cells,
	}, nil
}

func (cache *DatasetCache) Genes() ([]*Gene, error) {

	//log.Debug().Msgf("cripes %v", filepath.Join(cache.dir, cache.dataset.Path))

	db, err := sql.Open("sqlite3", cache.dataset.Url)

	if err != nil {
		return nil, err
	}

	defer db.Close()

	ret := make([]*Gene, 0, 50000)

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

	// whereSql, args := sys.BoolQuery(query, func(placeholder string, exact bool) string {

	// 	// if exact {
	// 	// 	return "(gex.gene_symbol = ? OR gex.ensembl_id = ?)"
	// 	// } else {
	// 	// 	return fmt.Sprintf("(gex.gene_symbol LIKE %s OR gex.ensembl_id LIKE %s)", placeholder, placeholder)
	// 	// }

	// 	return fmt.Sprintf("(gex.gene_symbol LIKE %s OR gex.ensembl_id LIKE %s)", placeholder, placeholder)
	// })

	where, err := sys.SqlBoolQuery(query, func(placeholder uint, matchType sys.MatchType) string {

		// if exact {
		// 	return "(gex.gene_symbol = ? OR gex.ensembl_id = ?)"
		// } else {
		// 	return fmt.Sprintf("(gex.gene_symbol LIKE %s OR gex.ensembl_id LIKE %s)", placeholder, placeholder)
		// }

		ph := fmt.Sprintf("?%d", placeholder)

		return fmt.Sprintf("(gex.gene_symbol LIKE %s OR gex.ensembl_id LIKE %s)", ph, ph)
	})

	if err != nil {
		return nil, err
	}

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

	finalSQL := SEARCH_GENE_SQL + where.Sql + fmt.Sprintf(" ORDER BY gex.gene_symbol LIMIT %d", limit)

	//log.Debug().Msgf("query %s", query)
	//log.Debug().Msgf("sql %s", finalSQL)
	//log.Debug().Msgf("args %v", args)

	db, err := sql.Open("sqlite3", cache.dataset.Url)

	if err != nil {
		return nil, err
	}

	defer db.Close()

	ret := make([]*Gene, 0, limit)

	rows, err := db.Query(finalSQL, where.Args...)

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
