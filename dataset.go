package scrna

import (
	"database/sql"
	"fmt"
	"path/filepath"

	"github.com/antonybholmes/go-scrna/dat"
	v2 "github.com/antonybholmes/go-scrna/dat/v2"
	"github.com/antonybholmes/go-sys"
	"github.com/antonybholmes/go-sys/log"
	"github.com/antonybholmes/go-sys/query"
)

type (
	Dataset struct {
		Id          string `json:"id"`
		Name        string `json:"name"`
		Species     string `json:"species"`
		Assembly    string `json:"assembly"`
		Url         string `json:"-"`
		Institution string `json:"institution"`
		Description string `json:"description"`
		Cells       int    `json:"cells"`
	}

	Gene struct {
		Id         string `json:"id"`
		Ensembl    string `json:"geneId"`
		GeneSymbol string `json:"geneSymbol"`
		File       string `json:"-"`
		Offset     int64  `json:"-"`
		Size       int32  `json:"-"`
	}

	// More human readable for output
	//   GexResultGene struct {
	// 	Ensembl    string      `json:"geneId"`
	// 	GeneSymbol string      `json:"geneSymbol"`
	// 	Gex        [][]float64 `json:"gex"`
	// }

	ClusterMetadata struct {
		Id          string `json:"id"`
		Name        string `json:"name"`
		Value       string `json:"value"`
		Description string `json:"description,omitempty"`
		Color       string `json:"color,omitempty"`
	}

	Cluster struct {
		Id        string                      `json:"id"`
		Metadata  map[string]*ClusterMetadata `json:"metadata,omitempty"`
		Color     string                      `json:"color"`
		Name      string                      `json:"name"`
		CellCount int                         `json:"cells"`
	}

	Pos struct {
		X float32 `json:"x"`
		Y float32 `json:"y"`
	}

	SingleCell struct {
		Id      string `json:"id"`
		Sample  string `json:"sampleId"`
		Cluster string `json:"clusterId"`
		Barcode string `json:"barcode"`
		Pos     Pos    `json:"pos"`
	}

	DatasetMetadata struct {
		Dataset  string        `json:"datasetId"`
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

	DatasetDB struct {
		dataset *Dataset
		db      *sql.DB
	}
)

// keep them in the entered order so we can preserve
// groupings such as N/GC/M which are not alphabetical
const (
	CellCountSql = `SELECT COUNT(cells.id) FROM cells`

	ClustersSql = `SELECT 
		clusters.id,
		clusters.name,
		clusters.cell_count,
		clusters.color
		FROM clusters`

	ClusterMetadataSQL = `SELECT
		cluster_metadata.id,
		cluster_metadata.cluster_id,
		metadata_types.name,
		metadata.value,
		metadata.color
		FROM cluster_metadata
		JOIN metadata ON cluster_metadata.metadata_id = metadata.id
		JOIN metadata_types ON metadata.metadata_type_id = metadata_types.id
		ORDER by cluster_metadata.cluster_id, metadata_types.id, metadata.id`

	CellsSql = `SELECT 
		cells.id,
		cells.barcode,
		cells.umap_x,
		cells.umap_y,
		cells.cluster_id,
		samples.name
		FROM cells
		JOIN samples ON cells.sample_id = samples.id
		ORDER BY cells.id`

	GenesSql = `SELECT 
		gex.id, 
		gex.ensembl_id,
		gex.gene_symbol 
		FROM gex
		ORDER BY gex.gene_symbol`

	FindGeneSql = `SELECT 
		gex.id, 
		gex.ensembl_id,
		gex.gene_symbol,
		gex.file,
		gex.offset,
		gex.size
		FROM gex
		WHERE gex.gene_symbol LIKE ?1 OR gex.ensembl_id LIKE ?1
		LIMIT 1`

	SearchGeneSql = `SELECT id, ensembl_id, gene_symbol FROM gex WHERE `
)

func NewDatasetDB(dataset *Dataset) *DatasetDB {
	return &DatasetDB{dataset: dataset,
		db: sys.Must(sql.Open(sys.Sqlite3DB, dataset.Url))}
}

func (dsdb *DatasetDB) Close() error {
	return dsdb.db.Close()
}

func (dsdb *DatasetDB) FindGenes(genes []string) ([]*Gene, error) {

	ret := make([]*Gene, 0, len(genes))

	for _, g := range genes {
		var gene Gene
		err := dsdb.db.QueryRow(FindGeneSql, g).Scan(
			&gene.Id,
			&gene.Ensembl,
			&gene.GeneSymbol,
			&gene.File,
			&gene.Offset,
			&gene.Size)

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

func (dsdb *DatasetDB) Gex(
	geneIds []string) (*dat.GexResults, error) {

	genes, err := dsdb.FindGenes(geneIds)

	if err != nil {
		return nil, err
	}

	datasetUrl := filepath.Dir(dsdb.dataset.Url)

	// where the gex data is located
	gexUrl := filepath.Join(datasetUrl, "gex")

	//cellCount := cache.dataset.Cells

	ret := dat.GexResults{
		DatasetId: dsdb.dataset.Id, //dat.ResultDataset{Id: dc.dataset.Id},
		Genes:     make([]*dat.GexGene, 0, len(genes)),
	}

	var gexCache = make(map[string]*dat.GexGene)

	for _, gene := range genes {
		gexFile := filepath.Join(gexUrl, gene.File)

		gexData, ok := gexCache[gexFile]

		if !ok {

			// f, err := os.Open(gexFile)
			// if err != nil {
			// 	return nil, err
			// }
			// defer f.Close()

			data, err := v2.SeekGexGeneFromDat(gexFile, gene.Offset)

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
		// 	//i := int32(gex[0])
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

func (dsdb *DatasetDB) Metadata() (*DatasetMetadata, error) {

	//log.Debug().Msgf("cripes %v", filepath.Join(cache.dir, cache.dataset.Path))

	rows, err := dsdb.db.Query(ClustersSql)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	clusters := make([]*Cluster, 0, 50)
	clusterMap := make(map[string]*Cluster)

	for rows.Next() {
		var cluster Cluster

		err := rows.Scan(
			&cluster.Id,
			&cluster.Name,
			&cluster.CellCount,
			&cluster.Color)

		if err != nil {
			return nil, err
		}

		cluster.Metadata = make(map[string]*ClusterMetadata, 5)

		clusters = append(clusters, &cluster)
		clusterMap[cluster.Id] = &cluster
	}

	// add metadata to clusters

	var clusterId string

	rows, err = dsdb.db.Query(ClusterMetadataSQL)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	log.Debug().Msgf("Dataset cluster s: %d", len(clusters))

	for rows.Next() {
		var md = ClusterMetadata{}

		err := rows.Scan(&md.Id, &clusterId, &md.Name, &md.Value, &md.Color)

		if err != nil {
			return nil, err
		}

		//index := clusterId - 1

		log.Debug().Msgf("Adding metadata to cluster: %s %s", clusterId, md.Name)

		clusterMap[clusterId].Metadata[md.Name] = &md
	}

	var cellCount int

	err = dsdb.db.QueryRow(CellCountSql).Scan(&cellCount)

	if err != nil {
		return nil, err
	}

	cells := make([]*SingleCell, 0, cellCount)

	rows, err = dsdb.db.Query(CellsSql)

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
		Dataset:  dsdb.dataset.Id,
		Clusters: clusters,
		Cells:    cells,
	}, nil
}

func (dsdb *DatasetDB) Genes() ([]*Gene, error) {

	//log.Debug().Msgf("cripes %v", filepath.Join(cache.dir, cache.dataset.Path))

	// 50k for the num of genes we expect
	ret := make([]*Gene, 0, 50000)

	rows, err := dsdb.db.Query(GenesSql)

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

func (dsdb *DatasetDB) SearchGenes(q string, limit int16) ([]*Gene, error) {

	where, err := query.SqlBoolQuery(q, func(placeholderIndex int, value string, addParens bool) string {
		// for slqlite
		ph := query.IndexedParam(placeholderIndex)

		// if matchType == sys.MatchTypeExact {
		// 	return fmt.Sprintf("(gex.gene_symbol = %s OR gex.ensembl_id = %s)", ph, ph)
		// } else {
		// 	return fmt.Sprintf("(gex.gene_symbol LIKE %s OR gex.ensembl_id LIKE %s)", ph, ph)
		// }

		// we use like even for exact matches to allow for case insensitivity
		return query.AddParens("gex.gene_symbol LIKE "+ph+" OR gex.ensembl_id LIKE "+ph, addParens)
	})

	if err != nil {
		return nil, err
	}

	finalSql := SearchGeneSql + where.Sql + fmt.Sprintf(" ORDER BY gex.gene_symbol LIMIT %d", limit)

	//log.Debug().Msgf("finalSQL %s", finalSQL)

	ret := make([]*Gene, 0, limit)

	rows, err := dsdb.db.Query(finalSql, query.IndexedNamedArgs(where.Args)...)

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
