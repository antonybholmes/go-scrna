package scrna

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/antonybholmes/go-scrna/dat"
	"github.com/antonybholmes/go-sys"
	"github.com/antonybholmes/go-sys/log"
	"github.com/antonybholmes/go-sys/query"
	"github.com/antonybholmes/go-web"
	"github.com/antonybholmes/go-web/auth/sqlite"
)

type (
	Idtype struct {
		Id   string `json:"id"`
		Name string `json:"name"`
	}

	NameValueType struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	}

	Species  = Idtype
	GexValue = Idtype

	// type GexType string

	// const (
	// 	GEX_TYPE_RNA_SEQ        GexType = "RNA-seq"
	// 	GEX_TYPE_RNA_MICROARRAY GexType = "Microarray"
	//)

	Dataset struct {
		Id       string `json:"id"`
		Name     string `json:"name"`
		Species  string `json:"species"`
		Assembly string `json:"assembly"`
		//Url         string `json:"-"`
		Institution string `json:"institution"`
		Description string `json:"description"`
		Cells       int    `json:"cells"`
	}

	Sample struct {
		Id       string          `json:"id"`
		Name     string          `json:"name"`
		AltNames []string        `json:"altNames"`
		Metadata []NameValueType `json:"metadata"`
	}

	Gene struct {
		Id         string `json:"id"`
		Ensembl    string `json:"geneId"`
		GeneSymbol string `json:"geneSymbol"`
		Url        string `json:"-"`
		Offset     int64  `json:"-"`
		Size       int64  `json:"-"`
	}

	ClusterMetadata struct {
		//Id          string `json:"-"`
		Name        string `json:"name"`
		Value       string `json:"value"`
		Description string `json:"description,omitempty"`
		//Color       string `json:"color,omitempty"`
	}

	Cluster struct {
		Metadata  map[string]string `json:"metadata,omitempty"`
		Id        string            `json:"id"`
		Color     string            `json:"color"`
		Name      string            `json:"name"`
		Label     int               `json:"label"`
		CellCount int               `json:"cells"`
	}

	Pos struct {
		X float64 `json:"x"`
		Y float64 `json:"y"`
	}

	SingleCell struct {
		SampleName string `json:"sampleName"`
		Barcode    string `json:"barcode,omitempty"`
		Pos
		ClusterLabel int `json:"clusterLabel"`
	}

	DatasetMetadata struct {
		Dataset  string        `json:"dataset"`
		Clusters []*Cluster    `json:"clusters"`
		Cells    []*SingleCell `json:"cells"`
	}

	//  RNASeqGex struct {
	// 	Dataset int     `json:"dataset"`
	// 	Sample  int     `json:"sample"`
	// 	Gene    int     `json:"gene"`
	// 	Counts  int     `json:"counts"`
	// 	TPM     float32 `json:"tpm"`
	// 	VST     float32 `json:"vst"`
	// }

	//  MicroarrayGex struct {
	// 	Dataset int     `json:"dataset"`
	// 	Sample  int     `json:"sample"`
	// 	Gene    int     `json:"gene"`
	// 	RMA     float32 `json:"vst"`
	// }

	ResultSample struct {
		//Dataset int     `json:"dataset"`
		Id string `json:"id"`
		//Gene    int     `json:"gene"`
		//Counts int     `json:"counts"`
		////TPM    float32 `json:"tpm"`
		//VST    float32 `json:"vst"`
		Value float32 `json:"value"`
	}

	ScrnaDB struct {
		db  *sql.DB
		dir string
	}
)

// approx size of dataset
const (
	DatasetSize = 500

	CellCountSql = `SELECT COUNT(cells.id) FROM cells`

	GenomesSQL = `SELECT DISTINCT
		g.id,
		g.public_id,
		g.name
		FROM genomes g
		ORDER BY g.name`

	AssembliesSql = `SELECT
		a.id,
		a.public_id,
		a.name
		FROM assemblies a
		JOIN genomes g ON a.genome_id = g.id
		WHERE g.public_id = :genome OR LOWER(g.name) = :genome
		ORDER BY a.name`

	AllTechnologiesSql = `SELECT DISTINCT
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

	DatasetsSql = `SELECT DISTINCT
		d.public_id,
		d.name,
		d.institution,
		g.name AS genome,
		a.name AS assembly,
		d.cells,
		d.description
		FROM datasets d
		JOIN assemblies a ON d.assembly_id = a.id
		JOIN genomes g ON a.genome_id = g.id
		JOIN dataset_permissions dp ON d.id = dp.dataset_id
		JOIN permissions p ON dp.permission_id = p.id
		WHERE 
			<<PERMISSIONS>> 
			AND LOWER(a.name) = :assembly
		ORDER BY d.name`

	// DatasetsPermissionsSql = `SELECT DISTINCT
	// 	datasets.id,
	// 	permissions.name as permission,
	// 	datasets.dataset_id
	// 	FROM datasets
	// 	JOIN dataset_permissions ON datasets.id = dataset_permissions.dataset_id
	// 	JOIN permissions ON dataset_permissions.permission_id = permissions.id
	// 	WHERE datasets.species = :species AND datasets.assembly = :assembly`

	// DatasetPermissionsSql = `SELECT DISTINCT
	// 	datasets.id,
	// 	permissions.name as permission
	// 	FROM datasets
	// 	JOIN dataset_permissions ON datasets.id = dataset_permissions.dataset_id
	// 	JOIN permissions ON dataset_permissions.permission_id = permissions.id
	// 	WHERE datasets.dataset_id = :id`

	DatasetSql = `SELECT 
		d.public_id,
		d.name,
		d.institution,
		g.name AS genome,
		a.name AS assembly,
		d.cells,
		d.description
		FROM datasets d
		JOIN genomes g ON d.genome_id = g.id
		JOIN assemblies a ON d.assembly_id = a.id
		JOIN dataset_permissions dp ON d.id = dp.dataset_id
		JOIN permissions p ON dp.permission_id = p.id
		WHERE 
			<<PERMISSIONS>>
			AND d.public_id = :id`

	FindGenesSql = `SELECT 
		gex.id, 
		g.public_id,
		g.gene_symbol,
		f.url,
		gex.offset,
		gex.size
		FROM gex 
		JOIN genes g ON gex.gene_id = g.id
		JOIN files f ON gex.file_id = f.id
		JOIN datasets d ON gex.dataset_id = d.id
		JOIN dataset_permissions dp ON d.id = dp.dataset_id
		JOIN permissions p ON dp.permission_id = p.id
		WHERE 
			<<PERMISSIONS>>
			AND d.public_id = :id 
			AND (g.public_id IN (<<GENES>>) OR g.ensembl IN (<<GENES>>) OR g.gene_symbol IN (<<GENES>>))`

	SearchGenesSql = ` SELECT 
		g.id, 
		g.ensembl,
		g.gene_symbol
		FROM gex gex
		JOIN genes g ON gex.gene_id = g.id
		JOIN datasets d ON gex.dataset_id = d.id
		JOIN dataset_permissions dp ON d.id = dp.dataset_id
		JOIN permissions p ON dp.permission_id = p.id
		WHERE 
			<<PERMISSIONS>>
			AND d.public_id = :id 
			AND (<<GENES>>)
		ORDER BY g.gene_symbol 
		LIMIT :limit`

	ClustersSql = `SELECT DISTINCT 
		c.public_id,
		c.label,
		c.name,
		c.cell_count,
		c.color,
		m.name AS metadata_name,
		cm.value AS metadata_value
		FROM clusters c
		JOIN datasets d ON c.dataset_id = d.id
		JOIN dataset_permissions dp ON d.id = dp.dataset_id
		JOIN permissions p ON dp.permission_id = p.id
		JOIN cluster_metadata cm ON c.id = cm.cluster_id
		JOIN metadata m ON cm.metadata_id = m.id
		WHERE
			<<PERMISSIONS>>
			AND d.public_id = :id
		ORDER BY c.name, m.name`

	CellsSql = `SELECT
		c.umap_x,
		c.umap_y,
		s.name,
		cl.label
		FROM cells c
		JOIN samples s ON c.sample_id = s.id
		JOIN clusters cl ON c.cluster_id = cl.id
		JOIN datasets d ON s.dataset_id = d.id
		WHERE d.public_id = :id
		ORDER BY c.id`

	GenesSql = `SELECT 
		g.id, 
		g.ensembl_id,
		g.gene_symbol 
		FROM gex gx
		JOIN genes g ON gx.gene_id = g.id
		JOIN datasets d ON gx.dataset_id = d.id
		WHERE d.public_id = :id
		ORDER BY g.gene_symbol`
)

// const DATASETS_SQL = `SELECT
// 	name
// 	FROM datasets
// 	ORDER BY datasets.name`

// type GexValue string

func NewScrnaDB(dir string) *ScrnaDB {

	// db, err := sql.Open("sqlite3", path)

	// if err != nil {
	// 	log.Fatal().Msgf("%s", err)
	// }

	// defer db.Close()

	return &ScrnaDB{dir: dir, db: sys.Must(sql.Open(sys.Sqlite3DB, filepath.Join(dir, "scrna.db"+sys.SqliteReadOnlySuffix)))}
}

func (sdb *ScrnaDB) Dir() string {
	return sdb.dir
}

func (sdb *ScrnaDB) Close() error {
	return sdb.db.Close()
}

// func (sdb *Datasetssdb) GetGenes(genes []string) ([]*GexGene, error) {
// 	db, err := sql.Open("sqlite3", sdb.dir)

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

func (sdb *ScrnaDB) Genomes() ([]*sys.Entity, error) {

	genomes := make([]*sys.Entity, 0, 10)

	rows, err := sdb.db.Query(GenomesSQL)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var genome sys.Entity

		err := rows.Scan(&genome.Id, &genome.PublicId, &genome.Name)

		if err != nil {
			return nil, err
		}

		genomes = append(genomes, &genome)
	}

	return genomes, nil
}

func (sdb *ScrnaDB) Assemblies(genome string) ([]*sys.Entity, error) {

	assemblies := make([]*sys.Entity, 0, 10)

	rows, err := sdb.db.Query(AssembliesSql, sql.Named("genome", web.FormatParam(genome)))

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var assembly sys.Entity

		err := rows.Scan(&assembly.Id, &assembly.PublicId, &assembly.Name)

		if err != nil {
			return nil, err
		}

		assemblies = append(assemblies, &assembly)
	}

	return assemblies, nil
}

func (sdb *ScrnaDB) Datasets(assembly string, isAdmin bool, permissions []string) ([]*Dataset, error) {

	namedArgs := []any{sql.Named("assembly", strings.ToLower(assembly))}

	query := sqlite.MakePermissionsSql(DatasetsSql, isAdmin, permissions, &namedArgs)

	datasets := make([]*Dataset, 0, 10)

	datasetRows, err := sdb.db.Query(query, namedArgs...)

	if err != nil {
		log.Debug().Msgf("%s", err)
		return nil, err
	}

	defer datasetRows.Close()

	for datasetRows.Next() {
		var dataset Dataset

		err := datasetRows.Scan(
			&dataset.Id,
			&dataset.Name,
			&dataset.Institution,
			&dataset.Species,
			&dataset.Assembly,
			&dataset.Cells,
			&dataset.Description)

		if err != nil {
			return nil, err
		}

		// log.Debug().Msgf("db %s", filepath.Join(sdb.dir, dataset.Url))

		// db2, err := sql.Open("sqlite3", filepath.Join(sdb.dir, dataset.Url))

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

// Return nil if user has permission to view dataset otherwise an error
// describing why not
// func (sdb *ScrnaDB) HasPermissionToViewDataset(datasetId string, permissions []string) error {
// 	//return errors.New("not implemented")

// 	if auth.HasAdminPermission(permissions) {
// 		return nil
// 	}

// 	rows, err := sdb.db.Query(DatasetPermissionsSql, sql.Named("id", datasetId))

// 	if err != nil {
// 		log.Error().Msgf("checking dataset permissions %s", err)
// 		return err
// 	}

// 	defer rows.Close()

// 	permissionSet := sys.NewStringSet().ListUpdate(permissions)

// 	var id string
// 	var permission string

// 	for rows.Next() {
// 		err := rows.Scan(&id, &permission)

// 		if err != nil {
// 			log.Error().Msgf("scanning dataset permissions %s", err)
// 			return err
// 		}

// 		if permissionSet.Has(permission) {
// 			return nil
// 		}
// 	}

// 	return errors.New("not allowed to view dataset: " + datasetId)
// }

func (sdb *ScrnaDB) dataset(datasetId string, isAdmin bool, permissions []string) (*Dataset, error) {

	namedArgs := []any{sql.Named("id", datasetId)}

	query := sqlite.MakePermissionsSql(DatasetSql, isAdmin, permissions, &namedArgs)

	var dataset Dataset

	err := sdb.db.QueryRow(query, namedArgs...).Scan(
		&dataset.Id,
		&dataset.Name,
		&dataset.Institution,
		&dataset.Species,
		&dataset.Assembly,
		&dataset.Cells,
		&dataset.Description)

	if err != nil {
		return nil, err
	}

	return &dataset, nil
}

func (sdb *ScrnaDB) SearchGenes(datasetId string, q string, limit int, isAdmin bool, permissions []string) ([]*Gene, error) {

	namedArgs := []any{sql.Named("id", datasetId),
		//sql.Named("q", fmt.Sprintf("%%%s%%", q)),
		sql.Named("limit", limit)}

	stmt := sqlite.MakePermissionsSql(SearchGenesSql, isAdmin, permissions, &namedArgs)

	where, err := query.SqlBoolQuery(q, func(placeholderIndex int, value string, addParens bool) string {
		log.Debug().Msgf("search gene q: %s %s", q, value)
		return query.AddParens("g.gene_id = '"+value+"' OR g.gene_symbol LIKE '"+value+"'", addParens)
	})

	if err != nil {
		return nil, err
	}

	stmt = strings.Replace(stmt, "<<GENES>>", where.Sql, 1)

	//log.Debug().Msgf("finalSQL %s", stmt)

	ret := make([]*Gene, 0, limit)

	rows, err := sdb.db.Query(stmt, namedArgs...)

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

func (sdb *ScrnaDB) GetGenes(datasetId string, geneIds []string, isAdmin bool, permissions []string) ([]*Gene, error) {

	namedArgs := []any{sql.Named("id", datasetId)}

	query := sqlite.MakePermissionsSql(FindGenesSql, isAdmin, permissions, &namedArgs)

	//log.Debug().Msgf("find genes sql: %s %v", query, namedArgs)

	query = makeInGenesSql(query, geneIds, &namedArgs)

	//log.Debug().Msgf("find genes sql: %s %v", query, namedArgs)

	rows, err := sdb.db.Query(query, namedArgs...)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	ret := make([]*Gene, 0, len(geneIds))

	for rows.Next() {
		var gene Gene
		err := rows.Scan(
			&gene.Id,
			&gene.Ensembl,
			&gene.GeneSymbol,
			&gene.Url,
			&gene.Offset,
			&gene.Size)

		if err != nil {
			return nil, err
		}
		// add as many genes as possible
		ret = append(ret, &gene)
	}

	return ret, nil
}

func (sdb *ScrnaDB) Gex(datasetId string,
	geneIds []string,
	isAdmin bool,
	permissions []string) (*dat.GexResults, error) {

	genes, err := sdb.GetGenes(datasetId, geneIds, isAdmin, permissions)

	if err != nil {
		return nil, err
	}

	//log.Debug().Msgf("found %d genes", len(genes))

	//datasetUrl := filepath.Dir(dsdb.dataset.Url)

	// where the gex data is located
	//gexUrl := filepath.Join(datasetUrl, "gex")

	//cellCount := cache.dataset.Cells

	ret := dat.GexResults{
		Dataset: datasetId, //dat.ResultDataset{Id: dc.dataset.Id},
		Genes:   make([]*dat.GexGene, 0, len(genes)),
	}

	//var gexCache = make(map[string]*dat.GexGene)

	for _, gene := range genes {
		gexFile := filepath.Join(sdb.dir, gene.Url)

		//gexData, ok := gexCache[gexFile]

		//if !ok {

		// f, err := os.Open(gexFile)
		// if err != nil {
		// 	return nil, err
		// }
		// defer f.Close()

		data, err := dat.SeekGexGeneFromDat(gexFile, gene.Offset)

		if err != nil {
			//log.Debug().Msgf("not able to read gex data for gene %s in dataset %s", gene.GeneSymbol, datasetId)
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

		//gexCache[gexFile] = data
		//gexData = data
		//}

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
		ret.Genes = append(ret.Genes, data)

	}

	return &ret, nil
}

// func (sdb *Datasetssdb) Metadata(publicId string) (*DatasetClusters, error) {

// 	dataset, err := sdb.dataset(publicId)

// 	if err != nil {
// 		return nil, err
// 	}

// 	datasetsdb := NewDatasetsdb(dataset)

// 	ret, err := datasetsdb.Metadata()

// 	if err != nil {
// 		return nil, err
// 	}

// 	return ret, nil
// }

func (sdb *ScrnaDB) Metadata(datasetId string, isAdmin bool, permissions []string) (*DatasetMetadata, error) {

	namedArgs := []any{sql.Named("id", datasetId)}

	query := sqlite.MakePermissionsSql(ClustersSql, isAdmin, permissions, &namedArgs)

	rows, err := sdb.db.Query(query, namedArgs...)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	clusters := make([]*Cluster, 0, 50)
	//clusterMap := make(map[string]*Cluster)

	var currentCluster *Cluster

	for rows.Next() {
		var cluster Cluster
		var metadata ClusterMetadata

		err := rows.Scan(
			&cluster.Id,
			&cluster.Label,
			&cluster.Name,
			&cluster.CellCount,
			&cluster.Color,
			&metadata.Name,
			&metadata.Value)

		if err != nil {
			return nil, err
		}

		if currentCluster == nil || currentCluster.Id != cluster.Id {
			// same cluster, add metadata
			currentCluster = &cluster
			currentCluster.Metadata = make(map[string]string, 5) // make([]*ClusterMetadata, 0, 5)
			clusters = append(clusters, currentCluster)
		}

		currentCluster.Metadata[metadata.Name] = metadata.Value
	}

	var cellCount int

	err = sdb.db.QueryRow(CellCountSql).Scan(&cellCount)

	if err != nil {
		return nil, err
	}

	cells := make([]*SingleCell, 0, cellCount)

	// in this query we do not check for permissions again as we have already
	// done so above
	rows, err = sdb.db.Query(CellsSql, sql.Named("id", datasetId))

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var cell SingleCell

		err := rows.Scan(

			&cell.Pos.X,
			&cell.Pos.Y,
			&cell.SampleName,
			&cell.ClusterLabel)

		if err != nil {
			return nil, err
		}

		cells = append(cells, &cell)
	}

	return &DatasetMetadata{
		Dataset:  datasetId,
		Clusters: clusters,
		Cells:    cells,
	}, nil
}

func (sdb *ScrnaDB) Genes(datasetId string, isAdmin bool, permissions []string) ([]*Gene, error) {

	//log.Debug().Msgf("cripes %v", filepath.Join(cache.dir, cache.dataset.Path))

	// 50k for the num of genes we expect
	ret := make([]*Gene, 0, 50000)

	namedArgs := []any{sql.Named("id", datasetId)}

	query := sqlite.MakePermissionsSql(GenesSql, isAdmin, permissions, &namedArgs)

	rows, err := sdb.db.Query(query, namedArgs...)

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

func makeInGenesClause(geneIds []string, namedArgs *[]any) string {
	inPlaceholders := make([]string, len(geneIds))

	// for i, perm := range geneIds {
	// 	ph := fmt.Sprintf("id%d", i+1)

	// 	inPlaceholders = append(inPlaceholders, "g.gene_id LIKE :"+ph) // OR g.ensembl_id LIKE :q)
	// 	inPlaceholders = append(inPlaceholders, "g.gene_symbol LIKE :"+ph)
	// 	*namedArgs = append(*namedArgs, sql.Named(ph, perm))
	// }

	for i, id := range geneIds {
		ph := fmt.Sprintf("g%d", i+1)
		inPlaceholders[i] = ":" + ph
		*namedArgs = append(*namedArgs, sql.Named(ph, id))
	}

	return strings.Join(inPlaceholders, ",")
}

func makeInGenesSql(query string, geneIds []string, namedArgs *[]any) string {
	inClause := makeInGenesClause(geneIds, namedArgs)

	return strings.ReplaceAll(query, "<<GENES>>", inClause)
}
